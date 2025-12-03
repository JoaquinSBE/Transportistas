import os
import io
import re
import base64
import requests
import msal
import pandas as pd
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo 
from sqlalchemy import text
from flask import current_app

ARG_TZ = ZoneInfo("America/Argentina/Buenos_Aires")

# --- FUNCIONES HELPER ---

def normalize_remito(remito_raw):
    if pd.isna(remito_raw): return ""
    try:
        s = str(remito_raw).strip()
        if s.endswith('.0'): s = s[:-2] 
        if '-' in s:
            parts = s.split('-')
            number_part = parts[-1]
        else:
            number_part = s
        return number_part.lstrip('0').strip()
    except Exception:
        return ""

def clean_patente(p):
    if pd.isna(p): return ""
    return re.sub(r'[^A-Z0-9]', '', str(p).upper())

def normalize_text(t):
    if pd.isna(t): return ""
    return str(t).strip().upper()

def get_graph_token():
    tenant_id = os.getenv("GRAPH_TENANT_ID")
    client_id = os.getenv("GRAPH_CLIENT_ID")
    secret    = os.getenv("GRAPH_CLIENT_SECRET")
    if not (tenant_id and client_id and secret): return None
    authority = f"https://login.microsoftonline.com/{tenant_id}"
    app_msal = msal.ConfidentialClientApplication(client_id, authority=authority, client_credential=secret)
    result = app_msal.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
    return result.get("access_token")

def download_sharepoint_excel(share_url, token):
    if not share_url: return None
    base64_value = base64.urlsafe_b64encode(share_url.encode("utf-8")).decode("utf-8")
    encoded_url = "u!" + base64_value.rstrip("=")
    endpoint = f"https://graph.microsoft.com/v1.0/shares/{encoded_url}/driveItem/content"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(endpoint, headers=headers)
    if resp.status_code == 200: return io.BytesIO(resp.content)
    return None

def prepare_dataframe(df, label="DF"):
    """Limpia columnas y repara fechas usando Fecha Entrada si es necesario"""
    if df is None or df.empty: return pd.DataFrame()
    
    df.columns = [str(c).strip() for c in df.columns]
    
    # 1. FILTRO DE ESTADO
    if 'Estado' in df.columns:
        df = df[df['Estado'].astype(str).str.strip().str.lower() == 'ingreso'].copy()

    # 2. Reparación de Fechas
    if 'Fecha Salida' not in df.columns:
        return pd.DataFrame()
        
    if 'Fecha Entrada' in df.columns:
        df['Fecha Salida'] = df['Fecha Salida'].fillna(df['Fecha Entrada'])
    
    # 3. Columna temporal (CON dayfirst=True)
    df['__temp_date'] = pd.to_datetime(df['Fecha Salida'], dayfirst=True, errors='coerce').dt.date
    
    df = df.dropna(subset=['__temp_date'])
    
    return df

def download_and_concat(links, token, label="", sheet_name="Reporte"):
    dfs = []
    print(f"📥 Descargando {label}...")
    for i, link in enumerate(links):
        if link:
            content = download_sharepoint_excel(link, token)
            if content:
                try:
                    df = pd.read_excel(content, sheet_name=sheet_name)
                    dfs.append(df)
                except Exception as e:
                    print(f"❌ Error leyendo Excel {label} #{i+1}: {e}")
    if dfs:
        return pd.concat(dfs, ignore_index=True)
    return pd.DataFrame()

# --- FUNCIÓN PRINCIPAL ---

def run_sbe_sync(db, Shipment):
    print("--- 🔍 INICIO SYNC SBE (V17: SOLO REMITO - CERO PATENTE) ---")
    
    try:
        res = db.session.execute(text("SELECT tolerance_kg FROM system_config LIMIT 1")).fetchone()
        tolerance_kg = float(res[0]) if res else 700.0
    except Exception:
        tolerance_kg = 700.0

    token = get_graph_token()
    if not token: return (0, "Error credenciales Azure.")

    # 1. DESCARGA
    links_hist = [os.getenv("SHAREPOINT_LINK_1"), os.getenv("SHAREPOINT_LINK_2")]
    df_historico = download_and_concat(links_hist, token, "Histórico", sheet_name="Reporte")

    links_online = [os.getenv("SHAREPOINT_LINK_ONLINE_1"), os.getenv("SHAREPOINT_LINK_ONLINE_2")]
    df_online = download_and_concat(links_online, token, "Online", sheet_name="Reporte Diario")

    # 2. PREPARACIÓN
    df_historico = prepare_dataframe(df_historico, "Histórico")
    df_online    = prepare_dataframe(df_online, "Online")
    
    hoy_arg = datetime.now(ARG_TZ).date()
    fecha_buffer = hoy_arg - timedelta(days=15)

    if not df_historico.empty:
        df_historico = df_historico[df_historico['__temp_date'] < hoy_arg].copy()

    if not df_online.empty:
        df_online = df_online[df_online['__temp_date'] >= fecha_buffer].copy()

    raw_df = pd.concat([df_historico, df_online], ignore_index=True)
    
    if raw_df.empty: return (0, "No hay datos 'Ingreso' válidos.")

    # 3. DEDUPLICACIÓN
    if 'Factura' in raw_df.columns:
        raw_df['Factura'] = raw_df['Factura'].astype(str)
        raw_df['__temp_factura'] = raw_df['Factura'].str.strip()
        raw_df = raw_df.drop_duplicates(subset=['__temp_factura'], keep='last')

    # 4. NORMALIZACIÓN
    raw_df['Key_Remito']  = raw_df['Factura'].apply(normalize_remito)
    raw_df['Key_Fecha']   = pd.to_datetime(raw_df['Fecha Salida'], dayfirst=True, errors='coerce').dt.date
    raw_df['Key_Patente'] = raw_df['Patente Tractor'].apply(clean_patente)
    
    if 'Patente Camión' in raw_df.columns:
        raw_df['Key_Acoplado'] = raw_df['Patente Camión'].apply(clean_patente)
    else:
        raw_df['Key_Acoplado'] = ""
        
    raw_df['Key_Origen'] = raw_df['Origen'].apply(normalize_text)
    raw_df['Peso Neto'] = pd.to_numeric(raw_df['Peso Neto'], errors='coerce').fillna(0)

    raw_df = raw_df.dropna(subset=['Key_Fecha'])

    # 5. AGRUPAR
    agg_rules = {
        'Peso Neto': 'sum',
        'Factura': 'first',          
        'Patente Tractor': 'first',
        'Patente Camión': 'first',
        'Fecha Salida': 'first',     
        'Fecha Entrada': 'first'
    }
    actual_agg = {k: v for k, v in agg_rules.items() if k in raw_df.columns}
    group_keys = ['Key_Remito', 'Key_Fecha', 'Key_Patente', 'Key_Acoplado', 'Key_Origen']
    
    for col in group_keys:
        if col in raw_df.columns: raw_df[col] = raw_df[col].fillna("")

    full_data_clean = raw_df.groupby(group_keys, as_index=False).agg(actual_agg)
    full_data_clean.reset_index(drop=True, inplace=True)

    full_data_clean['Patente_Clean'] = full_data_clean['Key_Patente']
    full_data_clean['Patente_Camion_Clean'] = full_data_clean['Key_Acoplado']
    full_data_clean['Remito_Norm'] = full_data_clean['Key_Remito']
    full_data_clean['Fecha_Solo'] = full_data_clean['Key_Fecha']

    # 6. CRUCE
    pendientes = db.session.query(Shipment).filter(
        Shipment.cert_status != 'Certificado',
        Shipment.remito_arenera != None, 
        Shipment.remito_arenera != "",
        Shipment.status.in_(['En viaje', 'Salió', 'Llego', 'Salido a SBE']) 
    ).all()

    matches = 0
    used_indices = set()

    for ship in pendientes:
        if ship.sbe_manual_override: continue

        ship_remito = normalize_remito(ship.remito_arenera)
        ship_trac = clean_patente(ship.tractor)
        ship_trail = clean_patente(ship.trailer)
        ship_date = ship.date
        
        match_row = None
        match_type = ""
        reasons = [] 

        # Filtro de fecha (Estricto >=)
        valid_sbe = full_data_clean[full_data_clean['Fecha_Solo'] >= ship_date]
        if valid_sbe.empty: continue 

        # ------------------------------------------------------------------
        # NIVEL 1: MATCH TOTAL (Remito + Alguna Patente)
        # ------------------------------------------------------------------
        if ship_remito:
            rem_cond = (valid_sbe['Remito_Norm'] == ship_remito)
            pat_cond = (
                (valid_sbe['Patente_Clean'] == ship_trac) |
                (valid_sbe['Patente_Camion_Clean'] == ship_trail) |
                (valid_sbe['Patente_Clean'] == ship_trail) |
                (valid_sbe['Patente_Camion_Clean'] == ship_trac)
            )
            found = valid_sbe[rem_cond & pat_cond].copy()
            
            if not found.empty:
                found = found[~found.index.isin(used_indices)]

            if not found.empty: 
                found['diff'] = (pd.to_datetime(found['Fecha_Solo']) - pd.to_datetime(ship_date)).dt.days
                found = found.sort_values('diff')
                if found.iloc[0]['diff'] <= 5:
                    match_row = found.iloc[0]; match_type = "Total"

        # ------------------------------------------------------------------
        # NIVEL 2: SOLO REMITO (Sin mirar patentes)
        # ------------------------------------------------------------------
        if match_row is None and ship_remito:
            found = valid_sbe[valid_sbe['Remito_Norm'] == ship_remito].copy()
            
            if not found.empty:
                found = found[~found.index.isin(used_indices)]

            if not found.empty: 
                found['diff'] = (pd.to_datetime(found['Fecha_Solo']) - pd.to_datetime(ship_date)).dt.days
                found = found.sort_values('diff')
                if found.iloc[0]['diff'] <= 5:
                    match_row = found.iloc[0]; match_type = "Remito"

        # ------------------------------------------------------------------
        # SE ELIMINÓ EL NIVEL 3 (SOLO PATENTE)
        # Si no coincidió el remito, no se cruza. Punto.
        # ------------------------------------------------------------------

        if match_row is not None:
            matches += 1
            used_indices.add(match_row.name)
            
            remito_limpio = str(match_row.get('Remito_Norm', ''))
            if not remito_limpio:
                remito_limpio = normalize_remito(str(match_row.get('Factura', '')))
                
            ship.sbe_remito = remito_limpio
            
            p_sbe_t = clean_patente(match_row.get('Patente Tractor',''))
            p_sbe_c = clean_patente(match_row.get('Patente Camión',''))
            
            # Prioridad de asignación para display
            if p_sbe_t == ship_trac or p_sbe_t == ship_trail:
                ship.sbe_patente = match_row.get('Patente Tractor','')
            elif p_sbe_c == ship_trac or p_sbe_c == ship_trail:
                ship.sbe_patente = match_row.get('Patente Camión','')
            else:
                ship.sbe_patente = match_row.get('Patente Tractor','')

            w_raw = float(match_row.get('Peso Neto', 0) or 0)
            ship.sbe_peso_neto = w_raw / 1000.0 if w_raw > 100 else w_raw
            
            fs = match_row.get('Fecha Salida')
            if pd.notna(fs): 
                sbe_date = pd.to_datetime(fs, dayfirst=True, errors='coerce')
                if pd.notna(sbe_date):
                    ship.sbe_fecha_salida = sbe_date
            
            fl = match_row.get('Fecha Entrada')
            if pd.notna(fl): 
                sbe_llegada = pd.to_datetime(fl, dayfirst=True, errors='coerce')
                if pd.notna(sbe_llegada):
                    ship.sbe_fecha_llegada = sbe_llegada

            # -------------------------------------------------------------
            # LÓGICA DE OBSERVACIONES (V17)
            # -------------------------------------------------------------
            
            if match_type == "Remito": 
                reasons.append("Revisar Patente (Coincide Remito)")
            
            # Doble check de Tractor (Por si matcheó remito pero con otro camión)
            if ship_trac and p_sbe_t:
                if ship_trac != p_sbe_t:
                    if "Revisar Patente" not in str(reasons):
                        reasons.append("Diferencia Patente Tractor")

            w_local = ship.peso_neto_arenera or 0
            w_sbe   = ship.sbe_peso_neto or 0
            if w_sbe > 0:
                diff_kg = abs(w_local - w_sbe) * 1000
                if diff_kg > tolerance_kg:
                    reasons.append(f"Dif. Peso ({int(diff_kg)}kg)")

            if reasons:
                ship.cert_status = "Observado"
                ship.observation_reason = ", ".join(reasons)
            else:
                ship.cert_status = "Pre-Aprobado"
                ship.observation_reason = None

    db.session.commit()
    return (matches, None)