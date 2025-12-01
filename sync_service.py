import os
import io
import re
import base64
import requests
import msal
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import text
from flask import current_app

# --- FUNCIONES HELPER ---

def normalize_remito(remito_raw):
    if not remito_raw: return ""
    try:
        s = str(remito_raw).strip()
        if '-' in s:
            parts = s.split('-')
            number_part = parts[-1]
        else:
            number_part = s
        return number_part.lstrip('0')
    except Exception:
        return ""

def clean_patente(p):
    if not p: return ""
    return re.sub(r'[^A-Z0-9]', '', str(p).upper())

def normalize_text(t):
    """Limpia texto genérico (Origen) para agrupar mejor"""
    if not t: return ""
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
    base64_value = base64.urlsafe_b64encode(share_url.encode("utf-8")).decode("utf-8")
    encoded_url = "u!" + base64_value.rstrip("=")
    endpoint = f"https://graph.microsoft.com/v1.0/shares/{encoded_url}/driveItem/content"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(endpoint, headers=headers)
    if resp.status_code == 200: return io.BytesIO(resp.content)
    return None

# --- FUNCIÓN PRINCIPAL ---

def run_sbe_sync(db, Shipment):
    print("--- 🔍 INICIO SYNC SBE (AGRUPACIÓN ESTRICTA) ---")
    
    try:
        res = db.session.execute(text("SELECT tolerance_kg FROM system_config LIMIT 1")).fetchone()
        tolerance_kg = float(res[0]) if res else 700.0
    except Exception:
        tolerance_kg = 700.0

    token = get_graph_token()
    if not token: return (0, "Error credenciales Azure.")

    links = [os.getenv("SHAREPOINT_LINK_1"), os.getenv("SHAREPOINT_LINK_2")]
    dfs = []

    for i, link in enumerate(links):
        if link:
            content = download_sharepoint_excel(link, token)
            if content:
                try:
                    df = pd.read_excel(content, sheet_name="Reporte")
                    dfs.append(df)
                except Exception as e:
                    print(f"Error leyendo Excel {i}: {e}")

    if not dfs: return (0, "No se descargaron archivos.")

    raw_df = pd.concat(dfs, ignore_index=True)
    raw_df.columns = [c.strip() for c in raw_df.columns]
    
    # -------------------------------------------------------
    # NUEVA LÓGICA DE AGRUPACIÓN SEGURA
    # -------------------------------------------------------
    
    # A. Filtro básico
    if 'Estado' in raw_df.columns:
        raw_df = raw_df[raw_df['Estado'].astype(str).str.strip().str.lower() == 'ingreso'].copy()
    
    raw_df = raw_df.dropna(subset=['Factura'])
    
    # B. Normalización para CLAVES DE AGRUPACIÓN
    # Creamos columnas temporales normalizadas para usarlas de llave única
    raw_df['Key_Remito']  = raw_df['Factura'].apply(normalize_remito)
    raw_df['Key_Fecha']   = pd.to_datetime(raw_df['Fecha Salida'], errors='coerce').dt.date
    
    # Normalizamos Patentes y Origen para evitar que un espacio o mayúscula impida la suma
    raw_df['Key_Patente'] = raw_df['Patente Tractor'].apply(clean_patente)
    # Si existe patente camión, la usamos, si no, usamos vacío para no romper el group
    if 'Patente Camión' in raw_df.columns:
        raw_df['Key_Acoplado'] = raw_df['Patente Camión'].apply(clean_patente)
    else:
        raw_df['Key_Acoplado'] = ""
        
    raw_df['Key_Origen'] = raw_df['Origen'].apply(normalize_text)
    
    # Asegurar peso numérico
    raw_df['Peso Neto'] = pd.to_numeric(raw_df['Peso Neto'], errors='coerce').fillna(0)

    # C. AGRUPAR: (Remito + Fecha + Patente + Acoplado + Origen) -> Sumar Peso
    # Esto es la seguridad máxima.
    agg_rules = {
        'Peso Neto': 'sum',
        # Para el resto, nos quedamos con el dato de la primera fila del grupo
        'Factura': 'first',          
        'Patente Tractor': 'first',
        'Patente Camión': 'first',
        'Fecha Salida': 'first',     
        'Fecha Entrada': 'first'
    }
    
    # Filtramos reglas por si alguna columna no existe en el Excel
    actual_agg = {k: v for k, v in agg_rules.items() if k in raw_df.columns}
    
    # Claves de agrupación
    group_keys = ['Key_Remito', 'Key_Fecha', 'Key_Patente', 'Key_Acoplado', 'Key_Origen']
    
    full_data_clean = raw_df.groupby(group_keys, as_index=False).agg(actual_agg)

    # D. Columnas limpias para el cruce final con la DB
    full_data_clean['Patente_Clean'] = full_data_clean['Key_Patente']
    full_data_clean['Patente_Camion_Clean'] = full_data_clean['Key_Acoplado']
    # (El remito ya está en Key_Remito, pero lo renombramos para compatibilidad con el código de abajo)
    full_data_clean['Remito_Norm'] = full_data_clean['Key_Remito']
    full_data_clean['Fecha_Solo'] = full_data_clean['Key_Fecha']

    print(f"📊 Filas procesadas (Agrupación Segura): {len(full_data_clean)}")

    # -------------------------------------------------------
    # LÓGICA DE CRUCE (Idéntica a la anterior, usando la tabla agrupada)
    # -------------------------------------------------------

    pendientes = db.session.query(Shipment).filter(
        Shipment.cert_status != 'Certificado',
        Shipment.remito_arenera != None, 
        Shipment.remito_arenera != "",
        Shipment.status.in_(['En viaje', 'Salió', 'Llego'])
    ).all()

    matches = 0

    for ship in pendientes:
        if ship.sbe_manual_override:
            continue

        ship_remito = normalize_remito(ship.remito_arenera)
        ship_trac = clean_patente(ship.tractor)
        ship_trail = clean_patente(ship.trailer)
        ship_date = ship.date
        
        match_row = None
        match_type = ""
        reasons = [] 

        # Filtro fecha
        valid_sbe = full_data_clean[full_data_clean['Fecha_Solo'] >= ship_date]
        if valid_sbe.empty: continue 

        # 1. Match Total
        if ship_remito:
            rem_cond = (valid_sbe['Remito_Norm'] == ship_remito)
            pat_cond = (
                (valid_sbe['Patente_Clean'] == ship_trac) |
                (valid_sbe['Patente_Camion_Clean'] == ship_trail) |
                (valid_sbe['Patente_Clean'] == ship_trail) |
                (valid_sbe['Patente_Camion_Clean'] == ship_trac)
            )
            found = valid_sbe[rem_cond & pat_cond]
            
            if not found.empty: 
                found = found.sort_values('Fecha_Solo')
                match_row = found.iloc[0]; match_type = "Total"

        # 2. Solo Remito
        if match_row is None and ship_remito:
            found = valid_sbe[valid_sbe['Remito_Norm'] == ship_remito]
            if not found.empty: 
                found = found.sort_values('Fecha_Solo')
                match_row = found.iloc[0]; match_type = "Remito"

        # 3. Solo Patente
        if match_row is None and (ship_trac or ship_trail):
            pat_cond = (valid_sbe['Patente_Clean'] == ship_trac) | \
                       (valid_sbe['Patente_Camion_Clean'] == ship_trac) | \
                       (valid_sbe['Patente_Clean'] == ship_trail) | \
                       (valid_sbe['Patente_Camion_Clean'] == ship_trail)
            found = valid_sbe[pat_cond]
            if not found.empty:
                for _, row in found.iterrows():
                    sbe_d = row['Fecha_Solo']
                    if sbe_d and (sbe_d - ship_date).days <= 7:
                        match_row = row; match_type = "Patente"; break

        # Actualización DB
        if match_row is not None:
            matches += 1
            
            ship.sbe_remito = str(match_row.get('Factura', ''))
            
            p_sbe_t = clean_patente(match_row.get('Patente Tractor',''))
            p_sbe_c = clean_patente(match_row.get('Patente Camión',''))
            
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
                sbe_date = pd.to_datetime(fs)
                ship.sbe_fecha_salida = sbe_date
                if sbe_date.date() < (ship.date - timedelta(days=1)):
                    reasons.append("Fecha SBE anterior")
            
            fl = match_row.get('Fecha Entrada')
            if pd.notna(fl): ship.sbe_fecha_llegada = pd.to_datetime(fl)

            if "Remito" in match_type: reasons.append("Revisar Patente")
            elif "Patente" in match_type: reasons.append("Revisar Remito")
            
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