# sync_service.py

import os
import io
import re
import base64
import requests
import msal
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import func
from flask import current_app

# --- FUNCIONES HELPER ---

def normalize_remito(remito_raw):
    """Normaliza el remito separando por '-' y quitando ceros a la izquierda."""
    if not remito_raw: return ""
    try:
        parts = str(remito_raw).split('-')
        # Tomamos la última parte y quitamos ceros a la izquierda
        return parts[-1].lstrip('0')
    except Exception:
        return ""

def get_graph_token():
    """Obtiene token de acceso para Microsoft Graph"""
    tenant_id = os.getenv("GRAPH_TENANT_ID")
    client_id = os.getenv("GRAPH_CLIENT_ID")
    secret    = os.getenv("GRAPH_CLIENT_SECRET")
    
    if not (tenant_id and client_id and secret):
        return None
        
    authority = f"https://login.microsoftonline.com/{tenant_id}"
    app_msal = msal.ConfidentialClientApplication(
        client_id, authority=authority, client_credential=secret
    )
    result = app_msal.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
    return result.get("access_token")

def download_sharepoint_excel(share_url, token):
    """Descarga Excel desde Link de Sharepoint usando Graph API"""
    base64_value = base64.urlsafe_b64encode(share_url.encode("utf-8")).decode("utf-8")
    encoded_url = "u!" + base64_value.rstrip("=")
    
    endpoint = f"https://graph.microsoft.com/v1.0/shares/{encoded_url}/driveItem/content"
    headers = {"Authorization": f"Bearer {token}"}
    
    resp = requests.get(endpoint, headers=headers)
    if resp.status_code == 200:
        return io.BytesIO(resp.content)
    return None

def clean_patente(p):
    """Normaliza patente: quita espacios, guiones y pone mayúsculas"""
    if not p: return ""
    return re.sub(r'[^A-Z0-9]', '', str(p).upper())


# --- FUNCIÓN PRINCIPAL DE SINCRONIZACIÓN ---

def run_sbe_sync(db, Shipment):
    """Función principal que ejecuta la sincronización y el cruce."""
    token = get_graph_token()
    if not token:
        return (0, "Error de configuración Azure (Faltan credenciales .env).")

    # 1. Descargar y unificar Excels
    links = [os.getenv("SHAREPOINT_LINK_1"), os.getenv("SHAREPOINT_LINK_2")]
    dfs = []
    
    for link in links:
        if link:
            file_content = download_sharepoint_excel(link, token)
            if file_content:
                try:
                    # USAMOS sheet_name="Reporte" como se confirmó
                    df = pd.read_excel(file_content, sheet_name="Reporte") 
                    dfs.append(df)
                except Exception as e:
                    current_app.logger.error(f"Error leyendo Excel SBE: {e}")
                    return (0, f"Error leyendo el archivo. Asegúrate que la hoja se llama 'Reporte'. Detalle: {e}") 

    if not dfs:
        return (0, "No se pudieron descargar los archivos de SBE.")

    full_data = pd.concat(dfs, ignore_index=True)
    full_data.columns = [c.strip() for c in full_data.columns]
    full_data = full_data.dropna(subset=['Factura']) 

    # Normalización de SBE para CRUCE
    full_data['Remito_Normalized'] = full_data['Factura'].apply(normalize_remito)
    full_data['Patente_Tractor_Clean'] = full_data['Patente Tractor'].apply(clean_patente)
    full_data['Patente_Camion_Clean'] = full_data['Patente Camión'].apply(clean_patente) # Nuevo campo de limpieza

    # 2. Buscar viajes PENDIENTES
    pendientes = db.session.query(Shipment).filter(
        Shipment.cert_status != 'Certificado',
        Shipment.status.in_(['En viaje', 'Llego'])
    ).all()

    matches_count = 0
    
    for ship in pendientes:
        # Normalización de datos LOCALES para CRUCE
        ship_remito = normalize_remito(ship.remito_arenera) # Remito local normalizado
        ship_tractor = clean_patente(ship.tractor)
        ship_trailer = clean_patente(ship.trailer)
        
        match_row = None
        match_type = ""

        # --- LÓGICA DE CRUCE DE 3 NIVELES ---

        # 1. INTENTO 1: REMITO + PATENTE (Cruzada)
        if ship_remito:
            
            # Condición de Remito: Que el remito normalizado SBE coincida con el remito normalizado local
            remito_match_cond = (full_data['Remito_Normalized'] == ship_remito)
            
            # Condición de Patente: Que la patente local (Tractor o Trailer) coincida con la Patente SBE (Tractor o Camión)
            # Esto maneja errores de registro (Tractor en campo Camion, etc.)
            plate_match_cond = (
                (full_data['Patente_Tractor_Clean'] == ship_tractor) | # SBE TRACTOR == LOCAL TRACTOR
                (full_data['Patente_Camion_Clean'] == ship_trailer) |  # SBE CAMION == LOCAL TRAILER
                (full_data['Patente_Tractor_Clean'] == ship_trailer) | # SBE TRACTOR == LOCAL TRAILER (Swapped)
                (full_data['Patente_Camion_Clean'] == ship_tractor)    # SBE CAMION == LOCAL TRACTOR (Swapped)
            )

            found = full_data[remito_match_cond & plate_match_cond]
            
            if not found.empty:
                match_row = found.iloc[0]
                match_type = "Total (Remito + Patente Cruzada)"

        # 2. INTENTO 2: SOLO REMITO (Si falló el 1)
        if match_row is None and ship_remito:
            found = full_data[full_data['Remito_Normalized'] == ship_remito]
            if not found.empty:
                match_row = found.iloc[0]
                match_type = "Por Remito (Fallo Patente)"

        # 3. INTENTO 3: SOLO PATENTE + VENTANA DE 7 DÍAS (Si falló 1 y 2)
        if match_row is None and (ship_tractor or ship_trailer):
            # Filtramos si la patente LOCAL (tractor o trailer) coincide con CUALQUIER patente SBE
            patente_search_cond = (full_data['Patente_Tractor_Clean'] == ship_tractor) | \
                                  (full_data['Patente_Camion_Clean'] == ship_tractor) | \
                                  (full_data['Patente_Tractor_Clean'] == ship_trailer) | \
                                  (full_data['Patente_Camion_Clean'] == ship_trailer)
            
            found = full_data[patente_search_cond]
            
            if not found.empty:
                # Buscamos coincidencias dentro de la ventana de 7 días
                for _, row in found.iterrows():
                    sbe_date_raw = row['Fecha Salida']
                    sbe_date_obj = pd.to_datetime(sbe_date_raw).date() if pd.notna(sbe_date_raw) else None

                    if sbe_date_obj:
                        time_diff = sbe_date_obj - ship.date 
                        
                        if time_diff.days >= 0 and time_diff.days <= 7:
                            match_row = row
                            match_type = "Por Patente + Fecha (Fallo Remito)"
                            break

        # --- GUARDAR RESULTADOS EN LA BASE ---
        if match_row is not None:
            matches_count += 1
            
            # Guardar Remito SBE (Guardamos el formato original SBE para referencia)
            ship.sbe_remito = str(match_row.get('Factura', ''))
            
            # Guardar Peso SBE (Convertimos a TN)
            peso_raw = float(match_row.get('Peso Neto', 0) or 0)
            ship.sbe_peso_neto = peso_raw / 1000.0 if peso_raw > 100 else peso_raw

            # Guardar Fechas
            fecha_salida_raw = match_row.get('Fecha Salida')
            if pd.notna(fecha_salida_raw):
                ship.sbe_fecha_salida = pd.to_datetime(fecha_salida_raw)
            
            # EVALUAR ESTADO AUTOMÁTICO
            peso_arenera = ship.peso_neto_arenera or 0
            peso_sbe     = ship.sbe_peso_neto or 0
            
            diff_pct = 0
            if peso_sbe > 0:
                diff_pct = abs(peso_arenera - peso_sbe) / peso_sbe * 100
            
            if diff_pct > 5:
                ship.cert_status = "Observado"
            elif match_type.startswith("Total"):
                 ship.cert_status = "Pre-Aprobado" 
            else:
                 # Si solo cruzó por remito o patente/fecha: requiere revisión manual.
                 ship.cert_status = "Observado" 

    db.session.commit()
    return (matches_count, None)