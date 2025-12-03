import os
import pandas as pd
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from sqlalchemy import text
from app import app, db, Shipment
from sync_service import download_and_concat, prepare_dataframe, clean_patente, normalize_remito

ARG_TZ = ZoneInfo("America/Argentina/Buenos_Aires")

def run_emergency_sync():
    print("--- 🚨 INICIO SYNC DE EMERGENCIA (SOLO PATENTE) ---")
    print("⚠️  Regla: Solo procesa viajes vacíos y registros SBE no utilizados.")

    with app.app_context():
        
        # 1. OBTENER VIAJES PENDIENTES "HUÉRFANOS"
        # Aquellos que NO tienen datos SBE cargados (sbe_remito es Null)
        orphans = db.session.query(Shipment).filter(
            Shipment.cert_status != 'Certificado',
            Shipment.status.in_(['En viaje', 'Salió', 'Llego', 'Salido a SBE']),
            (Shipment.sbe_remito == None) | (Shipment.sbe_remito == "")
        ).all()

        if not orphans:
            print("✅ No hay viajes pendientes sin cruzar. No es necesario correr esto.")
            return

        print(f"📋 Viajes locales sin cruzar encontrados: {len(orphans)}")

        # 2. OBTENER LISTA NEGRA (REMITOS SBE YA USADOS)
        # Consultamos todos los sbe_remito que SI existen en la base para no volver a usarlos
        used_remitos_query = db.session.query(Shipment.sbe_remito).filter(
            Shipment.sbe_remito != None,
            Shipment.sbe_remito != ""
        ).all()
        # Convertimos a un Set para búsqueda rápida (y normalizamos por si acaso)
        used_remitos_set = {normalize_remito(r[0]) for r in used_remitos_query}
        print(f"🚫 Registros SBE ya utilizados en DB: {len(used_remitos_set)}")

        # 3. DESCARGAR Y PREPARAR EXCEL
        # Reutilizamos las funciones de sync_service para no duplicar lógica
        token = None
        from sync_service import get_graph_token # Import local
        token = get_graph_token()
        
        if not token:
            print("❌ Error de credenciales Azure.")
            return

        links_hist = [os.getenv("SHAREPOINT_LINK_1"), os.getenv("SHAREPOINT_LINK_2")]
        links_online = [os.getenv("SHAREPOINT_LINK_ONLINE_1"), os.getenv("SHAREPOINT_LINK_ONLINE_2")]

        print("📥 Descargando bases...")
        df_h = download_and_concat(links_hist, token, "Histórico", "Reporte")
        df_o = download_and_concat(links_online, token, "Online", "Reporte Diario")

        df_h = prepare_dataframe(df_h, "Histórico")
        df_o = prepare_dataframe(df_o, "Online")

        # Filtrado de fechas (Igual que el principal para consistencia)
        hoy_arg = datetime.now(ARG_TZ).date()
        fecha_buffer = hoy_arg - timedelta(days=15)

        if not df_h.empty: df_h = df_h[df_h['__temp_date'] < hoy_arg].copy()
        if not df_o.empty: df_o = df_o[df_o['__temp_date'] >= fecha_buffer].copy()

        raw_df = pd.concat([df_h, df_o], ignore_index=True)
        
        if raw_df.empty:
            print("❌ No hay datos en Excel.")
            return

        # 4. LIMPIEZA Y FILTRO DE DISPONIBILIDAD
        # Normalizamos factura para comparar con la lista negra
        if 'Factura' not in raw_df.columns: return
        
        raw_df['Remito_Norm'] = raw_df['Factura'].apply(normalize_remito)
        
        # [FILTRO CRÍTICO] Eliminamos del DataFrame los remitos que YA están en la DB
        initial_len = len(raw_df)
        raw_df = raw_df[~raw_df['Remito_Norm'].isin(used_remitos_set)].copy()
        print(f"📉 Filas SBE disponibles (No usadas): {len(raw_df)} (Descartadas: {initial_len - len(raw_df)})")

        # Preparar claves
        raw_df['Key_Fecha'] = pd.to_datetime(raw_df['Fecha Salida'], dayfirst=True, errors='coerce').dt.date
        raw_df['Key_Patente'] = raw_df['Patente Tractor'].apply(clean_patente)
        raw_df['Peso Neto'] = pd.to_numeric(raw_df['Peso Neto'], errors='coerce').fillna(0)
        
        # Eliminar sin fecha
        raw_df = raw_df.dropna(subset=['Key_Fecha'])

        # 5. EL CRUCE DE EMERGENCIA (SOLO PATENTE)
        matches_count = 0
        
        for ship in orphans:
            ship_trac = clean_patente(ship.tractor)
            ship_date = ship.date
            
            if not ship_trac: continue

            # Filtro 1: Fecha (Llegada SBE >= Salida Local)
            candidates = raw_df[raw_df['Key_Fecha'] >= ship_date]
            
            if candidates.empty: continue

            # Filtro 2: Patente Tractor (Estricto)
            candidates = candidates[candidates['Key_Patente'] == ship_trac].copy()
            
            if candidates.empty: continue

            # Filtro 3: Proximidad (El más cercano en fecha, max 4 días)
            candidates['diff'] = (pd.to_datetime(candidates['Key_Fecha']) - pd.to_datetime(ship_date)).dt.days
            candidates = candidates.sort_values('diff')
            
            best_match = candidates.iloc[0]
            
            if best_match['diff'] <= 4:
                # ¡MATCH ENCONTRADO!
                matches_count += 1
                
                # Datos
                ship.sbe_remito = str(best_match['Remito_Norm'])
                ship.sbe_patente = str(best_match.get('Patente Tractor', ''))
                
                w_raw = float(best_match.get('Peso Neto', 0) or 0)
                ship.sbe_peso_neto = w_raw / 1000.0 if w_raw > 100 else w_raw
                
                # Fechas
                fs = best_match.get('Fecha Salida')
                if pd.notna(fs):
                    ship.sbe_fecha_salida = pd.to_datetime(fs, dayfirst=True, errors='coerce')
                
                fl = best_match.get('Fecha Entrada')
                if pd.notna(fl):
                    ship.sbe_fecha_llegada = pd.to_datetime(fl, dayfirst=True, errors='coerce')

                # ESTADO: OBSERVADO (Amarillo)
                # Avisamos que fue un rescate por patente
                ship.cert_status = "Observado"
                ship.observation_reason = "Match Emergencia (Solo Patente)"
                
                # IMPORTANTE: Agregar este remito al set de usados para que el siguiente loop no lo tome
                # (Aunque en este script no re-consultamos, es buena práctica si la lógica cambiara)
                # used_remitos_set.add(ship.sbe_remito) 

        db.session.commit()
        print(f"🚀 FIN DEL RESCATE. Se cruzaron {matches_count} viajes por patente.")

if __name__ == "__main__":
    run_emergency_sync()