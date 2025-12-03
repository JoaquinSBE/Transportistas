import os
import pandas as pd
from app import app, db, Shipment
from sync_service import download_and_concat, prepare_dataframe, get_graph_token, normalize_remito, clean_patente

def spy_remito():
    # 1. Pedir el remito problemático
    target = input("\n🕵️ INGRESA EL NÚMERO DE REMITO QUE NO CRUZA (Ej: 27062): ").strip()
    target_norm = normalize_remito(target)
    print(f"\n--- 🔎 BUSCANDO REMITO '{target}' (Normalizado: '{target_norm}') ---")

    # 2. Buscar en TU BASE DE DATOS (Local)
    print("\n1️⃣  Buscando en TU Base de Datos (Local)...")
    with app.app_context():
        # Buscamos coincidencias aproximadas
        ships = Shipment.query.filter(Shipment.remito_arenera.like(f"%{target}%")).all()
        
        if not ships:
            print("❌ NO SE ENCONTRÓ en tu base de datos con ese número.")
            print("   (Verifique que esté cargado en 'Todos los Camiones' y que el número sea correcto)")
            return

        print(f"✅ Se encontraron {len(ships)} viajes candidatos en tu DB:")
        for s in ships:
            s_norm = normalize_remito(s.remito_arenera)
            print(f"   - ID: {s.id} | Fecha: {s.date} | Estado: '{s.status}' | Cert: '{s.cert_status}'")
            print(f"     Remito DB: '{s.remito_arenera}' -> Norm: '{s_norm}'")
            
            # Chequeo de condiciones para ser 'Pendiente'
            es_pendiente = (
                s.cert_status != 'Certificado' and 
                s.status in ['En viaje', 'Salió', 'Llego', 'Salido a SBE']
            )
            if not es_pendiente:
                print("     ⚠️  ESTE VIAJE NO ES CONSIDERADO PENDIENTE (Por Estado o Certificado).")
                print("         (El sync lo ignora por seguridad).")

    # 3. Buscar en SBE (Excel Online/Histórico)
    print("\n2️⃣  Buscando en SBE (Excel)... Descargando...")
    token = get_graph_token()
    
    # Descargamos todo
    links_hist = [os.getenv("SHAREPOINT_LINK_1"), os.getenv("SHAREPOINT_LINK_2")]
    links_online = [os.getenv("SHAREPOINT_LINK_ONLINE_1"), os.getenv("SHAREPOINT_LINK_ONLINE_2")]
    
    df_h = download_and_concat(links_hist, token, "Histórico", "Reporte")
    df_o = download_and_concat(links_online, token, "Online", "Reporte Diario")
    
    # Preparamos
    df_h = prepare_dataframe(df_h, "Histórico")
    df_o = prepare_dataframe(df_o, "Online")
    
    raw_df = pd.concat([df_h, df_o], ignore_index=True)
    
    # Búsqueda en el DataFrame
    # Normalizamos la columna factura para buscar
    raw_df['Factura_Str'] = raw_df['Factura'].astype(str)
    raw_df['Factura_Norm'] = raw_df['Factura'].apply(normalize_remito)
    
    # Filtramos
    found = raw_df[raw_df['Factura_Norm'] == target_norm]
    
    if found.empty:
        print(f"❌ EL REMITO '{target}' NO APARECE EN LOS EXCEL DE SBE.")
        print("   Posibles causas:")
        print("   - No está cargado en el Excel.")
        print("   - Está cargado pero el 'Estado' no es 'Ingreso'.")
        print("   - No tiene 'Fecha Salida' ni 'Fecha Entrada' (ambas vacías).")
        
        # Intento de ayuda: buscar por patente
        print("\n   🔎 Buscando si aparece por PATENTE en los Excel...")
        if ships:
            pat = clean_patente(ships[0].tractor)
            found_pat = raw_df[raw_df['Patente Tractor'].apply(clean_patente) == pat]
            if not found_pat.empty:
                print(f"   ⚠️  ¡Encontré la patente {pat}! Pero con estos remitos:")
                print(found_pat[['Factura', 'Fecha Salida', 'Estado']].head())
    else:
        print(f"✅ ¡ENCONTRADO EN EXCEL! ({len(found)} filas)")
        for idx, row in found.iterrows():
            print(f"\n   📄 Fila Excel #{idx}:")
            print(f"      - Factura Raw: '{row.get('Factura')}'")
            print(f"      - Estado: '{row.get('Estado')}' (¿Es 'Ingreso'?)")
            print(f"      - Fecha Salida (Original): {row.get('Fecha Salida')}")
            print(f"      - Fecha Entrada: {row.get('Fecha Entrada')}")
            print(f"      - Fecha Calculada (__temp_date): {row.get('__temp_date')}")
            
            # Análisis de cruce
            if ships:
                s = ships[0] # Tomamos el primero de tu DB para comparar
                ship_date = s.date
                excel_date = row.get('__temp_date')
                
                print(f"      ------------------------------------------------")
                print(f"      ⚖️  COMPARACIÓN DE CRUCE:")
                
                # Check Fecha
                if excel_date:
                    diff = (excel_date - ship_date).days
                    print(f"      - Fecha DB: {ship_date} | Fecha Excel: {excel_date}")
                    print(f"      - Diferencia días: {diff}")
                    if diff < -2:
                        print("      🔴  FALLA: La fecha de Excel es muy vieja (Anterior a la salida).")
                    elif diff > 15: # Umbral muy amplio
                         print("      🔴  FALLA: La fecha de Excel es muy futura.")
                    else:
                        print("      🟢  FECHA OK.")
                else:
                    print("      🔴  FALLA: Fecha inválida en Excel.")

                # Check Remito
                print(f"      - Remito Norm DB: {normalize_remito(s.remito_arenera)}")
                print(f"      - Remito Norm Excel: {row.get('Factura_Norm')}")
                print("      🟢  REMITO OK." if normalize_remito(s.remito_arenera) == row.get('Factura_Norm') else "      🔴  REMITO DISTINTO.")

if __name__ == "__main__":
    spy_remito()