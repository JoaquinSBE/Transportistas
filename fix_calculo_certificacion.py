# recalculate_batch.py
import sys
import os
from datetime import date

# Importar contexto de Flask
sys.path.append(os.getcwd())
from app import app, db, Shipment, User, get_config

def recalcular_masivo():
    print("\n=== 🔄 RECÁLCULO MASIVO DE CERTIFICACIONES 🔄 ===")
    print("Este script corregirá los montos guardados en la base de datos")
    print("aplicando la lógica actual (Salida/Llegada y IVA 0.21).\n")

    # 1. Pedir rango de fechas de CERTIFICACIÓN
    start_str = input("Fecha Inicio Certificación (YYYY-MM-DD): ").strip()
    end_str   = input("Fecha Fin Certificación    (YYYY-MM-DD): ").strip()

    try:
        d_start = date.fromisoformat(start_str)
        d_end   = date.fromisoformat(end_str)
    except ValueError:
        print("❌ Fecha inválida.")
        return

    with app.app_context():
        # 2. Buscar viajes certificados en ese rango
        shipments = Shipment.query.filter(
            Shipment.cert_status == "Certificado",
            Shipment.cert_fecha >= d_start,
            Shipment.cert_fecha <= d_end
        ).order_by(Shipment.cert_fecha.asc(), Shipment.id.asc()).all()

        if not shipments:
            print("❌ No se encontraron viajes certificados en esas fechas.")
            return

        print(f"\n✅ Se encontraron {len(shipments)} viajes. Analizando...\n")
        
        # Configuración de tolerancia
        conf = get_config()
        tol_tn = conf.tolerance_kg / 1000.0
        
        cambios = []

        print(f"{'ID':<6} | {'Transp':<10} | {'Tipo':<8} | {'Peso Calc':<10} | {'Neto ANT':<12} | {'Neto NUEVO':<12} | {'DIFERENCIA'}")
        print("-" * 100)

        total_diff = 0.0

        for s in shipments:
            # --- LÓGICA DE RECÁLCULO (LA CORRECTA) ---
            
            # A. Definir Pesos
            peso_salida = s.peso_neto_arenera or 0
            # Peso llegada: Prioridad Manual (final) > SBE > Salida
            peso_llegada = s.final_peso if (s.final_peso and s.final_peso > 0) else (s.sbe_peso_neto or peso_salida)

            # B. Definir Precios (Mantenemos el precio unitario congelado si existe, sino el actual)
            p_flete = s.frozen_flete_price if s.frozen_flete_price is not None else (s.transportista.custom_price or 0)
            p_arena = s.arenera.custom_price or 0

            # C. Lógica Salida vs Llegada
            tn_base = 0.0
            merma_money = 0.0
            tipo_calculo = "???"

            if s.arenera.cert_type == 'salida':
                # Paga por Salida, sin merma
                tn_base = peso_salida
                merma_money = 0.0
                tipo_calculo = "SALIDA"
            else:
                # Paga por Llegada (Estándar)
                tn_base = peso_llegada
                tipo_calculo = "LLEGADA"
                
                # Cálculo Merma
                diff = peso_salida - peso_llegada
                if diff > tol_tn:
                    merma_money = (diff - tol_tn) * p_arena

            # D. Matemáticas Finales
            nuevo_neto = (tn_base * p_flete) - merma_money
            # CORRECCIÓN CRÍTICA DE IVA (0.21)
            nuevo_iva = max(0, nuevo_neto * 0.21)

            # Valores Antiguos para comparar
            viejo_neto = s.frozen_flete_neto or 0
            diferencia = nuevo_neto - viejo_neto
            
            # Mostrar fila
            print(f"{s.id:<6} | {s.transportista.username[:10]:<10} | {tipo_calculo:<8} | {tn_base:<10.2f} | ${viejo_neto:<11,.0f} | ${nuevo_neto:<11,.0f} | {diferencia:+.0f}")

            # Guardamos datos en memoria para aplicar después
            cambios.append({
                'shipment': s,
                'tn_base_final': tn_base, # Dato informativo
                'neto': nuevo_neto,
                'iva': nuevo_iva,
                'merma': merma_money,
                'price': p_flete
            })
            total_diff += diferencia

        print("-" * 100)
        print(f"TOTAL DIFERENCIA NETO EN LOTE: ${total_diff:,.2f}")

        # 3. Confirmación
        confirm = input("\n¿Desea aplicar estos cambios a la base de datos? (si/no): ").lower()
        
        if confirm == "si":
            for item in cambios:
                s = item['shipment']
                
                # ACTUALIZACIÓN DB
                s.frozen_flete_neto  = item['neto']
                s.frozen_flete_iva   = item['iva']
                s.frozen_merma_money = item['merma']
                s.frozen_flete_price = item['price']
                
                # Aseguramos que el peso final refleje lo calculado
                # (Si era llegada, ponemos llegada; si salida, ponemos salida? 
                # Mejor dejamos final_peso como el peso físico real de llegada para historial)
                if not s.final_peso and s.sbe_peso_neto:
                    s.final_peso = s.sbe_peso_neto

            db.session.commit()
            print("\n✅ ¡Cambios guardados exitosamente!")
            print("Ahora puedes volver a generar el PDF o ver la web y los montos estarán corregidos.")
        else:
            print("\n❌ Operación cancelada. No se hicieron cambios.")

if __name__ == "__main__":
    recalcular_masivo()