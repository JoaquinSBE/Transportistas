# move_date.py
import sys
import os
from datetime import datetime

# Configuración para importar app.py
sys.path.append(os.getcwd())

from app import app, db, Shipment, or_

def cambiar_fecha_certificacion():
    print("\n=== 📅 SCRIPT PARA MOVER FECHA DE CERTIFICACIÓN 📅 ===")
    
    while True:
        remito = input("\nIngrese el N° de Remito a buscar (o 'q' para salir): ").strip()
        if remito.lower() == 'q':
            break

        with app.app_context():
            # 1. Buscar coincidencias
            resultados = Shipment.query.filter(
                or_(
                    Shipment.remito_arenera.ilike(f"%{remito}%"),
                    Shipment.sbe_remito.ilike(f"%{remito}%"),
                    Shipment.final_remito.ilike(f"%{remito}%")
                )
            ).order_by(Shipment.date.desc()).all()

            if not resultados:
                print("❌ No se encontraron viajes con ese remito.")
                continue

            # 2. Mostrar resultados
            print(f"\n✅ Se encontraron {len(resultados)} coincidencias:\n")
            print(f"{'ID':<6} | {'F. VIAJE':<10} | {'F. CERTIF':<12} | {'CHOFER':<20} | {'REMITO':<15}")
            print("-" * 80)
            
            for r in resultados:
                f_viaje = r.date.strftime("%d/%m/%Y")
                f_cert = r.cert_fecha.strftime("%d/%m/%Y") if r.cert_fecha else "PENDIENTE"
                rem_mostrar = r.final_remito or r.remito_arenera or "-"
                print(f"{r.id:<6} | {f_viaje:<10} | {f_cert:<12} | {r.chofer[:19]:<20} | {rem_mostrar:<15}")
            print("-" * 80)

            # 3. Seleccionar ID
            try:
                sel_id = input("\nIngrese el ID del viaje a modificar: ").strip()
                if not sel_id: continue
                shipment_id = int(sel_id)
            except ValueError:
                print("❌ ID inválido.")
                continue

            s = db.session.get(Shipment, shipment_id)
            if not s:
                print("❌ Viaje no encontrado.")
                continue

            # 4. Pedir nueva fecha
            print(f"\nEstás editando el viaje #{s.id}. Fecha Cert. actual: {s.cert_fecha}")
            nueva_fecha_str = input("Ingrese la NUEVA FECHA DE CERTIFICACIÓN (YYYY-MM-DD): ").strip()
            
            try:
                # Convertimos string a objeto date
                nueva_fecha = datetime.strptime(nueva_fecha_str, "%Y-%m-%d").date()
                
                # APLICAR CAMBIO
                s.cert_fecha = nueva_fecha
                
                # Si no estaba certificado, forzamos el estado (opcional, pero seguro)
                if s.cert_status != "Certificado":
                    print("⚠️ Aviso: El viaje no estaba certificado. Se cambiará el estado a 'Certificado'.")
                    s.cert_status = "Certificado"
                
                db.session.commit()
                print(f"\n✅ ¡LISTO! Fecha de certificación actualizada al {nueva_fecha.strftime('%d/%m/%Y')}.")
                
            except ValueError:
                print("❌ Formato de fecha incorrecto. Use AÑO-MES-DIA (ej: 2023-12-25)")
            except Exception as e:
                print(f"❌ Error inesperado: {e}")

if __name__ == "__main__":
    cambiar_fecha_certificacion()