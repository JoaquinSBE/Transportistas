# fix_shipment.py
import sys
import os

# Aseguramos que pueda importar app.py desde el directorio actual
sys.path.append(os.getcwd())

# Importamos lo necesario, incluyendo or_ para la búsqueda
from app import app, db, Shipment, calculate_shipment_financials, or_

def buscar_por_remito(termino):
    """Busca viajes que coincidan parcial o totalmente con el remito indicado."""
    print(f"\n🔍 Buscando viajes con remito similar a '{termino}'...")
    
    with app.app_context():
        # Busca en los 3 campos de remito posibles
        resultados = Shipment.query.filter(
            or_(
                Shipment.remito_arenera.ilike(f"%{termino}%"),
                Shipment.sbe_remito.ilike(f"%{termino}%"),
                Shipment.final_remito.ilike(f"%{termino}%")
            )
        ).order_by(Shipment.date.desc()).all()
        
        if not resultados:
            print("❌ No se encontraron viajes con ese remito.")
            return None

        print(f"\n✅ Se encontraron {len(resultados)} coincidencias:\n")
        print(f"{'ID':<6} | {'FECHA':<10} | {'CHOFER':<20} | {'REMITO ORIGEN':<15} | {'REMITO SBE/FIN':<15} | {'ESTADO':<10}")
        print("-" * 90)
        
        for r in resultados:
            f_remito = r.remito_arenera or "-"
            s_remito = r.final_remito or r.sbe_remito or "-"
            fecha = r.date.strftime("%d/%m/%Y")
            print(f"{r.id:<6} | {fecha:<10} | {r.chofer[:19]:<20} | {f_remito:<15} | {s_remito:<15} | {r.cert_status:<10}")
        
        print("-" * 90)
        return resultados

def corregir_y_recalcular(shipment_id, nuevo_peso_real):
    """Aplica la corrección de peso y recalcula finanzas."""
    print("\n⏳ Iniciando corrección...")
    
    with app.app_context():
        s = db.session.get(Shipment, shipment_id)
        
        if not s:
            print(f"❌ Error: El viaje ID {shipment_id} no existe.")
            return

        print(f"\n📊 --- DATOS ACTUALES (ID: {s.id}) ---")
        print(f"   Chofer: {s.chofer}")
        print(f"   Peso Llegada Actual: {s.final_peso or s.sbe_peso_neto} Tn")
        print(f"   Monto Flete Congelado: ${s.frozen_flete_neto}")

        try:
            nuevo_peso = float(nuevo_peso_real)
        except ValueError:
            print("❌ Error: El peso debe ser un número (ej: 3.18).")
            return

        # Aplicamos cambios
        s.sbe_peso_neto = nuevo_peso
        s.final_peso = nuevo_peso
        s.observation_reason = "Script Corrección Manual"

        # FORZAMOS EL RECÁLCULO
        calculate_shipment_financials(s)
        db.session.commit()

        print(f"\n✅ --- ÉXITO: VIAJE ACTUALIZADO ---")
        print(f"   Nuevo Peso: {s.final_peso} Tn")
        print(f"   Nuevo Neto: ${s.frozen_flete_neto}")
        print(f"   Nuevo IVA:  ${s.frozen_flete_iva}")

if __name__ == "__main__":
    print("\n=== 🛠️ HERRAMIENTA DE CORRECCIÓN DE VIAJES 🛠️ ===")
    
    while True:
        print("\n¿Qué desea hacer?")
        print("1. Buscar ID por número de Remito")
        print("2. Corregir viaje (tengo el ID)")
        print("3. Salir")
        
        opcion = input("\nOpción [1-3]: ").strip()

        if opcion == "1":
            remito = input("Ingrese número de remito a buscar: ").strip()
            if remito:
                buscar_por_remito(remito)
        
        elif opcion == "2":
            sid = input("Ingrese el ID del viaje: ").strip()
            peso = input("Ingrese el PESO REAL de llegada (ej. 3.18): ").strip()
            
            if sid and peso:
                corregir_y_recalcular(int(sid), peso)
            else:
                print("⚠️ Faltan datos.")
                
        elif opcion == "3":
            print("Adiós.")
            break
        else:
            print("Opción inválida.")