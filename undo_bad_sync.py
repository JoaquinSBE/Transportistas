from app import app, db, Shipment
from sqlalchemy import or_

def force_reset():
    print("--- 🧨 INICIANDO BORRADO TOTAL DE DATOS SBE (NO CERTIFICADOS) ---")
    
    with app.app_context():
        # Seleccionamos TODO lo que no esté certificado y tenga algo de SBE cargado
        # (Sin importar si fue hace 10 minutos o 2 días)
        targets = db.session.query(Shipment).filter(
            Shipment.cert_status != 'Certificado',
            or_(
                Shipment.sbe_remito != None,
                Shipment.sbe_peso_neto != None
            )
        ).all()
        
        count = 0
        for s in targets:
            # Borramos todo rastro de SBE
            s.sbe_remito = None
            s.sbe_peso_neto = None
            s.sbe_fecha_salida = None
            s.sbe_fecha_llegada = None
            s.sbe_patente = None
            
            # Volvemos a estado virgen
            s.cert_status = "Pendiente"
            s.observation_reason = None
            
            count += 1
            
        db.session.commit()
        print(f"✅ ¡LIMPIEZA COMPLETA! Se resetearon {count} viajes.")
        print("   Ahora puedes correr el Sync V7 y solo cruzará lo correcto.")

if __name__ == "__main__":
    force_reset()