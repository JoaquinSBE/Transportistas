# fix_bad_sync.py
from app import app, db, Shipment

def clean_premature_matches():
    with app.app_context():
        print("--- Buscando cruces prematuros ---")
        
        # Buscar viajes que tienen datos de SBE pero NO tienen remito de arenera
        bad_ships = db.session.query(Shipment).filter(
            (Shipment.sbe_remito != None) | (Shipment.sbe_fecha_llegada != None), # Tienen datos SBE
            (Shipment.remito_arenera == None) | (Shipment.remito_arenera == "")   # Pero NO tienen salida
        ).all()

        count = 0
        for s in bad_ships:
            print(f"Limpiando ID #{s.id} - Patente {s.tractor}")
            s.sbe_remito = None
            s.sbe_peso_neto = None
            s.sbe_fecha_salida = None
            s.sbe_fecha_llegada = None
            s.sbe_patente = None
            s.cert_status = "Pendiente" # Lo devolvemos a pendiente limpio
            s.observation_reason = None
            count += 1
        
        db.session.commit()
        print(f"--- ¡Listo! Se corrigieron {count} viajes. ---")

if __name__ == "__main__":
    clean_premature_matches()