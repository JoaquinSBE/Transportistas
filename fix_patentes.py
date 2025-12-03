from app import app, db, Shipment, Chofer

def fix_spaces():
    print("--- 🧹 INICIANDO LIMPIEZA DE ESPACIOS EN PATENTES ---")
    
    with app.app_context():
        # 1. Limpiar Viajes (Shipments)
        shipments = db.session.query(Shipment).all()
        count_s = 0
        for s in shipments:
            changed = False
            
            # Limpiar Tractor
            if s.tractor and " " in s.tractor:
                s.tractor = s.tractor.replace(" ", "").upper()
                changed = True
            
            # Limpiar Batea
            if s.trailer and " " in s.trailer:
                s.trailer = s.trailer.replace(" ", "").upper()
                changed = True
                
            if changed:
                count_s += 1

        # 2. Limpiar Choferes Guardados (Chofer)
        choferes = db.session.query(Chofer).all()
        count_c = 0
        for c in choferes:
            changed = False
            
            if c.tractor and " " in c.tractor:
                c.tractor = c.tractor.replace(" ", "").upper()
                changed = True
                
            if c.trailer and " " in c.trailer:
                c.trailer = c.trailer.replace(" ", "").upper()
                changed = True
                
            if changed:
                count_c += 1

        db.session.commit()
        
        print(f"✅ LISTO.")
        print(f"   - Se corrigieron {count_s} Viajes.")
        print(f"   - Se corrigieron {count_c} Choferes frecuentes.")

if __name__ == "__main__":
    fix_spaces()