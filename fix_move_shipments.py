from app import app, db, Shipment, User

def reassign_shipments():
    print("--- 🚚 REASIGNACIÓN DE VIAJES (CAMBIO DE TRANSPORTISTA) ---")
    print("Este script mueve la titularidad de los viajes basándose en el número de remito.")
    
    # 1. Pedir Remitos
    raw_input = input("\n📝 Ingresa los números de remito (separados por coma): ").strip()
    
    if not raw_input:
        print("❌ No ingresaste datos.")
        return

    target_remitos = [r.strip() for r in raw_input.split(',') if r.strip()]

    with app.app_context():
        # 2. Buscar los viajes
        shipments = db.session.query(Shipment).filter(
            Shipment.remito_arenera.in_(target_remitos)
        ).all()

        if not shipments:
            print("❌ No se encontraron viajes con esos remitos en la base de datos.")
            return

        # 3. Mostrar resumen de lo encontrado
        print(f"\n📋 SE ENCONTRARON {len(shipments)} VIAJES:")
        print(f"{'ID':<6} | {'FECHA':<12} | {'REMITO':<15} | {'DUEÑO ACTUAL'}")
        print("-" * 60)
        
        for s in shipments:
            owner_name = s.transportista.username if s.transportista else "Sin Asignar"
            print(f"{s.id:<6} | {s.date.strftime('%d/%m/%Y'):<12} | {s.remito_arenera:<15} | {owner_name}")

        # 4. Pedir el nuevo dueño
        new_owner_name = input("\n👤 Ingresa el NOMBRE DE USUARIO del Nuevo Transportista: ").strip()
        
        # Validar usuario
        # Usamos ilike para que no importe mayúsculas/minúsculas
        new_owner = User.query.filter(
            User.username.ilike(new_owner_name), 
            User.tipo == 'transportista'
        ).first()

        if not new_owner:
            print(f"❌ Error: El usuario '{new_owner_name}' no existe o no es de tipo 'transportista'.")
            return

        print(f"\n>>> SE MOVERÁN ESTOS {len(shipments)} VIAJES A: {new_owner.username} (ID: {new_owner.id})")

        # 5. Confirmación final
        confirm = input("¿Estás seguro? Escribe 'SI' para confirmar: ")
        
        if confirm != "SI":
            print("Cancelado. No se hicieron cambios.")
            return

        # 6. Ejecutar cambios
        count = 0
        for s in shipments:
            # Cambiamos el dueño del viaje
            s.transportista_id = new_owner.id
            
            # Opcional: Si el viaje fue cargado por un operador, también podemos actualizar eso
            # s.operador_id = new_owner.id 
            
            count += 1
        
        db.session.commit()
        print(f"✅ ¡ÉXITO! {count} viajes fueron transferidos a {new_owner.username}.")

if __name__ == "__main__":
    reassign_shipments()