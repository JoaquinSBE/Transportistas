from app import app, db, Shipment

def reset_specific_certifications():
    print("--- 🎯 RESET DE CERTIFICACIONES POR REMITO ---")
    print("Ingresa los números de remito (origen) que deseas volver a procesar.")
    
    # 1. Pedir input
    raw_input = input("Remitos (separados por coma, ej: 1234, 5678): ").strip()
    
    if not raw_input:
        print("❌ No ingresaste ningún número.")
        return

    # Limpiar la lista de remitos ingresados
    target_remitos = [r.strip() for r in raw_input.split(',') if r.strip()]

    # 2. Buscar y Ejecutar
    with app.app_context():
        # Buscamos coincidencias que estén Certificadas
        shipments = db.session.query(Shipment).filter(
            Shipment.remito_arenera.in_(target_remitos),
            Shipment.cert_status == 'Certificado'
        ).all()

        if not shipments:
            print(f"❌ No se encontraron viajes 'Certificados' con esos remitos: {target_remitos}")
            return

        # 3. Mostrar resumen antes de actuar
        print(f"\n⚠️  SE ENCONTRARON {len(shipments)} VIAJES PARA RESETEAR:")
        for s in shipments:
            print(f"   - ID: {s.id} | Remito: {s.remito_arenera} | Fecha: {s.date}")

        confirm = input("\n¿Confirmar reset de estos viajes? (Escribe 'SI'): ")
        if confirm != "SI":
            print("Cancelado.")
            return

        # 4. Aplicar Reset
        count = 0
        for s in shipments:
            # Restaurar Estado (según si tenía cruce SBE o no)
            if s.sbe_remito:
                if s.observation_reason:
                    s.cert_status = "Observado"
                else:
                    s.cert_status = "Pre-Aprobado"
            else:
                s.cert_status = "Pendiente"

            # Limpiar datos de Certificación
            s.cert_fecha = None
            
            # Limpiar datos Financieros (Descongelar)
            s.final_remito = None
            s.final_peso = None
            
            s.frozen_flete_price = None
            s.frozen_arena_price = None
            s.frozen_merma_money = 0.0
            s.frozen_flete_neto = None
            s.frozen_flete_iva = 0.0
            
            count += 1
        
        db.session.commit()
        print(f"\n✅ ¡LISTO! {count} viajes han vuelto a la bandeja de entrada.")

if __name__ == "__main__":
    reset_specific_certifications()