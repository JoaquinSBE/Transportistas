from app import app, db, Shipment

def reset_certificaciones():
    print("--- ⚠️  RESET DE CERTIFICACIONES ---")
    print("Este script devolverá los viajes 'Certificados' al estado 'Pendiente/Pre-Aprobado'.")
    print("Se eliminarán los precios congelados y las fechas de pago calculadas.")
    
    # 1. Confirmación de seguridad
    confirm = input("¿Estás seguro de que quieres BORRAR TODAS las certificaciones? (escribe 'SI'): ")
    if confirm != "SI":
        print("Cancelado.")
        return

    # 2. Ejecución
    with app.app_context():
        # Buscamos solo los que están Certificados
        certificados = db.session.query(Shipment).filter(
            Shipment.cert_status == 'Certificado'
        ).all()
        
        count = 0
        for s in certificados:
            # A. Restaurar Estado
            # Si tiene datos de SBE, vuelve a "Pre-Aprobado" (o Observado si tenía notas),
            # si no, vuelve a "Pendiente".
            if s.sbe_remito:
                if s.observation_reason:
                    s.cert_status = "Observado"
                else:
                    s.cert_status = "Pre-Aprobado"
            else:
                s.cert_status = "Pendiente"

            # B. Limpiar datos de Certificación
            s.cert_fecha = None
            
            # C. Limpiar datos Financieros (Descongelar)
            s.final_remito = None
            s.final_peso = None
            
            s.frozen_flete_price = None
            s.frozen_arena_price = None
            s.frozen_merma_money = 0.0
            s.frozen_flete_neto = None
            s.frozen_flete_iva = 0.0
            
            count += 1
        
        db.session.commit()
        print(f"✅ ¡LISTO! Se 'des-certificaron' {count} viajes.")
        print("   Ahora aparecen nuevamente en la bandeja de 'Pendientes' para procesar.")

if __name__ == "__main__":
    reset_certificaciones()