# cron_sync_runner.py
import os
from app import app, db, Shipment # Importamos los elementos necesarios de la app principal
from datetime import datetime
from sync_service import run_sbe_sync

if __name__ == "__main__":
    # 1. Inicializar el contexto de la aplicación
    with app.app_context():
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Iniciando tarea de sincronización SBE...")
        
        # 2. Ejecutar la función de sincronización y cruce
        matches_count, error = run_sbe_sync(db, Shipment)
        
        # 3. Mostrar el resultado
        if error:
            print(f"ERROR: {error}")
        else:
            print(f"Sincronización completada. {matches_count} envíos actualizados.")