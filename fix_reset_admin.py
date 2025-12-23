from app import app, db, User
from werkzeug.security import generate_password_hash

def force_reset_admin():
    # 1. Pedir nueva clave
    print("--- RESET DE EMERGENCIA PARA ADMIN ---")
    new_pass = input("Ingresa la nueva contraseña para 'admin': ").strip()
    
    if not new_pass:
        print("Cancelado. No ingresaste contraseña.")
        return

    # 2. Conectar a DB
    with app.app_context():
        # Buscamos al usuario admin (asegúrate que tu usuario se llame 'admin' o el que uses)
        u = db.session.query(User).filter(User.username == 'admin').first()
        
        if u:
            u.password_hash = generate_password_hash(new_pass)
            db.session.commit()
            print(f"✅ ¡ÉXITO! La contraseña de '{u.username}' ha sido actualizada.")
        else:
            print("❌ ERROR: El usuario 'admin' no existe en la base de datos.")

if __name__ == "__main__":
    force_reset_admin()