# -*- coding: utf-8 -*-
import base64
import requests
import os
import io, csv
from datetime import datetime, date, timedelta
from functools import wraps
from sqlalchemy import func, case, text
from flask import Flask, render_template, request, redirect, url_for, session, flash, abort, make_response
from flask_sqlalchemy import SQLAlchemy
from werkzeug.security import generate_password_hash, check_password_hash
from openpyxl import Workbook
import sync_service
import threading
import time
from zoneinfo import ZoneInfo
from xhtml2pdf import pisa
from flask_mail import Mail, Message

# ----------------------------
# CONFIGURACIÓN ZONA HORARIA
# ----------------------------
# Definimos la zona horaria oficial para toda la app
ARG_TZ = ZoneInfo("America/Argentina/Buenos_Aires")

def get_arg_now():
    """Devuelve datetime actual en Argentina"""
    return datetime.now(ARG_TZ)

def get_arg_today():
    """Devuelve date actual en Argentina"""
    return datetime.now(ARG_TZ).date()

# ----------------------------
# Configuración por .env
# ----------------------------
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass

# Variables de ambiente
SECRET_KEY   = os.getenv("FLASK_SECRET_KEY") or os.getenv("SECRET_KEY", "clave-super-secreta-para-dev")
TPL_EXT      = os.getenv("TPL_EXT", ".html")
PASSWORD_LOG = os.getenv("PASSWORD_LOG", os.path.join("Data", "passwords.log"))
ADMIN_USER   = os.getenv("ADMIN_USER", "admin")
ADMIN_PASS   = os.getenv("ADMIN_PASS", "admin123")
DB_SCHEMA    = os.getenv("DB_SCHEMA", "transportistas")

# Solo PostgreSQL (sin fallback)
DATABASE_URL = (os.getenv("DATABASE_URL") or "").strip()
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL no está definido. La app requiere PostgreSQL.")

# Normalización a psycopg3 y search_path
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://","postgresql+psycopg://", 1)
elif DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = "postgresql+psycopg://" + DATABASE_URL.split("://", 1)[1]

if DATABASE_URL.startswith("postgresql+psycopg://"):
    sep = '&' if '?' in DATABASE_URL else '?'
    DATABASE_URL = f"{DATABASE_URL}{sep}options=-csearch_path%3D{DB_SCHEMA},public"


# ----------------------------
# Inicialización Flask / DB
# ----------------------------
app = Flask(__name__, template_folder="templates")
app.secret_key = SECRET_KEY

app.jinja_env.globals.update(get_arg_today=get_arg_today)

app.config.update(
    SQLALCHEMY_DATABASE_URI=DATABASE_URL,
    SQLALCHEMY_TRACK_MODIFICATIONS=False,
    SQLALCHEMY_ENGINE_OPTIONS={
        "pool_size": 5,
        "max_overflow": 5,
        "pool_pre_ping": True,
        "pool_recycle": 1800,
    },
)

db = SQLAlchemy(app)

# ----------------------------
# Mails
# ----------------------------

app.config['MAIL_SERVER'] = os.getenv('MAIL_SERVER')
app.config['MAIL_PORT'] = int(os.getenv('MAIL_PORT', 587))
app.config['MAIL_USE_TLS'] = os.getenv('MAIL_USE_TLS') == 'True'
app.config['MAIL_USERNAME'] = os.getenv('MAIL_USERNAME')
app.config['MAIL_PASSWORD'] = os.getenv('MAIL_PASSWORD')
app.config['MAIL_DEFAULT_SENDER'] = os.getenv('MAIL_DEFAULT_SENDER')

# Log “seguro” del DSN (sin credenciales)
_safe = DATABASE_URL.split("@", 1)[-1] if "@" in DATABASE_URL else DATABASE_URL
app.logger.info(
    "DB URI efectiva → postgresql://***:***@" + _safe
    if "postgresql+psycopg" in DATABASE_URL else
    "DB URI efectiva → " + _safe
)

# Aseguro carpeta para el log de contraseñas
os.makedirs(os.path.dirname(PASSWORD_LOG), exist_ok=True)

# Helper de templates con extensión configurable
def tpl(name: str) -> str:
    return f"{name}{TPL_EXT}"

def _resolve_range(data: dict):
    rng = (data.get('range') or '').strip().lower()
    dfrom = data.get('from')
    dto   = data.get('to')
    if dfrom and dto:
        try:
            start = date.fromisoformat(dfrom)
            end   = date.fromisoformat(dto)
            if end < start:
                start, end = end, start
            return start, end
        except Exception:
            pass
    hoy = get_arg_today()
    if rng in ('', 'today'):
        return hoy, hoy
    if rng == 'week':
        return hoy - timedelta(days=6), hoy
    if rng == 'month':
        start = hoy.replace(day=1)
        if start.month == 12:
            end = start.replace(year=start.year + 1, month=1, day=1) - timedelta(days=1)
        else:
            end = start.replace(month=start.month + 1, day=1) - timedelta(days=1)
        return start, end
    return hoy, hoy

def get_maestro_id(user_id):
    """Retorna el ID del maestro (padre) o el ID del usuario si es maestro."""
    u = db.session.get(User, user_id)
    if not u:
        return None
    if u.parent_id:
        return u.parent_id
    return u.id

def get_family_ids(user_id):
    """Devuelve una lista con el ID del maestro y todos sus sub-usuarios."""
    maestro_id = get_maestro_id(user_id)
    if not maestro_id:
        return []

    sub_users = db.session.query(User.id).filter_by(parent_id=maestro_id).all()
    
    ids = [maestro_id] + [u.id for u in sub_users]
    return ids

def norm_username(s: str) -> str:
    return (s or "").strip().lower()

def calcular_cuil(dni_str: str, gender: str | None) -> str:
    digits = ''.join(ch for ch in (dni_str or '') if ch.isdigit()).zfill(8)
    if len(digits) != 8:
        return ""
    g = (gender or "").upper()
    pref = 20 if g == "M" else (27 if g == "F" else 23)
    weights = [5,4,3,2,7,6,5,4,3,2]
    def calc_dv(prefijo: int):
        base = f"{prefijo:02d}{digits}"
        total = sum(int(d)*w for d, w in zip(base, weights))
        dv = 11 - (total % 11)
        if dv == 11: return 0
        if dv == 10: return None
        return dv
    dv = calc_dv(pref)
    if dv is None:
        pref = 23
        dv = calc_dv(pref)
        if dv is None:
            pref = 24
            dv = calc_dv(pref)
            if dv is None:
                return ""
    return f"{pref:02d}{digits}{dv}"

def get_config():
    conf = SystemConfig.query.first()
    if not conf:
        conf = SystemConfig()
        db.session.add(conf)
        db.session.commit()
    return conf

# ----------------------------
# MODELOS
# ----------------------------
class User(db.Model):
    __tablename__ = "user"
    id             = db.Column(db.Integer, primary_key=True)
    username       = db.Column(db.String(50), unique=True, nullable=False, index=True)
    password_hash  = db.Column(db.String(512), nullable=False)
    tipo           = db.Column(db.String(20), nullable=False) # admin, transportista, arenera, gestion
    email          = db.Column(db.String(120), nullable=True)
    custom_price   = db.Column(db.Float, default=0.0)
    cert_type      = db.Column(db.String(20), default='llegada')
    parent_id      = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=True)
    sub_users      = db.relationship("User", backref=db.backref("parent", remote_side=[id]),lazy="dynamic")
    payment_days   = db.Column(db.Integer, default=30)

    shipments_sent = db.relationship(
        "Shipment",
        foreign_keys="Shipment.transportista_id",
        backref="transportista",
        cascade="all, delete-orphan",
        lazy="dynamic",
    )
    shipments_received = db.relationship(
        "Shipment",
        foreign_keys="Shipment.arenera_id",
        backref="arenera",
        cascade="all, delete-orphan",
        lazy="dynamic",
    )
    quotas = db.relationship(
        "Quota",
        foreign_keys="Quota.transportista_id",
        backref="transportista_user",
        cascade="all, delete-orphan",
        lazy="dynamic",
    )
    shipments_operated = db.relationship(
        "Shipment",
        foreign_keys="Shipment.operador_id", # <-- NUEVA FK
        backref="operador",
        lazy="dynamic",
    )

class Shipment(db.Model):
    __tablename__ = "shipment"
    id               = db.Column(db.Integer, primary_key=True)
    transportista_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False, index=True)
    arenera_id       = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False, index=True)
    operador_id      = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False, index=True, default=0)
    date             = db.Column(db.Date, nullable=False, index=True)
    
    # Datos cargados por el Transportista
    chofer           = db.Column(db.String(100), nullable=False)
    dni              = db.Column(db.String(50), nullable=False)
    gender           = db.Column(db.String(10), nullable=False)
    tipo             = db.Column(db.String(20), nullable=False)
    tractor          = db.Column(db.String(20), nullable=False)
    trailer          = db.Column(db.String(20), nullable=False)
    
    status           = db.Column(db.String(20), nullable=False, default="En viaje", index=True)

    # --- PASO 1: Datos informados por la ARENERA (En planta) ---
    remito_arenera    = db.Column(db.String(50), nullable=True, index=True)
    peso_neto_arenera = db.Column(db.Float, nullable=True) # Ej: 30.5

    # --- PASO 2: Datos oficiales SBE (Vienen de Azure/Excel) ---
    sbe_remito        = db.Column(db.String(50), nullable=True)
    sbe_peso_neto     = db.Column(db.Float, nullable=True)
    sbe_fecha_salida  = db.Column(db.DateTime, nullable=True)
    sbe_fecha_llegada = db.Column(db.DateTime, nullable=True)
    sbe_patente       = db.Column(db.String(20), nullable=True)
    sbe_manual_override = db.Column(db.Boolean, default=False)
    
    # --- PASO 2: Certificación (Admin) ---
    cert_status       = db.Column(db.String(20), default="Pendiente") 
    cert_fecha        = db.Column(db.Date, nullable=True)
    
    # Datos Finales (Los que valen para pagar)
    final_remito      = db.Column(db.String(50), nullable=True)
    final_peso        = db.Column(db.Float, nullable=True)
    observation_reason = db.Column(db.String(100), nullable=True)

    #Snapshot financiero
    frozen_flete_price = db.Column(db.Float, nullable=True)
    frozen_arena_price = db.Column(db.Float, nullable=True)
    frozen_merma_money = db.Column(db.Float, default=0.0) 
    frozen_flete_neto  = db.Column(db.Float, default=0.0)
    frozen_flete_iva   = db.Column(db.Float, default=0.0)  
class Quota(db.Model):
    __tablename__ = "quota"
    id               = db.Column(db.Integer, primary_key=True)
    transportista_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False, index=True)
    arenera_id       = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False, index=True)
    date             = db.Column(db.Date, nullable=False, index=True)
    limit            = db.Column(db.Integer, nullable=False, default=0)
    used             = db.Column(db.Integer, nullable=False, default=0)
    arenera          = db.relationship("User", foreign_keys=[arenera_id])
    __table_args__   = (
        db.UniqueConstraint("transportista_id", "arenera_id", "date", name="uix_quota"),
    )

class Chofer(db.Model):
    __tablename__ = "chofer"
    id                  = db.Column(db.Integer, primary_key=True)
    transportista_id    = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False, index=True)
    nombre              = db.Column(db.String(100), nullable=False)
    dni                 = db.Column(db.String(50), unique=True, nullable=False, index=True) # DNI como clave unica global
    gender              = db.Column(db.String(10), nullable=True)
    tractor             = db.Column(db.String(20), nullable=True)
    trailer             = db.Column(db.String(20), nullable=True)
    tipo                = db.Column(db.String(20), nullable=True)
    
    # Relacion: un Chofer pertenece a un Transportista
    transportista       = db.relationship("User", backref=db.backref("choferes", lazy="dynamic"))

    __table_args__      = (
        db.UniqueConstraint("transportista_id", "dni", name="uix_transportista_dni"),
    )

class SystemConfig(db.Model):
    __tablename__ = "system_config"
    id             = db.Column(db.Integer, primary_key=True)
    # Logística
    tolerance_kg   = db.Column(db.Float, default=700.0)  # Tolerancia (ej: 700 kg)
    
    # Económico (Valores por defecto)
    dispatch_price = db.Column(db.Float, default=0.0)    # Precio Despacho
    sand_price     = db.Column(db.Float, default=0.0)    # Costo Arena (para multas)
    transport_price= db.Column(db.Float, default=0.0)    # Precio Transporte por Tn
    
    # Emails de notificación (opcional si quieres globales)
    admin_email    = db.Column(db.String(120), nullable=True)


# ----------------------------
# Bootstrapping DB (schema + tablas + admin)
# ----------------------------
with app.app_context():
    # crea schema si no existe
    if DB_SCHEMA and DB_SCHEMA != "public":
        try:
            db.session.execute(text(f"CREATE SCHEMA IF NOT EXISTS {DB_SCHEMA}"))
            db.session.commit()
        except Exception:
            db.session.rollback()

    # crea tablas
    db.create_all()

    # ampliar columna de hash si quedó corta de una versión anterior
    try:
        db.session.execute(text('ALTER TABLE "user" ALTER COLUMN password_hash TYPE VARCHAR(512)'))
        db.session.commit()
    except Exception:
        db.session.rollback()

    # asegura admin por defecto
    admin = db.session.query(User).filter(func.lower(User.username) == norm_username(ADMIN_USER)).first()
    if not admin:
        admin = User(
            username=norm_username(ADMIN_USER),
            password_hash=generate_password_hash(ADMIN_PASS),
            tipo="admin",
        )
        db.session.add(admin)
        db.session.commit()

# ----------------------------
# Decoradores de auth
# ----------------------------
def login_required(f):
    @wraps(f)
    def wrapped(*args, **kwargs):
        if "user_id" not in session:
            return redirect(url_for("login_page"))
        return f(*args, **kwargs)
    return wrapped

def role_required(*roles):
    def deco(f):
        @wraps(f)
        def wrapped(*args, **kwargs):
            if session.get("tipo") not in roles:
                flash("No tienes permisos para acceder a esta sección.", "error")
                return redirect(url_for("login_page"))
            
            return f(*args, **kwargs)
        return wrapped
    return deco

# ----------------------------
# LOGIN / LOGOUT
# ----------------------------
@app.route("/")
def login_page():
    return render_template(tpl("login"))

@app.route("/login", methods=["POST"])
def login():
    u = norm_username(request.form.get("usuario"))
    p = (request.form.get("clave") or "").strip()
    
    user = db.session.query(User).filter(func.lower(User.username) == u).first()
    
    if user and check_password_hash(user.password_hash, p):
        session.clear()
        session["user_id"] = user.id
        session["tipo"]    = user.tipo

        if user.tipo == "admin":
            return redirect(url_for("admin_panel"))
            
        if user.tipo == "gestion":
            return redirect(url_for("admin_dashboard"))
            
        if user.tipo == "transportista":
            return redirect(url_for("transportista_panel"))
            
        if user.tipo == "arenera":
            return redirect(url_for("arenera_panel"))
            
    flash("Usuario o clave incorrectos", "error")
    return redirect(url_for("login_page"))

@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login_page"))

# ----------------------------
# CAMBIO DE CLAVE
# ----------------------------
@app.route("/change_password", methods=["GET", "POST"])
@login_required
def change_password():
    user = User.query.get(session["user_id"])
    if request.method == "POST":
        actual = request.form["current_password"].strip()
        nueva  = request.form["new_password"].strip()
        if check_password_hash(user.password_hash, actual):
            user.password_hash = generate_password_hash(nueva, method="pbkdf2:sha256")
            db.session.commit()
            flash("Contraseña actualizada correctamente.", "success")
            return redirect(url_for(f"{user.tipo}_panel"))
        flash("Contraseña actual no coincide", "error")
    return render_template(tpl("change_password"), user=user)

# ----------------------------
# PANEL ADMIN
# ----------------------------
@app.route("/admin")
@login_required
@role_required("admin")
def admin_panel():
    today = get_arg_today()
    
    # Calcular inicio y fin de semana para filtrar cuotas
    week_start = today - timedelta(days=today.weekday())
    week_end   = week_start + timedelta(days=6)
    
    # 1. Transportistas
    t_list = []
    for t in User.query.filter_by(tipo="transportista").all():
        total_sent = Shipment.query.filter_by(transportista_id=t.id).count()
        
        # Sumar límites y usados de la semana
        q_stats = db.session.query(
            func.sum(Quota.limit).label('total'),
            func.sum(Quota.used).label('used')
        ).filter(
            Quota.transportista_id == t.id,
            Quota.date >= week_start,
            Quota.date <= week_end
        ).first()

        limit = q_stats.total or 0
        used  = q_stats.used or 0
        remaining = max(0, limit - used)

        t_list.append({
            "t": t, 
            "sent": total_sent, 
            "quota": remaining,    # El resultado (Total - Tomados)
            "quota_limit": limit,  # El Total
            "quota_used": used     # Los Tomados
        })

    # 2. Areneras (Maestras)
    a_list = []
    for a in User.query.filter_by(tipo="arenera", parent_id=None).all():
        total_ship = Shipment.query.filter_by(arenera_id=a.id).count()
        a_list.append({"a": a, "shipments": total_ship})

    # 3. Sub-Areneras
    sub_a_list = User.query.filter(User.tipo=="arenera", User.parent_id != None).all()

    # 4. Gestión
    g_list = User.query.filter_by(tipo="gestion").all()

    return render_template(
        tpl("admin_panel"),
        transportistas=t_list,
        areneras=a_list,
        sub_areneras=sub_a_list,
        gestores=g_list,      
    )

@app.route("/admin/create_user", methods=["POST"])
@login_required
@role_required("admin")
def create_user():
    uname = norm_username(request.form.get("username"))
    pwd   = (request.form.get("password") or "").strip()
    tipo  = request.form.get("tipo")
    
    # Lógica para sub-arenera
    parent_id = None
    if tipo == "sub_arenera":
        tipo = "arenera" # En base de datos sigue siendo 'arenera'
        try:
            parent_id = int(request.form.get("parent_id"))
        except (TypeError, ValueError):
            flash("Debes seleccionar una Arenera Padre para el sub-usuario.", "error")
            return redirect(url_for("admin_panel"))

    if not uname or not pwd or tipo not in ("transportista", "arenera", "gestion", "admin"):
        flash("Datos inválidos", "error")
        return redirect(url_for("admin_panel"))

    if db.session.query(User).filter(func.lower(User.username) == uname).first():
        flash("Usuario ya existe.", "error")
        return redirect(url_for("admin_panel"))

    nuevo = User(
        username=uname, 
        password_hash=generate_password_hash(pwd), 
        tipo=tipo,
        parent_id=parent_id
    )
    db.session.add(nuevo)
    db.session.commit()

    flash("Usuario creado.", "success")
    return redirect(url_for("admin_panel"))

@app.post("/admin/reset_password_admin")
@login_required
@role_required("admin")
def reset_password_admin():
    user_id = request.form.get("user_id")
    new_pass = request.form.get("new_password")
    
    if not user_id or not new_pass:
        flash("Faltan datos.", "error")
        return redirect(url_for("admin_panel"))
        
    u = db.session.get(User, user_id)
    if not u:
        flash("Usuario no encontrado.", "error")
        return redirect(url_for("admin_panel"))
        
    # Generamos el hash seguro sin guardar el texto plano
    u.password_hash = generate_password_hash(new_pass)
    db.session.commit()
    
    flash(f"Clave actualizada para {u.username}. Nueva clave temporal: {new_pass}", "success")
    return redirect(url_for("admin_panel"))

@app.route("/admin/delete_user/<int:user_id>")
@login_required
@role_required("admin")
def delete_user(user_id):
    u = User.query.get_or_404(user_id)

    if u.tipo == "arenera":
        has_shipments = Shipment.query.filter_by(arenera_id=u.id).count() > 0
        has_quotas    = Quota.query.filter_by(arenera_id=u.id).count() > 0
        if has_shipments or has_quotas:
            flash("No se puede eliminar la arenera porque tiene datos asociados (viajes/cuotas).", "error")
            return redirect(url_for("admin_panel"))

    db.session.delete(u)
    db.session.commit()
    flash("Usuario eliminado", "success")
    return redirect(url_for("admin_panel"))

@app.route("/admin/quotas/<int:transportista_id>", methods=["GET", "POST"])
@login_required
@role_required("admin")
def manage_quotas(transportista_id):
    t = User.query.get_or_404(transportista_id)
    
    # CORRECCIÓN: Solo traemos areneras MAESTRAS (parent_id es None)
    # Los sub-usuarios no tienen cuota propia, usan la del maestro.
    areneras = User.query.filter(User.tipo == "arenera", User.parent_id == None).order_by(User.username.asc()).all()

    # leer fecha inicial desde POST o GET
    if request.method == "POST":
        start_str = (request.form.get("start_date") or "").strip()
    else:
        start_str = (request.args.get("start") or "").strip()

    try:
        start_date = date.fromisoformat(start_str) if start_str else get_arg_today()
    except ValueError:
        start_date = get_arg_today()

    week_dates = [start_date + timedelta(days=i) for i in range(7)]

    if request.method == "POST":
        total_inserts = total_updates = total_deletes = 0

        try:
            for a in areneras:
                for d in week_dates:
                    key = f"q_{a.id}_{d.isoformat()}"
                    raw = (request.form.get(key) or "").strip()

                    # Vacío o "0" => borrar (si existe)
                    if raw == "" or raw == "0":
                        # Usamos db.session.query para evitar problemas de autoflush
                        q = db.session.query(Quota).filter_by(
                            transportista_id=transportista_id,
                            arenera_id=a.id,
                            date=d
                        ).first()
                        if q:
                            db.session.delete(q)
                            total_deletes += 1
                        continue

                    # Parsear límite
                    try:
                        v = int(raw)
                    except ValueError:
                        continue
                    if v < 0: v = 0

                    q = db.session.query(Quota).filter_by(
                        transportista_id=transportista_id,
                        arenera_id=a.id,
                        date=d
                    ).first()

                    if q:
                        # Update
                        q.limit = v
                        if (q.used or 0) > v:
                            q.used = v
                        total_updates += 1
                    else:
                        # Insert
                        q = Quota(
                            transportista_id=transportista_id,
                            arenera_id=a.id,
                            date=d,
                            limit=v,
                            used=0
                        )
                        db.session.add(q)
                        total_inserts += 1

            db.session.commit()
            flash(f"Cuotas guardadas: {total_inserts} nuevas, {total_updates} actualizadas.", "success")
        except Exception as e:
            db.session.rollback()
            flash(f"Error al guardar cuotas: {e}", "error")

        return redirect(url_for("manage_quotas", transportista_id=transportista_id, start=start_date.isoformat()))

    # GET: armar datos para la grilla
    existing = (
        Quota.query
        .filter(
            Quota.transportista_id == transportista_id,
            Quota.date.in_(week_dates)
        ).all()
    )
    quota_map = {(q.arenera_id, q.date): q for q in existing}

    # Contar usados (Filtrando por el ID maestro de la arenera en Shipment)
    from sqlalchemy import func
    used_rows = (
        db.session.query(
            Shipment.arenera_id,
            Shipment.date,
            func.count(Shipment.id)
        )
        .filter(
            Shipment.transportista_id == transportista_id,
            Shipment.date >= week_dates[0],
            Shipment.date <= week_dates[-1],
        )
        .group_by(Shipment.arenera_id, Shipment.date)
        .all()
    )
    used_map = {(aid, d): int(c) for (aid, d, c) in used_rows}

    return render_template(
        tpl("admin_quotas"),
        transportista=t,
        areneras=areneras,
        week_dates=week_dates,
        quota_map=quota_map,
        used_map=used_map,
        start_date=start_date
    )


# Listado general de camiones (admin)
@app.route("/admin/todos_camiones")
@login_required
@role_required("admin")
def todos_camiones():
    status        = request.args.get("status")
    transportista = (request.args.get("transportista") or "").strip().lower()
    arenera       = (request.args.get("arenera") or "").strip().lower()
    dfrom_str     = (request.args.get("from") or "").strip()
    dto_str       = (request.args.get("to") or "").strip()

    q = Shipment.query

    # fechas (opcionales)
    try:
        if dfrom_str:
            q = q.filter(Shipment.date >= date.fromisoformat(dfrom_str))
        if dto_str:
            q = q.filter(Shipment.date <= date.fromisoformat(dto_str))
    except ValueError:
        pass

    if status:
        q = q.filter(Shipment.status == status)

    shipments = q.order_by(Shipment.date.desc(), Shipment.id.desc()).all()

    if transportista:
        shipments = [s for s in shipments if transportista in s.transportista.username.lower()]
    if arenera:
        shipments = [s for s in shipments if arenera in s.arenera.username.lower()]

    return render_template(
        tpl("todos_camiones"),
        shipments=shipments, status=status, transportista=transportista, arenera=arenera,
        dfrom=dfrom_str, dto=dto_str
    )


# ----------------------------
# DASHBOARD ADMIN - API de datos
# ----------------------------
@app.post("/admin/dashboard_data")
@login_required
@role_required("admin", "gestion")
def admin_dashboard_data():
    # --- A. FILTROS Y CONSULTA ---
    data = request.get_json(silent=True) or {}
    dfrom, dto = _resolve_range(data)
    arenera_id = data.get("arenera_id")
    
    # Query Base: Traemos todo lo que esté en el rango de fechas
    base_q = db.session.query(Shipment).filter(Shipment.date >= dfrom, Shipment.date <= dto)
    
    if arenera_id and str(arenera_id) != "all":
        base_q = base_q.filter(Shipment.arenera_id == int(arenera_id))
    
    all_ships = base_q.all()

    conf = get_config()
    tol_tn = conf.tolerance_kg / 1000.0

    # --- B. LÓGICA DE ESTADOS (NUEVO RASTREO) ---
    
    # 1. Yendo a Arenera (Tramo 1)
    # Criterio: No tiene peso de carga Y no ha llegado a SBE (por si acaso)
    ships_to_source = [
        s for s in all_ships 
        if (not s.peso_neto_arenera or s.peso_neto_arenera == 0) 
        and not s.sbe_fecha_llegada
    ]
    count_to_source = len(ships_to_source)

    # 2. Yendo a SBE (Tramo 2 - Cargado)
    # Criterio: TIENE peso de carga (>0) PERO NO tiene fecha de llegada SBE
    ships_to_dest = [
        s for s in all_ships 
        if (s.peso_neto_arenera and s.peso_neto_arenera > 0) 
        and not s.sbe_fecha_llegada
    ]
    count_to_dest = len(ships_to_dest)
    tn_to_dest    = sum(s.peso_neto_arenera for s in ships_to_dest)

    # 3. Descargados en SBE (Finalizados)
    # Criterio: Tiene fecha de llegada SBE (Confirmado por balanza destino)
    ships_arrived = [s for s in all_ships if s.sbe_fecha_llegada is not None]
    count_arrived = len(ships_arrived)
    tn_arrived    = sum(s.final_peso or s.sbe_peso_neto or 0 for s in ships_arrived)
    tn_recibidas_total = sum(s.final_peso or s.sbe_peso_neto or 0 for s in all_ships)

    # Totales generales para KPI
    total_viajes = len(all_ships)
    total_moving = count_to_source + count_to_dest # Total activos en mapa
    
    tn_despachadas_total = sum(s.peso_neto_arenera or 0 for s in all_ships)
    avg_carga = (tn_arrived / count_arrived) if count_arrived > 0 else 0

    # --- C. CÁLCULOS FINANCIEROS Y GRÁFICOS ---
    daily_stats = {}
    payments_detail = []
    arenera_volumen = {}
    top_trans_map = {}
    total_costo_proyectado = 0.0

    for s in all_ships:
        # Gráfico Diario
        d_str = s.date.isoformat()
        if d_str not in daily_stats: daily_stats[d_str] = {"out": 0.0, "in": 0.0}
        
        w_out = s.peso_neto_arenera or 0
        w_in  = s.final_peso or s.sbe_peso_neto or 0
        
        daily_stats[d_str]["out"] += w_out
        daily_stats[d_str]["in"]  += w_in

        # Rankings
        a_name = s.arenera.username
        arenera_volumen[a_name] = arenera_volumen.get(a_name, 0) + w_out
        
        t_name = s.transportista.username
        top_trans_map[t_name] = top_trans_map.get(t_name, 0) + w_in

        # --- CÁLCULO DE PAGOS ---
        base_date = s.cert_fecha or s.date
        
        # A. Flete
        if w_in > 0:
            neto_flete = 0.0
            if s.frozen_flete_neto is not None:
                neto_flete = s.frozen_flete_neto
            else:
                price_flete = s.transportista.custom_price or 0
                price_arena_ref = s.arenera.custom_price or 0
                tn_base = w_out if s.arenera.cert_type == 'salida' else w_in
                merma_money = 0.0
                if s.arenera.cert_type != 'salida':
                    diff = w_out - w_in
                    if diff > tol_tn: merma_money = (diff - tol_tn) * price_arena_ref
                neto_flete = (tn_base * price_flete) - merma_money

            iva_flete = max(0, neto_flete * 1.21)
            if iva_flete > 0:
                total_costo_proyectado += iva_flete
                d_pay = base_date + timedelta(days=(s.transportista.payment_days or 30))
                payments_detail.append({
                    "raw": d_pay, "fecha": d_pay.strftime("%d/%m/%Y"),
                    "monto": iva_flete, "entidad": s.transportista.username, "tipo": "Flete",
                    "dia": ["Lun","Mar","Mié","Jue","Vie","Sáb","Dom"][d_pay.weekday()]
                })

        # B. Arena
        if w_out > 0:
            p_arena = s.frozen_arena_price if s.frozen_arena_price is not None else (s.arenera.custom_price or 0)
            neto_arena = w_out * p_arena
            iva_arena = max(0, neto_arena * 1.21)
            if iva_arena > 0:
                total_costo_proyectado += iva_arena
                d_pay = base_date + timedelta(days=(s.arenera.payment_days or 30))
                payments_detail.append({
                    "raw": d_pay, "fecha": d_pay.strftime("%d/%m/%Y"),
                    "monto": iva_arena, "entidad": s.arenera.username, "tipo": "Arena",
                    "dia": ["Lun","Mar","Mié","Jue","Vie","Sáb","Dom"][d_pay.weekday()]
                })

    # --- D. PREPARAR JSON ---
    sorted_days = sorted(daily_stats.keys())
    chart_labels = [datetime.strptime(d, "%Y-%m-%d").strftime("%d/%m") for d in sorted_days]
    data_out = [daily_stats[d]["out"] for d in sorted_days]
    data_in  = [daily_stats[d]["in"]  for d in sorted_days]

    # Agrupar pagos
    pay_map = {}
    for p in payments_detail:
        k = f"{p['raw']}_{p['entidad']}_{p['tipo']}"
        if k not in pay_map:
            pay_map[k] = {**p, "count": 0, "monto": 0}
        pay_map[k]["monto"] += p["monto"]
        pay_map[k]["count"] += 1
    payments_final = sorted(pay_map.values(), key=lambda x: x["raw"])

    sorted_areneras = sorted(arenera_volumen.items(), key=lambda x: x[1], reverse=True)
    sorted_trans = sorted(top_trans_map.items(), key=lambda x: x[1], reverse=True)[:5]

    return {
        "kpi": {
            "viajes_total": total_viajes,
            
            # NUEVOS KPIs DE SEGUIMIENTO
            "viajes_ruta": total_moving,   # Suma de (1) y (2)
            "to_source": count_to_source,  # (1) Vacíos yendo a arenera
            "to_dest": count_to_dest,      # (2) Cargados yendo a SBE
            "tn_to_dest": tn_to_dest,      # Tn flotando en ruta
            "arrived": count_arrived,      # (3) Llegaron
            
            "tn_out": tn_despachadas_total,
            "tn_in": tn_arrived,
            "costo_total": total_costo_proyectado
        },
        "chart_data": { "labels": chart_labels, "out": data_out, "in": data_in },
        "payments_table": payments_final,
        "arenera_stats": [{"name": k, "value": v} for k, v in sorted_areneras],
        "top_trans": {
            "labels": [x[0] for x in sorted_trans],
            "values": [x[1] for x in sorted_trans]
        }
    }


@app.route("/admin/dashboard")
@login_required
@role_required("admin", "gestion")
def admin_dashboard():
    # Cargamos las areneras para el filtro desplegable
    areneras = User.query.filter_by(tipo="arenera", parent_id=None).order_by(User.username).all()
    return render_template(tpl("admin_dashboard"), areneras=areneras)

@app.route("/admin/certificacion")
@login_required
@role_required("admin", "gestion")
def admin_certificacion():
    view_mode  = request.args.get("view", "pendiente")
    tid_filter = request.args.get("transportista_id")
    aid_filter = request.args.get("arenera_id")
    
    # Filtros de Fecha (Opcionales para la vista Pendiente, pero útiles)
    start_str = request.args.get("start")
    end_str   = request.args.get("end")
    
    today = get_arg_today()
    
    # Lógica de fechas
    use_date_filter = False
    if start_str and end_str:
        try:
            start_date = date.fromisoformat(start_str)
            end_date   = date.fromisoformat(end_str)
            use_date_filter = True
        except ValueError:
            pass

    # Listas para selectores
    trans_list = User.query.filter_by(tipo="transportista").order_by(User.username).all()
    aren_list  = User.query.filter_by(tipo="arenera", parent_id=None).order_by(User.username).all()

    q = Shipment.query
    
    if view_mode == "historial":
        q = q.filter(Shipment.cert_status == "Certificado")
        # Historial filtra por Fecha de Certificación
        if use_date_filter:
            q = q.filter(Shipment.cert_fecha >= start_date, Shipment.cert_fecha <= end_date)
        q = q.order_by(Shipment.cert_fecha.desc(), Shipment.id.desc())
    else:
        # Pendientes filtra por Fecha de Viaje (Salida)
        q = q.filter(Shipment.cert_status != "Certificado")
        if use_date_filter:
            q = q.filter(Shipment.date >= start_date, Shipment.date <= end_date)
            
        q = q.order_by(case((Shipment.cert_status == "Observado", 1), else_=2), Shipment.date.desc())

    # Filtros de ID
    if tid_filter and tid_filter != "all":
        q = q.filter(Shipment.transportista_id == int(tid_filter))
    if aid_filter and aid_filter != "all":
        q = q.filter(Shipment.arenera_id == int(aid_filter))

    shipments = q.all()
    
    return render_template(
        tpl("admin_certification"), 
        shipments=shipments, 
        view_mode=view_mode,
        transportistas=trans_list, 
        areneras=aren_list,
        sel_tid=tid_filter,        
        sel_aid=aid_filter,
        # Pasamos las fechas para mantenerlas en el input (si existen)
        start_date=start_str, 
        end_date=end_str
    )

@app.post("/admin/certificar/<int:shipment_id>")
@login_required
@role_required("admin")
def certify_shipment(shipment_id):
    s = Shipment.query.get_or_404(shipment_id)
    
    # 1. Datos Físicos Finales
    s.final_remito = s.sbe_remito if s.sbe_remito else s.remito_arenera
    peso_final = s.sbe_peso_neto if (s.sbe_peso_neto and s.sbe_peso_neto > 0) else s.peso_neto_arenera
    s.final_peso = peso_final

    # 2. Precios al momento
    price_flete = s.transportista.custom_price or 0
    price_arena = s.arenera.custom_price or 0
    
    # 3. CÁLCULO FINANCIERO (NUEVA FÓRMULA)
    conf = get_config()
    tol_tn = conf.tolerance_kg / 1000.0
    
    peso_salida = s.peso_neto_arenera or 0
    merma_money = 0.0
    tn_base_flete = 0.0

    if s.arenera.cert_type == 'salida':
        # Si es por salida, se paga todo lo cargado y no hay multa
        tn_base_flete = peso_salida
        merma_money = 0.0
    else:
        # Si es por llegada (Estándar)
        tn_base_flete = peso_final # Se paga sobre lo recibido
        
        # Calculamos multa aparte
        diff = peso_salida - peso_final
        if diff > tol_tn:
            excess = diff - tol_tn
            merma_money = excess * price_arena # Multa = Exceso * Precio Arena

    # Neto = (Tn * Precio Flete) - Multa
    flete_neto = (tn_base_flete * price_flete) - merma_money
    flete_iva  = flete_neto * 1.21
    if flete_iva < 0: flete_iva = 0 

    # 4. Guardar Snapshot
    s.frozen_flete_price = price_flete
    s.frozen_arena_price = price_arena
    s.frozen_merma_money = merma_money
    s.frozen_flete_neto  = flete_neto
    s.frozen_flete_iva   = flete_iva

    # 5. Estado
    s.cert_status = "Certificado"
    s.cert_fecha  = get_arg_today()
    if s.status != "Llego": 
        s.status = "Llego"

    db.session.commit()
    
    flash(f"Viaje #{s.id} certificado. Neto: ${flete_neto:,.0f} (Multa: ${merma_money:,.0f})", "success")
    return redirect(url_for("admin_certificacion"))

@app.post("/admin/corregir_sbe/<int:shipment_id>")
@login_required
@role_required("admin")
def corregir_sbe(shipment_id):
    s = Shipment.query.get_or_404(shipment_id)
    
    # 1. RESET
    if request.form.get("action") == "reset":
        s.sbe_remito = None
        s.sbe_peso_neto = None
        s.sbe_fecha_salida = None
        s.sbe_fecha_llegada = None
        s.sbe_patente = None
        s.sbe_manual_override = False
        s.cert_status = "Pendiente"
        s.observation_reason = None
        db.session.commit()
        flash("Corrección manual eliminada. Listo para re-sincronizar.", "info")
        return redirect(url_for("admin_certificacion"))

    # 2. GUARDADO MANUAL
    nuevo_remito = request.form.get("sbe_remito", "").strip()
    nuevo_peso   = request.form.get("sbe_peso", "").strip()
    
    if nuevo_remito: 
        s.sbe_remito = nuevo_remito
        
    if nuevo_peso:   
        try:
            s.sbe_peso_neto = float(nuevo_peso.replace(",", "."))
        except ValueError:
            flash("Error: Peso inválido.", "error")
            return redirect(url_for("admin_certificacion"))
    
    # Activar bloqueo manual
    s.sbe_manual_override = True 
    
    # --- CORRECCIÓN DE FECHAS (Tu pedido 5 y 6) ---
    # Si es manual y no tiene fechas de SBE, usamos las fechas del viaje original
    # para que no queden vacías y el filtro de fechas funcione.
    if not s.sbe_fecha_salida:
        s.sbe_fecha_salida = datetime.combine(s.date, datetime.min.time()) # Convertir date a datetime
    
    if not s.sbe_fecha_llegada:
        # Si no hay llegada, asumimos el mismo día de salida por defecto
        s.sbe_fecha_llegada = s.sbe_fecha_salida

    # Recalcular estado
    if s.sbe_remito and s.sbe_peso_neto is not None:
         s.cert_status = "Pre-Aprobado"
         s.observation_reason = "Corregido Manualmente"
         
    db.session.commit()
    flash("Datos corregidos. Fechas ajustadas automáticamente.", "success")
    return redirect(url_for("admin_certificacion"))

# ----------------------------
# PANEL TRANSPORTISTA
# ----------------------------
@app.route("/transportista/panel")
@login_required
@role_required("transportista")
def transportista_panel():
    u   = User.query.get(session["user_id"])
    hoy = get_arg_today()

    week_start = hoy - timedelta(days=hoy.weekday())
    next_monday = week_start + timedelta(days=7)

    en_viaje_total = (Shipment.query
        .filter_by(transportista_id=u.id, status="En viaje")
        .count())

    llegados_semana = (Shipment.query
        .filter(
            Shipment.transportista_id == u.id,
            Shipment.status == "Llego",
            Shipment.date >= week_start,
            Shipment.date <  next_monday
        ).count())
    
    # 1. CONSULTA AÑADIDA: Obtener la lista de Choferes del transportista
    choferes_list = Chofer.query.filter_by(transportista_id=u.id).all() 

    stats = {
        "en_viaje":   en_viaje_total,
        "llegados":   llegados_semana,
        "week_from":  week_start,
        "week_to":    next_monday - timedelta(days=1),
    }

    envios = (Shipment.query
              .filter_by(transportista_id=u.id)
              .order_by(Shipment.date.desc(), Shipment.id.desc())
              .all())

    assignments = []
    for q in Quota.query.filter_by(transportista_id=u.id, date=hoy).all():
        if q.arenera:
            assignments.append({
                "arenera": q.arenera,
                "daily_limit": q.limit,
                "daily_used": q.used
            })

    return render_template(
        tpl("transportista_panel"),
        user=u, today=hoy,
        stats=stats,
        assignments=assignments,
        envios=envios,
        choferes=choferes_list, 
    )

# --- Agregar en app.py ---

@app.route("/transportista/history")
@login_required
@role_required("transportista")
def transportista_history():
    u = db.session.get(User, session["user_id"])

    start_str = (request.args.get("start_date") or "").strip()
    end_str   = (request.args.get("end_date") or "").strip()
    status    = (request.args.get("status") or "").strip()
    search    = (request.args.get("search") or "").strip().lower()

    # Rango de fechas inteligente
    min_date = db.session.query(func.min(Shipment.date)).filter_by(transportista_id=u.id).scalar() or get_arg_today()
    max_date = db.session.query(func.max(Shipment.date)).filter_by(transportista_id=u.id).scalar() or get_arg_today()

    try:
        start_date = date.fromisoformat(start_str) if start_str else min_date
    except ValueError:
        start_date = min_date
    try:
        end_date = date.fromisoformat(end_str) if end_str else max_date
    except ValueError:
        end_date = max_date
    if end_date < start_date:
        start_date, end_date = end_date, start_date

    # Query base
    base_rows = (Shipment.query
                 .filter(
                     Shipment.transportista_id == u.id,
                     Shipment.date >= start_date,
                     Shipment.date <= end_date,
                 )
                 .order_by(Shipment.date.desc(), Shipment.id.desc())
                 .all())

    # Búsqueda
    if search:
        search_id = -1
        clean_search = search.replace("#", "")
        if clean_search.isdigit():
            search_id = int(clean_search)
        
        needle = search
        def hit(s: Shipment) -> bool:
            if s.id == search_id: return True
            vals = [
                s.chofer, s.dni, s.tractor, s.trailer, s.tipo,
                getattr(s.arenera, "username", ""), # Buscar por nombre de Arenera
                s.remito_arenera, 
                s.final_remito
            ]
            return any(needle in (v or "").lower() for v in vals)
        base_rows = [s for s in base_rows if hit(s)]

    counts = {
        "total":      len(base_rows),
        "en_viaje":   sum(1 for s in base_rows if s.status == "En viaje"),
        "llegados":   sum(1 for s in base_rows if s.status == "Llego"),
    }

    if status:
        rows = [s for s in base_rows if s.status == status]
    else:
        rows = base_rows

    return render_template(
        tpl("transportista_history"),
        user=u,
        shipments=rows,
        counts=counts,
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
        status=status,
        search=(request.args.get("search") or "").strip(),
    )

@app.get("/transportista/export")
@login_required
@role_required("transportista")
def transportista_export():
    u = db.session.get(User, session["user_id"])

    # Fechas (con hora Argentina)
    today = get_arg_today()
    start_str = request.args.get("start")
    end_str   = request.args.get("end")

    try:
        if start_str:
            start_date = date.fromisoformat(start_str)
        else:
            start_date = today.replace(day=1)
            
        if end_str:
            end_date = date.fromisoformat(end_str)
        else:
            end_date = today
    except ValueError:
        start_date = today.replace(day=1)
        end_date   = today

    # Query: Viajes del transportista (Incluye Join con Operador para saber quién cargó)
    rows = (db.session.query(Shipment)
            .outerjoin(User, Shipment.operador_id == User.id)
            .filter(
                Shipment.transportista_id == u.id,
                Shipment.date >= start_date,
                Shipment.date <= end_date
            )
            .order_by(Shipment.date.desc(), Shipment.id.desc())
            .all())

    # --- Generar Excel ---
    wb = Workbook()
    ws = wb.active
    ws.title = "Reporte Detallado"

    # Encabezados Enriquecidos
    headers = [
        "ID", "Fecha Salida", "Fecha Llegada", "Arenera", 
        "Chofer", "DNI", "Patente Tractor", "Patente Batea", "Tipo",
        "Remito Origen", "Tn Origen", 
        "Remito Destino", "Tn Destino (Pagable)", 
        "Merma Descontada (Tn)", # Nueva
        "Precio Flete ($)",      # Nueva
        "Total Neto ($)",        # Nueva
        "Estado Viaje", "Estado Certificación"
    ]
    ws.append(headers)

    # Config global para tolerancias (fallback)
    conf = get_config()
    tol_tn = conf.tolerance_kg / 1000.0

    for s in rows:
        # 1. Determinar valores (Snapshot vs En Vivo)
        is_cert = (s.cert_status == "Certificado")
        
        if s.frozen_flete_neto is not None:
            # --- DATOS CONGELADOS (HISTÓRICOS) ---
            precio_unit = s.frozen_flete_price or 0
            neto_viaje  = s.frozen_flete_neto or 0
            merma_plata = s.frozen_merma_money or 0
            
            # Reconstruimos la Tn de Merma para mostrarla
            tn_merma = 0.0
            if merma_plata > 0 and s.frozen_arena_price:
                tn_merma = merma_plata / s.frozen_arena_price

            # Reconstruimos el peso pagable
            peso_pagable = (neto_viaje + merma_plata) / (precio_unit if precio_unit else 1)
            
            remito_final = s.final_remito
            
        else:
            # --- CÁLCULO EN VIVO (NO CERTIFICADO AÚN) ---
            # Usamos los valores actuales de referencia
            precio_unit = u.custom_price or 0
            
            loaded  = s.peso_neto_arenera or 0
            arrived = s.final_peso or s.sbe_peso_neto or 0
            
            tn_merma = 0.0
            merma_plata = 0.0
            
            # Lógica de pago según Arenera
            if s.arenera.cert_type == 'salida':
                peso_pagable = loaded
            else:
                # Por llegada (con descuento merma)
                diff = loaded - arrived
                if diff > tol_tn:
                    tn_merma = diff - tol_tn
                    # Estimamos la multa con precio actual
                    merma_plata = tn_merma * (s.arenera.custom_price or 0)
                
                peso_pagable = arrived - tn_merma
            
            neto_viaje = (peso_pagable * precio_unit) - merma_plata
            remito_final = s.final_remito if is_cert else (s.sbe_remito or "")

        # 2. Fechas
        f_salida = s.date.strftime("%d/%m/%Y")
        f_llegada = s.sbe_fecha_llegada.strftime("%d/%m/%Y") if s.sbe_fecha_llegada else ""
        
        # 3. Escribir fila
        ws.append([
            s.id,
            f_salida,
            f_llegada,
            s.arenera.username if s.arenera else "",
            
            s.chofer,
            s.dni,
            s.tractor,
            s.trailer,
            s.tipo,
            
            s.remito_arenera or "",        
            s.peso_neto_arenera or 0,     
            
            remito_final or "",                  
            peso_pagable or 0,
            
            tn_merma if tn_merma > 0 else 0, # Merma
            precio_unit,                     # Precio
            neto_viaje,                      # Total $
            
            s.status,                      
            s.cert_status or "Pendiente"
        ])

    # Ajustar anchos de columna automáticamente
    for col in ws.columns:
        max_length = 0
        column = col[0].column_letter
        for cell in col:
            try:
                if len(str(cell.value)) > max_length:
                    max_length = len(str(cell.value))
            except: pass
        ws.column_dimensions[column].width = (max_length + 2)

    bio = io.BytesIO()
    wb.save(bio)
    bio.seek(0)

    resp = make_response(bio.getvalue())
    resp.headers["Content-Type"] = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    fname = f"Reporte_{u.username}_{start_date.strftime('%d%m')}-{end_date.strftime('%d%m')}.xlsx"
    resp.headers["Content-Disposition"] = f'attachment; filename="{fname}"'
    return resp

@app.route("/transportista/quotas")
@login_required
@role_required("transportista")
def transportista_quotas():
    u = db.session.get(User, session["user_id"])
    today      = get_arg_today()
    week_dates = [today + timedelta(days=i) for i in range(7)]
    quotas     = (
        Quota.query.filter_by(transportista_id=u.id)
        .filter(Quota.date.in_(week_dates))
        .order_by(Quota.date)
        .all()
    )

    by_date = {}
    for q in quotas:
        if q.arenera:
            by_date.setdefault(q.date, []).append(q)

    # Totales disponibles por fecha (debe ir ANTES del return)
    totals_by_date = {}
    for d in week_dates:
        lst = by_date.get(d, [])
        totals_by_date[d] = sum(max((q.limit or 0) - (q.used or 0), 0) for q in lst)

    return render_template(
        tpl("transportista_quotas"),
        user=u,
        week_dates=week_dates,
        by_date=by_date,
        totals_by_date=totals_by_date
    )

@app.route("/transportista/arenera/<int:arenera_id>", methods=["GET", "POST"])
@login_required
@role_required("transportista")
def transportista_arenera(arenera_id):
    u = db.session.get(User, session["user_id"])
    
    # Fecha
    date_str = request.args.get("date", get_arg_today().isoformat())
    try: dt = date.fromisoformat(date_str)
    except ValueError: dt = get_arg_today()

    # Cupo
    q = Quota.query.filter_by(transportista_id=u.id, arenera_id=arenera_id, date=dt).first_or_404()

    if request.method == "POST":
        if "delete_id" in request.form:
            # ... (Lógica de borrado igual que antes) ...
            sid = request.form.get("delete_id")
            s = Shipment.query.get(sid)
            if s and s.transportista_id==u.id and s.status=="En viaje" and s.date==dt:
                db.session.delete(s)
                q.used = max(0, (q.used or 0) - 1)
                db.session.commit()
                flash("Viaje eliminado.", "success")
        else:
            # CREAR VIAJE
            if (q.used or 0) < (q.limit or 0):
                # Datos básicos del Formulario
                chofer  = request.form.get("nombre_apellido", "").strip()
                dni     = request.form.get("dni", "").strip()
                gender  = request.form.get("gender", "M")
                tipo    = request.form.get("tipo", "")
                tractor = request.form.get("patente_tractor", "").strip().upper()
                trailer = request.form.get("patente_batea", "").strip().upper()
                
                if chofer and dni and tractor:
                    new_ship = Shipment(
                        transportista_id=u.id,
                        arenera_id=arenera_id,
                        operador_id=u.id,
                        date=dt,
                        chofer=chofer,
                        dni=dni,
                        gender=gender,
                        tipo=tipo,
                        tractor=tractor,
                        trailer=trailer,
                        
                        # ESTADO INICIAL CRÍTICO:
                        status="En viaje",
                        peso_neto_arenera=None, # Sin peso aún -> "Yendo a Arenera"
                        cert_status="Pendiente"
                    )
                    db.session.add(new_ship)
                    q.used = (q.used or 0) + 1
                    db.session.commit()
                    flash("Viaje iniciado. Diríjase a la Arenera.", "success")
                else:
                    flash("Faltan datos.", "error")
            else:
                flash("Sin cupo disponible.", "error")
        
        return redirect(url_for("transportista_arenera", arenera_id=arenera_id, date=dt.isoformat()))

    # Listado
    shipments = Shipment.query.filter_by(transportista_id=u.id, arenera_id=arenera_id, date=dt).order_by(Shipment.id.desc()).all()

    return render_template(
        tpl("transportista_arenera"),
        assignment=q, shipments=shipments, used=(q.used or 0), limit=(q.limit or 0), date=dt
    )

@app.route("/transportista/choferes", methods=["GET", "POST"])
@login_required
@role_required("transportista")
def transportista_choferes():
    u_id = session["user_id"]
    
    if request.method == "POST":
        action = request.form.get("action")
        
        # --- A. CREAR / ACTUALIZAR CHOFER ---
        if action == "save":
            dni = (request.form.get("dni") or "").strip()
            nombre = (request.form.get("nombre") or "").strip()
            tractor = (request.form.get("tractor") or "").strip().upper().replace(" ", "")
            trailer = (request.form.get("trailer") or "").strip().upper().replace(" ", "")
            gender = (request.form.get("gender") or "M").strip()
            tipo = (request.form.get("tipo") or "").strip() # Asumiendo que agregaste 'tipo' al modelo Chofer
            chofer_id = request.form.get("chofer_id", type=int)

            if not (dni and nombre):
                flash("El DNI y el Nombre son obligatorios.", "error")
                return redirect(url_for("transportista_choferes"))

            if chofer_id:
                # Editar Chofer existente
                chofer = Chofer.query.filter_by(id=chofer_id, transportista_id=u_id).first()
                if not chofer:
                    flash("Chofer no encontrado.", "error")
                    return redirect(url_for("transportista_choferes"))
            else:
                # Crear nuevo Chofer - Validar unicidad global
                exists = Chofer.query.filter_by(dni=dni).first()
                if exists:
                    flash(f"Ya existe un chofer con DNI {dni} registrado.", "error")
                    return redirect(url_for("transportista_choferes"))
                
                chofer = Chofer(transportista_id=u_id, dni=dni)
            
            chofer.nombre = nombre
            chofer.tractor = tractor
            chofer.trailer = trailer
            chofer.gender = gender
            chofer.tipo = tipo

            db.session.add(chofer)
            db.session.commit()
            flash(f"Chofer {nombre} guardado exitosamente.", "success")
        
        # --- B. ELIMINAR CHOFER ---
        elif action == "delete":
            chofer_id = request.form.get("chofer_id", type=int)
            chofer = Chofer.query.filter_by(id=chofer_id, transportista_id=u_id).first_or_404()
            
            db.session.delete(chofer)
            db.session.commit()
            flash("Chofer eliminado.", "success")

        return redirect(url_for("transportista_choferes"))
    
    # GET: Listar choferes (Serializando para Jinja y JS)
    choferes_raw = Chofer.query.filter_by(transportista_id=u_id).order_by(Chofer.nombre.asc()).all()
    
    choferes = []
    for c in choferes_raw:
        choferes.append({
            'id': c.id,
            'nombre': c.nombre,
            'dni': c.dni,
            'gender': c.gender,
            'tractor': c.tractor,
            'trailer': c.trailer,
            'tipo': c.tipo,
        })

    return render_template(tpl("transportista_choferes"), choferes=choferes)

# --- Agregar en app.py, junto a las rutas del transportista ---

@app.route("/api/choferes/mine")
@login_required
@role_required("transportista")
def api_choferes_mine():
    u_id = session["user_id"]
    
    # Optimizamos, solo necesitamos los campos para el autocomplete
    choferes = db.session.query(Chofer).filter_by(transportista_id=u_id).all()
    
    data = []
    for c in choferes:
        data.append({
            'dni': c.dni,
            'nombre': c.nombre,
            'gender': c.gender,
            'tractor': c.tractor,
            'trailer': c.trailer,
            'tipo': c.tipo,
        })
    
    # Flask ya sabe cómo devolver diccionarios como JSON
    return data

@app.post("/transportista/shipment/<int:ship_id>/delete")
@login_required
@role_required("transportista")
def delete_own_shipment(ship_id):
    s = Shipment.query.get_or_404(ship_id)
    if s.transportista_id != session.get("user_id"):
        abort(403)
    if s.status != "En viaje":
        flash("Sólo se puede eliminar cuando el viaje está 'En viaje'.", "error")
        return redirect(url_for("transportista_panel"))
    q = Quota.query.filter_by(
        transportista_id=s.transportista_id,
        arenera_id=s.arenera_id,
        date=s.date
    ).first()
    if q and q.used > 0:
        q.used -= 1
    db.session.delete(s)
    db.session.commit()
    flash("Viaje eliminado.", "success")
    return redirect(url_for("transportista_panel"))

# ----------------------------
# PANEL ARENERA
# ----------------------------

@app.route("/arenera")
@login_required
@role_required("arenera")
def arenera_panel():
    u = db.session.get(User, session["user_id"])
    
    # 1. Obtener Familia
    fam = get_family_ids(session["user_id"])
    if not fam: 
        flash("Error de permisos.", "error")
        return redirect(url_for("login"))
    
    # Filtros desde URL
    sh = request.args.get("search", "").strip().lower()
    trans_filter = request.args.get("trans_filter")
    
    # 2. Query Base: SOLO MOSTRAR "En viaje" (Los que están yendo a buscar carga)
    # Una vez cargados, pasan a "Salido a SBE" y salen de esta lista.
    q = Shipment.query.filter(
        Shipment.arenera_id.in_(fam),
        Shipment.status == "En viaje" 
    )

    # Buscador de Texto
    if sh: 
        q = q.filter(
            func.lower(Shipment.chofer).like(f"%{sh}%") | 
            Shipment.dni.like(f"%{sh}%") | 
            func.lower(Shipment.tractor).like(f"%{sh}%")
        )
    
    # Filtro de Transportista
    if trans_filter and trans_filter != "all":
        try:
            q = q.filter(Shipment.transportista_id == int(trans_filter))
        except ValueError: pass

    # Orden: El más viejo arriba (FIFO)
    ships = q.order_by(Shipment.date.asc(), Shipment.id.asc()).all()
    
    # Lista para selector de transportistas (solo los que tienen camiones viniendo)
    active_trans_ids = db.session.query(Shipment.transportista_id).filter(
        Shipment.arenera_id.in_(fam),
        Shipment.status == "En viaje"
    ).distinct().all()
    
    active_transports = []
    if active_trans_ids:
        t_ids = [r[0] for r in active_trans_ids]
        active_transports = User.query.filter(User.id.in_(t_ids)).order_by(User.username).all()

    # Estadísticas Rápidas del día
    today = get_arg_today()
    stats = {
        "en_viaje": Shipment.query.filter(Shipment.arenera_id.in_(fam), Shipment.status=="En viaje", Shipment.date==today).count(),
        "salido": Shipment.query.filter(Shipment.arenera_id.in_(fam), Shipment.status=="Salido a SBE", Shipment.date==today).count(),
        "search": sh,
        "trans_filter": trans_filter
    }
    
    return render_template(
        tpl("arenera_panel"), 
        user=u, 
        shipments=ships, 
        stats=stats,
        active_transports=active_transports
    )

@app.route("/arenera/history")
@login_required
@role_required("arenera")
def arenera_history():
    u = db.session.get(User, session["user_id"])
    fam = get_family_ids(session["user_id"])

    # --- 1. Rango Inteligente de Fechas ---
    # Busca la fecha min/max real de la DB para no ocultar viajes viejos por defecto
    min_db_date = db.session.query(func.min(Shipment.date)).filter(Shipment.arenera_id.in_(fam)).scalar() or get_arg_today()
    max_db_date = db.session.query(func.max(Shipment.date)).filter(Shipment.arenera_id.in_(fam)).scalar() or get_arg_today()

    start_str = request.args.get("start_date", "")
    end_str   = request.args.get("end_date", "")
    
    # Si no elige fecha, mostramos TODO el historial disponible
    start_date = date.fromisoformat(start_str) if start_str else min_db_date
    end_date   = date.fromisoformat(end_str)   if end_str   else max_db_date

    if end_date < start_date:
        start_date, end_date = end_date, start_date

    # --- 2. QUERY BASE ---
    base_q = Shipment.query.filter(
        Shipment.arenera_id.in_(fam),
        Shipment.date >= start_date,
        Shipment.date <= end_date
    )

    # --- 3. CONTEOS (Chips) ---
    all_in_range = base_q.order_by(Shipment.date.desc(), Shipment.id.desc()).all()
    
    counts = {
        "total": len(all_in_range),
        "en_viaje": 0,
        "salido": 0,
        "llegado": 0,
        "certificado": 0
    }

    # Calculamos los totales excluyentes
    for s in all_in_range:
        if s.cert_status == 'Certificado':
            counts["certificado"] += 1
        elif s.status in ['Llego', 'Llegado a SBE'] or s.sbe_fecha_llegada:
            counts["llegado"] += 1
        elif s.status == 'Salido a SBE' or (s.remito_arenera and not s.sbe_fecha_llegada):
            counts["salido"] += 1
        else:
            counts["en_viaje"] += 1

    # --- 4. APLICAR FILTROS ---
    final_q = base_q
    status = request.args.get("status", "")
    
    if status:
        if status == "Llegado a SBE":
            # CORRECCIÓN AQUÍ: Que haya llegado PERO NO esté certificado
            final_q = final_q.filter(
                (
                    (Shipment.status == "Llego") | 
                    (Shipment.status == "Llegado a SBE") |
                    (Shipment.sbe_fecha_llegada != None)
                ),
                (Shipment.cert_status != "Certificado") # <--- EXCLUSIÓN AGREGADA
            )
        elif status == "Salido a SBE":
            final_q = final_q.filter(
                (Shipment.status == "Salido a SBE") |
                (
                    (Shipment.remito_arenera != None) & 
                    (Shipment.remito_arenera != "") & 
                    (Shipment.sbe_fecha_llegada == None) & 
                    (Shipment.status != "En viaje") &
                    (Shipment.cert_status != "Certificado")
                )
            )
        elif status == "En viaje":
             final_q = final_q.filter(Shipment.status == "En viaje")
        elif status == "Certificado":
             final_q = final_q.filter(Shipment.cert_status == "Certificado")
        else:
            final_q = final_q.filter(Shipment.status == status)

    # --- 5. BUSCADOR ---
    search = request.args.get("search", "").strip().lower()
    if search:
        if search.isdigit():
             final_q = final_q.filter(
                 (Shipment.id == int(search)) |
                 (Shipment.dni.like(f"%{search}%")) |
                 (Shipment.remito_arenera.like(f"%{search}%"))
             )
        else:
            final_q = final_q.filter(
                func.lower(Shipment.chofer).like(f"%{search}%") | 
                func.lower(Shipment.tractor).like(f"%{search}%") |
                func.lower(Shipment.remito_arenera).like(f"%{search}%")
            )

    rows = final_q.order_by(Shipment.date.desc(), Shipment.id.desc()).all()

    return render_template(
        tpl("arenera_history"),
        user=u,
        shipments=rows,
        counts=counts,
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
        status=status,
        search=search
    )

@app.post("/arenera/update/<int:shipment_id>")
@login_required
@role_required('arenera')
def arenera_update(shipment_id):
    s = Shipment.query.get_or_404(shipment_id)
    fam = get_family_ids(session["user_id"])
    
    if s.arenera_id not in fam: abort(403)
    
    # Seguridad absoluta: Si ya llegó a SBE o está certificado, NO SE TOCA.
    if s.cert_status == "Certificado" or s.status in ["Llego", "Llegado a SBE"]:
        flash("El viaje ya ha llegado a destino o está certificado. No se puede editar.", "error")
        return redirect(url_for("arenera_history"))
    
    action = request.form.get('action')

    # --- ACCIÓN 1: CONFIRMAR SALIDA (Desde Panel) ---
    if action == "confirmar_salida":
        rem = request.form.get('remito_arenera', "").strip()
        peso = request.form.get('peso_neto_arenera', "").strip()

        if not rem or not peso: 
            flash("Faltan datos (Remito o Peso).", "error")
            return redirect(url_for("arenera_panel"))
        
        # Validar Duplicados
        dup = Shipment.query.filter(
            Shipment.arenera_id.in_(fam), 
            Shipment.id != s.id, 
            Shipment.remito_arenera == rem, 
            Shipment.cert_status != 'Certificado'
        ).first()
        
        if dup:
            flash(f"⚠️ El remito {rem} ya existe en el viaje #{dup.id}.", "error")
            return redirect(url_for("arenera_panel"))

        try: 
            s.peso_neto_arenera = float(peso.replace(",", "."))
        except ValueError: 
            flash("Peso inválido.", "error")
            return redirect(url_for("arenera_panel"))
            
        s.remito_arenera = rem
        # CAMBIO DE ESTADO: Pasa a historial como "Salido a SBE"
        s.status = "Salido a SBE"
        
        # Limpieza por si acaso
        s.sbe_remito = None
        s.sbe_peso_neto = None
        s.cert_status = "Pendiente"
        
        s.operador_id = session["user_id"]
        db.session.commit()
        flash("✅ Salida confirmada. El viaje pasó al Historial.", "success")
        return redirect(url_for("arenera_panel"))

    # --- ACCIÓN 2: REVERTIR / CORREGIR (Desde Historial) ---
    elif action == "revertir":
        # Solo permitimos revertir si está en "Salido a SBE"
        if s.status == "Salido a SBE":
            s.status = "En viaje"
            s.cert_status = "Pendiente"
            flash("Corrección habilitada: El viaje ha vuelto a Recepción.", "info")
            db.session.commit()
            return redirect(url_for("arenera_panel")) # Lo mandamos al panel para que edite
        else:
            flash("No se puede revertir este viaje.", "error")
            return redirect(url_for("arenera_history"))

    return redirect(url_for("arenera_panel"))

@app.get("/arenera/export")
@login_required
@role_required("arenera")
def arenera_export():
    u = db.session.get(User, session["user_id"])
    
    # 1. Obtener Familia
    family_ids = get_family_ids(session["user_id"])
    if not family_ids:
        flash("Error identificando cuenta.", "error")
        return redirect(url_for("arenera_panel"))

    # 2. Fechas
    today = get_arg_today()
    start_str = request.args.get("start")
    end_str   = request.args.get("end")

    try:
        if start_str:
            start_date = date.fromisoformat(start_str)
        else:
            start_date = today.replace(day=1)
        if end_str:
            end_date = date.fromisoformat(end_str)
        else:
            end_date = today
    except ValueError:
        start_date, end_date = today, today

    # 3. Query
    rows = (Shipment.query
            .filter(
                Shipment.arenera_id.in_(family_ids),
                Shipment.date >= start_date,
                Shipment.date <= end_date,
            )
            .order_by(Shipment.date.desc(), Shipment.id.desc())
            .all())

    # --- Generar Excel ---
    wb = Workbook()
    ws = wb.active
    ws.title = "Reporte de Ventas"

    # Encabezados Solicitados
    headers = [
        "ID", 
        "Fecha Salida", 
        "Remito", 
        "Transportista", 
        "Chofer", 
        "DNI", 
        "CUIL",             # Nueva
        "Tn Despachadas", 
        "Precio Unitario ($)", 
        "Subtotal Ventas ($)",
        "Estado", 
        "Fecha Certificación"
    ]
    ws.append(headers)

    for s in rows:
        # Cálculo de Valores
        peso_pagable = s.peso_neto_arenera or 0
        
        # Precio (Histórico vs Actual)
        if s.frozen_arena_price is not None:
            precio_unit = s.frozen_arena_price
        else:
            precio_unit = s.arenera.custom_price or 0
            
        total_venta = peso_pagable * precio_unit

        # Fechas
        f_salida = s.date.strftime("%d/%m/%Y")
        f_certif = s.cert_fecha.strftime("%d/%m/%Y") if s.cert_fecha else "-"
        
        # Cálculo de CUIL (Usando tu función helper existente)
        cuil_str = calcular_cuil(s.dni, s.gender)

        ws.append([
            s.id,
            f_salida,
            s.remito_arenera or "",
            s.transportista.username if s.transportista else "",
            s.chofer,
            s.dni,
            cuil_str,       # Columna CUIL
            peso_pagable,   # Tn
            precio_unit,    # $ Unit
            total_venta,    # $ Total
            s.cert_status if s.cert_status != "Pendiente" else s.status,
            f_certif
        ])

    # Ajuste de ancho
    for col in ws.columns:
        max_length = 0
        column = col[0].column_letter
        for cell in col:
            try:
                if len(str(cell.value)) > max_length:
                    max_length = len(str(cell.value))
            except: pass
        ws.column_dimensions[column].width = (max_length + 2)

    bio = io.BytesIO()
    wb.save(bio)
    bio.seek(0)

    resp = make_response(bio.getvalue())
    resp.headers["Content-Type"] = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    fname = f"Ventas_{u.username}_{start_date.strftime('%d%m')}-{end_date.strftime('%d%m')}.xlsx"
    resp.headers["Content-Disposition"] = f'attachment; filename="{fname}"'
    return resp

@app.get("/admin/export")
@login_required
@role_required("admin")
def admin_export():
    # Filtros opcionales ?start=YYYY-MM-DD&end=YYYY-MM-DD&status=...
    start_str = (request.args.get("start") or "").strip()
    end_str   = (request.args.get("end") or "").strip()
    status    = (request.args.get("status") or "").strip()

    q = Shipment.query
    if start_str:
        try:
            q = q.filter(Shipment.date >= date.fromisoformat(start_str))
        except ValueError:
            pass
    if end_str:
        try:
            q = q.filter(Shipment.date <= date.fromisoformat(end_str))
        except ValueError:
            pass
    if status:
        q = q.filter(Shipment.status == status)

    rows = (q
        .join(User, Shipment.transportista_id==User.id)
        .add_columns(User.username.label("transportista"))
        .all())

    # Construir XLSX
    wb = Workbook()
    ws = wb.active
    ws.title = "Historial"
    headers = ["Fecha","Transportista","Arenera","Chofer","DNI","Tractor","Batea","Tipo","Estado"]
    ws.append(headers)

    for s, transportista in rows:
        arenera_name = s.arenera.username if s.arenera else ""
        ws.append([
            s.date.strftime("%Y-%m-%d"),
            transportista,
            arenera_name,
            s.chofer,
            s.dni,
            s.tractor,
            s.trailer,
            s.tipo,
            "Llegó" if s.status == "Llego" else s.status
        ])

    bio = io.BytesIO()
    wb.save(bio)
    bio.seek(0)
    resp = make_response(bio.getvalue())
    resp.headers["Content-Type"] = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    fname = f"historial_{get_arg_now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    resp.headers["Content-Disposition"] = f'attachment; filename="{fname}"'
    return resp

@app.route("/admin/config", methods=["GET", "POST"])
@login_required
@role_required("admin")
def admin_config():
    conf = get_config()
    
    if request.method == "POST":
        # Globales
        conf.tolerance_kg   = float(request.form.get("tolerance_kg", 0))
        conf.dispatch_price = float(request.form.get("dispatch_price", 0))
        
        # Iterar campos dinámicos (Emails y Precios)
        # Formato esperado: email_12, price_12
        for key, val in request.form.items():
            if "_" not in key: continue
            prefix, uid_str = key.split("_", 1)           
            if prefix in ["email", "price", "certtype", "paydays"]: 
                            try:
                                uid = int(uid_str)
                                u = db.session.get(User, uid)
                                if u:
                                    if prefix == "email": u.email = val.strip()
                                    elif prefix == "price": u.custom_price = float(val) if val else 0.0
                                    elif prefix == "certtype": u.cert_type = val
                                    elif prefix == "paydays": u.payment_days = int(val) if val else 30 # <-- Nuevo
                            except ValueError:
                                pass
                    
        db.session.commit()
        flash("Configuracion actualizada.", "success")
        return redirect(url_for("admin_config"))
  
    transportistas = User.query.filter_by(tipo="transportista").order_by(User.username).all()
    areneras       = User.query.filter_by(tipo="arenera").order_by(User.username).all()
    
    return render_template(tpl("admin_config"), 
                           conf=conf, 
                           transportistas=transportistas, 
                           areneras=areneras)

# --- Modificar en app.py ---

@app.route("/admin/resumen")
@login_required
@role_required("admin", "gestion")
def admin_resumen():
    conf = get_config()
    tol_tn = conf.tolerance_kg / 1000.0 
    
    start_str = request.args.get("start")
    end_str   = request.args.get("end")
    
    # Fecha Argentina
    today = get_arg_today()
    
    try:
        start_date = date.fromisoformat(start_str) if start_str else today.replace(day=1)
        end_date   = date.fromisoformat(end_str)   if end_str   else today
    except ValueError:
        start_date, end_date = today, today

    # 1. Preparación de Estructuras (Columnas Fijas)
    all_trans_users = User.query.filter_by(tipo="transportista").order_by(User.username).all()
    all_trans_ids = set()
    trans_names   = {}
    
    for t in all_trans_users:
        all_trans_ids.add(t.id)
        trans_names[t.id] = t.username

    # Query: Solo Certificados en el rango
    rows = (Shipment.query
            .filter(Shipment.cert_status == "Certificado")
            .filter(Shipment.cert_fecha >= start_date, Shipment.cert_fecha <= end_date)
            .order_by(Shipment.cert_fecha.asc())
            .all())
    
    # Definimos las 6 matrices
    mat_neto_cert = {}  
    mat_iva_pay   = {}  
    mat_tn_out    = {}  
    mat_tn_in     = {}  
    mat_tn_diff   = {}  
    mat_trucks    = {}  

    for s in rows:
        tid = s.transportista_id
        
        if tid not in all_trans_ids:
            all_trans_ids.add(tid)
            trans_names[tid] = s.transportista.username
        
        loaded  = s.peso_neto_arenera or 0
        arrived = s.final_peso or 0
        
        # --- LÓGICA HÍBRIDA: CONGELADO vs LEGADO ---
        if s.frozen_flete_neto is not None:
            neto = s.frozen_flete_neto
            iva  = s.frozen_flete_iva
            
            diff_tn_visual = 0
            if s.frozen_merma_money > 0 and (s.frozen_arena_price or 0) > 0:
                diff_tn_visual = s.frozen_merma_money / s.frozen_arena_price
            elif s.frozen_merma_money == 0:
                 diff_tn_visual = 0 
        else:
            # Fallback cálculo al vuelo
            price_flete = s.transportista.custom_price or 0
            price_arena = s.arenera.custom_price or 0
            diff = loaded - arrived
            merma_money = 0.0
            diff_tn_visual = 0.0
            
            if s.arenera.cert_type != 'salida':
                if diff > tol_tn: 
                    diff_tn_visual = diff - tol_tn
                    merma_money = diff_tn_visual * price_arena
                payable = arrived - diff_tn_visual
            else:
                payable = loaded

            neto = (payable * price_flete) - merma_money
            iva  = max(0, neto * 1.21)

        # Fechas
        d_cert = s.cert_fecha or s.date
        days = s.transportista.payment_days or 30
        d_pago = d_cert + timedelta(days=days)
        
        # Helper de llenado
        def add_val(mat, d, t, v):
            if d not in mat: mat[d] = {}
            mat[d][t] = mat[d].get(t, 0) + v

        add_val(mat_neto_cert, d_cert, tid, neto)
        add_val(mat_iva_pay,   d_pago, tid, iva)
        add_val(mat_tn_out,    d_cert, tid, loaded)
        add_val(mat_tn_in,     d_cert, tid, arrived)
        add_val(mat_tn_diff,   d_cert, tid, diff_tn_visual)
        add_val(mat_trucks,    d_cert, tid, 1)

    # --- PROCESAMIENTO FINAL ---
    sorted_trans_ids = sorted(list(all_trans_ids), key=lambda x: trans_names[x])
    
    # Función interna para procesar la matriz
    def process_matrix_data(matrix_dict):
        sorted_dates = sorted(matrix_dict.keys())
        result_rows = []
        
        # Inicializamos acumuladores
        totals_by_col = {tid: 0.0 for tid in sorted_trans_ids}
        grand_total_sum = 0.0 # <--- AQUÍ SE INICIALIZA LA VARIABLE QUE DABA ERROR

        for d in sorted_dates:
            # Creamos el objeto fila (IMPORTANTE: usar "row_total" como pide el HTML)
            row_data = {"date": d, "cells": [], "row_total": 0.0}
            
            for tid in sorted_trans_ids:
                val = matrix_dict[d].get(tid, 0.0)
                row_data["cells"].append(val)
                row_data["row_total"] += val
                totals_by_col[tid] += val
            
            grand_total_sum += row_data["row_total"]
            result_rows.append(row_data)
            
        return result_rows, totals_by_col, grand_total_sum

    # Generar las 6 tablas usando la función corregida
    t1, c1, g1 = process_matrix_data(mat_neto_cert)
    t2, c2, g2 = process_matrix_data(mat_iva_pay)
    t3, c3, g3 = process_matrix_data(mat_tn_out)
    t4, c4, g4 = process_matrix_data(mat_tn_in)
    t5, c5, g5 = process_matrix_data(mat_tn_diff)
    t6, c6, g6 = process_matrix_data(mat_trucks)

    return render_template(tpl("admin_resumen"),
        start=start_date, end=end_date,
        trans_ids=sorted_trans_ids,
        trans_names=trans_names,
        tables=[
            {"title": "Monto a Certificar (Neto)", "sub": "Por Fecha Certificación", "rows": t1, "cols": c1, "tot": g1, "fmt": "money", "bg": "bg-green"},
            {"title": "Monto a Pagar (c/IVA)",     "sub": "Por Fecha PAGO", "rows": t2, "cols": c2, "tot": g2, "fmt": "money", "bg": "bg-blue"},
            {"title": "Toneladas Despachadas",    "sub": "Por Fecha Certificación", "rows": t3, "cols": c3, "tot": g3, "fmt": "float", "bg": "bg-yellow"},
            {"title": "Toneladas Recibidas",      "sub": "Por Fecha Certificación", "rows": t4, "cols": c4, "tot": g4, "fmt": "float", "bg": "bg-gray"},
            {"title": "Toneladas Diferencia (Multa)", "sub": "Por Fecha Certificación", "rows": t5, "cols": c5, "tot": g5, "fmt": "float", "bg": "bg-red"},
            {"title": "Cantidad Camiones",          "sub": "Por Fecha Certificación", "rows": t6, "cols": c6, "tot": g6, "fmt": "int",   "bg": "bg-white"},
        ]
    )

@app.route("/admin/control_arena", methods=["GET", "POST"])
@login_required
@role_required("admin", "gestion")
def admin_control_arena():
    aid = request.args.get("arenera_id")
    start_str = request.args.get("start")
    end_str   = request.args.get("end")
    
    today = get_arg_today()
    
    if not start_str:
        last_monday = today - timedelta(days=today.weekday() + 7)
        start_date = last_monday
        end_date   = last_monday + timedelta(days=6)
    else:
        try:
            start_date = date.fromisoformat(start_str)
            end_date   = date.fromisoformat(end_str)
        except ValueError:
            start_date = end_date = today

    areneras = User.query.filter_by(tipo="arenera", parent_id=None).order_by(User.username).all()
    
    shipments = []
    total_tn = 0.0
    total_money = 0.0
    selected_arenera = None

    if aid and aid != "none":
        selected_arenera = User.query.get(int(aid))
        
        # Traemos solo lo CERTIFICADO (es decir, lo que llegó y validaste)
        q = (Shipment.query
             .filter(Shipment.arenera_id == int(aid))
             .filter(Shipment.cert_status == "Certificado")
             .filter(Shipment.date >= start_date, Shipment.date <= end_date)
             .order_by(Shipment.date.asc())
             )
        shipments = q.all()
        
        for s in shipments:
            # LÓGICA CORREGIDA:
            # A la arenera SIEMPRE se le paga lo que dice su Remito de Salida (peso_neto_arenera).
            # La validación de si llegó o no, ya está hecha (por eso está Certificado).
            # No usamos el peso SBE para pagarle a la arenera.
            
            peso = s.peso_neto_arenera or 0
            
            # Precio congelado o actual
            precio = s.frozen_arena_price if s.frozen_arena_price is not None else (s.arenera.custom_price or 0)
            
            monto = peso * precio
            
            total_tn += peso
            total_money += monto
            
            s._calc_precio = precio
            s._calc_total_arena = monto

    return render_template(tpl("admin_control_arena"),
                           areneras=areneras,
                           shipments=shipments,
                           selected_arenera=selected_arenera,
                           sel_aid=aid,
                           start=start_date, end=end_date,
                           total_tn=total_tn, total_money=total_money)

@app.route("/admin/control_flete", methods=["GET", "POST"])
@login_required
@role_required("admin", "gestion")
def admin_control_flete():
    tid = request.args.get("transportista_id")
    start_str = request.args.get("start")
    end_str   = request.args.get("end")
    date_mode = request.args.get("mode", "cert") 
    
    today = get_arg_today()
    
    if not start_str:
        start_date = end_date = today
    else:
        try:
            start_date = date.fromisoformat(start_str)
            end_date   = date.fromisoformat(end_str)
        except ValueError:
            start_date = end_date = today

    transportistas = User.query.filter_by(tipo="transportista").order_by(User.username).all()
    
    shipments = []
    total_tn = 0.0
    total_neto = 0.0
    total_iva = 0.0
    total_final = 0.0
    selected_trans = None

    if tid and tid != "none":
        selected_trans = User.query.get(int(tid))
        
        q = Shipment.query.filter(
            Shipment.transportista_id == int(tid),
            Shipment.cert_status == "Certificado"
        )
        
        if date_mode == 'travel':
            q = q.filter(Shipment.date >= start_date, Shipment.date <= end_date).order_by(Shipment.date.asc())
        else:
            q = q.filter(Shipment.cert_fecha >= start_date, Shipment.cert_fecha <= end_date).order_by(Shipment.cert_fecha.asc())
            
        shipments = q.all()
        
        conf = get_config()
        tol_tn = conf.tolerance_kg / 1000.0
        
        for s in shipments:
            # 1. Obtener Valores (Snapshot o Cálculo)
            if s.frozen_flete_neto is not None:
                # Datos Congelados
                precio_flete = s.frozen_flete_price or 0
                merma_money  = s.frozen_merma_money or 0
                neto         = s.frozen_flete_neto or 0
                
                # CORRECCIÓN HISTÓRICA: 
                # Si el valor congelado de IVA era el total por error, lo recalculamos bien para mostrarlo.
                # Si tu DB ya tiene datos mal guardados, esto lo arregla visualmente:
                iva_monto = neto * 0.21
                
            else:
                # Datos Calculados (Fallback)
                loaded  = s.peso_neto_arenera or 0
                arrived = s.final_peso or 0
                precio_flete = s.transportista.custom_price or 0
                precio_arena = s.arenera.custom_price or 0
                
                tn_base = loaded if s.arenera.cert_type == 'salida' else arrived
                
                merma_money = 0.0
                if s.arenera.cert_type != 'salida':
                    diff = loaded - arrived
                    if diff > tol_tn:
                        merma_money = (diff - tol_tn) * precio_arena

                neto = (tn_base * precio_flete) - merma_money
                
                # CORRECCIÓN MATEMÁTICA AQUÍ:
                # El IVA es el 21% del neto (0.21), no el 121% (1.21)
                iva_monto = max(0, neto * 0.21)

            # Acumuladores
            total_tn   += (s.final_peso or 0)
            total_neto += neto
            total_iva  += iva_monto
            
            # Datos para la vista
            s._calc_price = precio_flete
            s._calc_merma = merma_money
            s._calc_neto  = neto
            
            # El total de la fila es Neto + IVA
            s._calc_total = neto + iva_monto

    # Total Final General
    total_final = total_neto + total_iva

    return render_template(tpl("admin_control_flete"),
                           transportistas=transportistas,
                           shipments=shipments,
                           selected_trans=selected_trans,
                           sel_tid=tid,
                           start=start_date, end=end_date,
                           date_mode=date_mode,
                           total_tn=total_tn, 
                           total_neto=total_neto,
                           total_iva=total_iva,
                           total_final=total_final)

@app.route("/admin/sync_sbe")
@login_required
@role_required("admin")
def sync_sbe():
    from sync_service import run_sbe_sync
    matches, err = run_sbe_sync(db, Shipment)
    if err: 
        flash(err, "error")
    else: 
        flash(f"Sync ok: {matches} cruces realizados.", "success")
    return redirect(request.referrer or url_for("admin_panel"))

@app.route("/admin/generate_pdf", methods=["GET"])
@login_required
@role_required("admin", "gestion")
def generate_pdf():
    # Llama al motor
    pdf_bytes, fname, error = _create_pdf_internal(
        request.args.get("target_id"),
        request.args.get("type"),
        request.args.get("start"),
        request.args.get("end"),
        request.args.get("mode", "cert")
    )
    
    if error:
        flash(error, "error")
        return redirect(request.referrer or url_for('admin_panel'))
        
    response = make_response(pdf_bytes)
    response.headers['Content-Type'] = 'application/pdf'
    # 'inline' para verlo en el navegador, 'attachment' para bajarlo directo
    response.headers['Content-Disposition'] = f'inline; filename={fname}'
    return response

def _create_pdf_internal(target_id, target_type, start_str, end_str, date_mode):
    """Helper interno: Genera los bytes del PDF y el nombre del archivo."""
    from xhtml2pdf import pisa
    
    # 1. Fechas
    today = get_arg_today()
    try:
        start_date = date.fromisoformat(start_str)
        end_date   = date.fromisoformat(end_str)
    except (ValueError, TypeError):
        start_date = end_date = today

    # 2. Query
    target_user = User.query.get(int(target_id))
    if not target_user: return None, None, "Usuario no encontrado"

    q = Shipment.query.filter(Shipment.cert_status == "Certificado")
    
    if target_type == 'transportista':
        q = q.filter(Shipment.transportista_id == target_user.id)
    else:
        q = q.filter(Shipment.arenera_id == target_user.id)

    if date_mode == 'travel':
        q = q.filter(Shipment.date >= start_date, Shipment.date <= end_date).order_by(Shipment.date.asc())
    else:
        q = q.filter(Shipment.cert_fecha >= start_date, Shipment.cert_fecha <= end_date).order_by(Shipment.cert_fecha.asc())

    shipments = q.all()
    if not shipments: return None, None, "No hay datos certificados."

    # 3. Cálculos
    total_tn = 0.0
    subtotal = 0.0
    descuento_dinero = 0.0
    tn_merma = 0.0
    items = []
    
    ref_price = target_user.custom_price or 0

    for s in shipments:
        # Lógica Híbrida (Snapshot vs Fallback)
        if s.frozen_flete_neto is not None:
            precio_unit = s.frozen_flete_price if target_type == 'transportista' else s.frozen_arena_price
            if target_type == 'transportista':
                neto_linea = s.frozen_flete_neto
                merma_linea = s.frozen_merma_money or 0
                peso_pagable = (neto_linea + merma_linea) / (precio_unit if precio_unit else 1)
            else:
                peso_pagable = s.peso_neto_arenera or 0
                neto_linea = peso_pagable * (precio_unit or 0)
                merma_linea = 0
        else:
            # Fallback simplificado
            precio_unit = ref_price
            merma_linea = 0
            peso_pagable = s.final_peso if target_type=='transportista' else s.peso_neto_arenera
            neto_linea = peso_pagable * precio_unit

        total_tn += peso_pagable
        subtotal += (neto_linea + merma_linea)
        descuento_dinero += merma_linea
        
        if merma_linea > 0 and precio_unit:
             tn_merma += (merma_linea / precio_unit)

        items.append({
            "remito": s.final_remito,
            "f_salida": s.date.strftime("%d/%m"),
            "f_llegada": s.sbe_fecha_llegada.strftime("%d/%m") if s.sbe_fecha_llegada else "-",
            "f_certif": s.cert_fecha.strftime("%d/%m") if s.cert_fecha else "-",
            "chofer": s.chofer[:18], 
            "patente": s.tractor,
            "peso": peso_pagable,
            "total_linea": neto_linea
        })

    total_neto = subtotal - descuento_dinero
    total_iva_inc = total_neto * 1.21

    # 4. Renderizado
    html = render_template(
        "pdf_template.html",
        target_id=target_user.id,
        tipo_reporte="Flete / Transporte" if target_type == 'transportista' else "Venta de Arena",
        empresa=target_user.username,
        fecha_emision=today.strftime("%d/%m/%Y"),
        periodo_inicio=start_date.strftime("%d/%m/%Y"),
        periodo_fin=end_date.strftime("%d/%m/%Y"),
        items=items,
        total_tn=total_tn,
        precio_unitario=ref_price,
        subtotal=subtotal,
        descuento_dinero=descuento_dinero,
        tn_merma=tn_merma,
        total_neto=total_neto,
        total_iva_inc=total_iva_inc
    )

    pdf_io = io.BytesIO()
    pisa_status = pisa.CreatePDF(io.StringIO(html), dest=pdf_io)

    if pisa_status.err: return None, None, f"Error PDF: {pisa_status.err}"

    pdf_io.seek(0)
    fname = f"Certif_{target_user.username}_{start_date.strftime('%d%m')}.pdf"
    
    return pdf_io.getvalue(), fname, None

@app.route("/admin/preview_send", methods=["GET"])
@login_required
@role_required("admin", "gestion")
def preview_send():
    target_id   = request.args.get("target_id")
    target_type = request.args.get("type")
    start       = request.args.get("start")
    end         = request.args.get("end")
    mode        = request.args.get("mode", "cert")

    target_user = User.query.get_or_404(int(target_id))
    
    # Generamos la URL para que el iframe muestre el PDF
    pdf_src = url_for('generate_pdf', target_id=target_id, type=target_type, start=start, end=end, mode=mode)
    
    return render_template(tpl("admin_preview_send"), 
                           user=target_user, 
                           pdf_url=pdf_src,
                           # Pasamos los datos para el form oculto
                           target_id=target_id, target_type=target_type, start=start, end=end, mode=mode)

@app.post("/admin/send_email_action")
@login_required
@role_required("admin")
def send_email_action():
    # 1. Recuperar parámetros del formulario
    target_id = request.form.get("target_id")
    target_type = request.form.get("target_type")
    start = request.form.get("start")
    end = request.form.get("end")
    mode = request.form.get("mode")
    
    email_dest = request.form.get("email_dest")
    subject    = request.form.get("subject")
    body       = request.form.get("body")

    if not email_dest:
        flash("El destinatario no tiene email configurado.", "error")
        return redirect(request.referrer)

    # 2. Generar el PDF en memoria (Bytes)
    # Llamamos a nuestro motor interno
    pdf_bytes, fname, error = _create_pdf_internal(
        target_id, target_type, start, end, mode
    )
    
    if error or not pdf_bytes:
        flash(f"Error generando el PDF: {error}", "error")
        return redirect(request.referrer)

    # 3. Enviar Email vía Microsoft Graph
    try:
        send_email_graph(
            destinatario=email_dest,
            asunto=subject,
            cuerpo=body,
            attachment_bytes=pdf_bytes,
            attachment_name=fname
        )
        flash(f"✅ Liquidación enviada correctamente a {email_dest} (vía Microsoft)", "success")
        
    except Exception as e:
        # Loguear error en consola para debug
        print(f"Error enviando mail: {e}")
        flash(f"❌ Error enviando correo: {e}", "error")

    # 4. Volver al panel correspondiente
    if target_type == 'transportista':
        return redirect(url_for('admin_control_flete', transportista_id=target_id, start=start, end=end, mode=mode))
    else:
        return redirect(url_for('admin_control_arena', arenera_id=target_id, start=start, end=end))

# -----------------------------------------------------------
# MICROSOFT GRAPH MAIL SERVICE
# -----------------------------------------------------------

def get_graph_token_mail():
    """Obtiene token para enviar correos (Scope .default incluye Mail.Send)"""
    tenant_id = os.getenv("GRAPH_TENANT_ID")
    client_id = os.getenv("GRAPH_CLIENT_ID")
    secret    = os.getenv("GRAPH_CLIENT_SECRET")
    
    url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    data = {
        "client_id": client_id,
        "client_secret": secret,
        "scope": "https://graph.microsoft.com/.default",
        "grant_type": "client_credentials"
    }
    
    try:
        resp = requests.post(url, data=data)
        resp.raise_for_status()
        return resp.json().get("access_token")
    except Exception as e:
        print(f"Error obteniendo Token Graph: {e}")
        return None

def send_email_graph(destinatario, asunto, cuerpo, attachment_bytes=None, attachment_name="documento.pdf"):
    """Envía correo con adjunto opcional usando Microsoft Graph API"""
    token = get_graph_token_mail()
    if not token:
        raise Exception("No se pudo obtener el token de Microsoft Graph.")

    sender = os.getenv("MAIL_SENDER_EMAIL")
    url = f"https://graph.microsoft.com/v1.0/users/{sender}/sendMail"
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    # Estructura base del mensaje
    message_payload = {
        "subject": asunto,
        "body": {
            "contentType": "HTML", # O "Text"
            "content": cuerpo.replace("\n", "<br>") # Convertir saltos de línea a HTML
        },
        "toRecipients": [
            {"emailAddress": {"address": destinatario}}
        ]
    }

    # Agregar adjunto si existe
    if attachment_bytes:
        # Graph requiere el archivo en Base64 string
        b64_content = base64.b64encode(attachment_bytes).decode("utf-8")
        
        message_payload["attachments"] = [
            {
                "@odata.type": "#microsoft.graph.fileAttachment",
                "name": attachment_name,
                "contentType": "application/pdf",
                "contentBytes": b64_content
            }
        ]

    payload = {"message": message_payload, "saveToSentItems": "true"}

    resp = requests.post(url, headers=headers, json=payload)
    
    if not resp.ok:
        raise Exception(f"Error Graph API: {resp.status_code} - {resp.text}")
        
    return True

# ----------------------------
# MAIN
# ----------------------------
if __name__ == "__main__":
    app.run(host=os.getenv("FLASK_HOST", "0.0.0.0"),
            port=int(os.getenv("FLASK_PORT", "5000")),
            debug=os.getenv("FLASK_DEBUG", "1") == "1")