# -*- coding: utf-8 -*-
import os
import io, csv
from datetime import datetime, date, timedelta
from functools import wraps
from sqlalchemy import func, case, cast, Integer, text
from flask import Flask, render_template, request, redirect, url_for, session, flash, abort, make_response
from flask_sqlalchemy import SQLAlchemy
from werkzeug.security import generate_password_hash, check_password_hash

# ----------------------------
# Configuración por .env
# ----------------------------
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass

SECRET_KEY   = os.getenv("FLASK_SECRET_KEY") or os.getenv("SECRET_KEY", "clave-super-secreta-para-dev")
TPL_EXT      = os.getenv("TPL_EXT", ".html")
PASSWORD_LOG = os.getenv("PASSWORD_LOG", os.path.join("Data", "passwords.log"))
ADMIN_USER   = os.getenv("ADMIN_USER", "admin")
ADMIN_PASS   = os.getenv("ADMIN_PASS", "admin123")

# Detección simple de Render
IS_RENDER = bool(os.getenv("RENDER") or os.getenv("RENDER_EXTERNAL_URL"))

# Esquema
DB_SCHEMA  = os.getenv("DB_SCHEMA", "transportistas")

# DATABASE_URL: en Render es obligatorio, en local podés caer a SQLite
DATABASE_URL = (os.getenv("DATABASE_URL") or "").strip()
if not DATABASE_URL:
    if IS_RENDER:
        raise RuntimeError("DATABASE_URL no está definido en producción")
    # Sólo local
    DATABASE_URL = "sqlite:///instance/app.db"

# Normalización a psycopg3
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://","postgresql+psycopg://", 1)
elif DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = "postgresql+psycopg://" + DATABASE_URL.split("://", 1)[1]

# search_path por URL (transportistas,public) — evita usar event connect
if DATABASE_URL.startswith("postgresql+psycopg://"):
    sep = '&' if '?' in DATABASE_URL else '?'
    DATABASE_URL = f"{DATABASE_URL}{sep}options=-csearch_path%3D{DB_SCHEMA},public"

# ----------------------------
# Inicialización Flask / DB
# ----------------------------

app = Flask(__name__, template_folder="templates")

# Secret
app.secret_key = os.getenv("FLASK_SECRET_KEY") or os.getenv("SECRET_KEY", "clave-super-secreta-para-dev")

# Ambiente
IS_RENDER = bool(os.getenv("RENDER") or os.getenv("RENDER_EXTERNAL_URL"))
DB_SCHEMA = os.getenv("DB_SCHEMA", "transportistas")

# DATABASE_URL: obligatorio en Render, fallback a SQLite solo local
db_url = (os.getenv("DATABASE_URL") or "").strip()
if not db_url:
    if IS_RENDER:
        raise RuntimeError("DATABASE_URL no está definido en producción")
    db_url = "sqlite:///instance/app.db"

# Normalización a Psycopg3
if db_url.startswith("postgres://"):
    db_url = db_url.replace("postgres://", "postgresql+psycopg://", 1)
elif db_url.startswith("postgresql://"):
    db_url = "postgresql+psycopg://" + db_url.split("://", 1)[1]

# search_path por URL (schema primero, luego public)
if db_url.startswith("postgresql+psycopg://"):
    sep = '&' if '?' in db_url else '?'
    db_url = f"{db_url}{sep}options=-csearch_path%3D{DB_SCHEMA},public"

# Config de SQLAlchemy antes de instanciar la extensión
app.config.update(
    SQLALCHEMY_DATABASE_URI=db_url,
    SQLALCHEMY_TRACK_MODIFICATIONS=False,
    SQLALCHEMY_ENGINE_OPTIONS={
        "pool_size": 5,
        "max_overflow": 5,
        "pool_pre_ping": True,
        "pool_recycle": 1800,
    },
)

db = SQLAlchemy(app)

# Log de verificación del DSN (sin credenciales)
_safe = db_url.split("@", 1)[-1] if "@" in db_url else db_url
app.logger.info(
    "DB URI efectiva → postgresql://***:***@" + _safe
    if "postgresql+psycopg" in db_url else
    "DB URI efectiva → " + _safe
)

# --- (Primera vez) crear schema y tablas ---
# Podés controlar esto con INIT_DB_ON_BOOT=0 si no querés que corra en cada deploy
if os.getenv("INIT_DB_ON_BOOT", "1") == "1":
    with app.app_context():
        # Crear schema si no existe (solo en Postgres)
        try:
            if DB_SCHEMA and DB_SCHEMA != "public":
                db.session.execute(text(f"CREATE SCHEMA IF NOT EXISTS {DB_SCHEMA}"))
                db.session.commit()
        except Exception:
            pass
        # Crear tablas de tu app
        db.create_all()

# Aseguro carpeta para el log de contraseñas
os.makedirs(os.path.dirname(PASSWORD_LOG), exist_ok=True)

# Helper para resolver nombre de template con extensión configurable
def tpl(name: str) -> str:
    return f"{name}{TPL_EXT}"

def _resolve_range(data: dict):
    """
    Devuelve (dfrom, dto) como date, ambos inclusive.
    Soporta:
      - data['range'] in {'today','week','month'}
      - data['from'] y data['to'] ('YYYY-MM-DD')
    """
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

    hoy = date.today()
    if rng == 'today' or rng == '':
        return hoy, hoy
    if rng == 'week':
        # semana móvil: [hoy-6, hoy]
        return hoy - timedelta(days=6), hoy
    if rng == 'month':
        # mes calendario actual
        start = hoy.replace(day=1)
        # fin de mes
        if start.month == 12:
            end = start.replace(year=start.year + 1, month=1, day=1) - timedelta(days=1)
        else:
            end = start.replace(month=start.month + 1, day=1) - timedelta(days=1)
        return start, end

    # fallback
    return hoy, hoy

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
        if dv == 10: return None  # “resto 1” → caso especial
        return dv

    dv = calc_dv(pref)
    if dv is None:
        # Regla conocida: cambiar a 23 y recalcular
        pref = 23
        dv = calc_dv(pref)
        if dv is None:
            # casos muy raros
            pref = 24
            dv = calc_dv(pref)
            if dv is None:
                return ""

    return f"{pref:02d}{digits}{dv}"

# ----------------------------
# MODELOS
# ----------------------------
class User(db.Model):
    __tablename__ = "user"
    id             = db.Column(db.Integer, primary_key=True)
    username       = db.Column(db.String(50), unique=True, nullable=False, index=True)
    password_hash  = db.Column(db.String(128), nullable=False)
    tipo           = db.Column(db.String(20), nullable=False)  # 'admin' | 'transportista' | 'arenera'

    # Relación de envíos emitidos (como transportista)
    shipments_sent = db.relationship(
        "Shipment",
        foreign_keys="Shipment.transportista_id",
        backref="transportista",
        cascade="all, delete-orphan",
        lazy="dynamic",
    )

    # Relación de envíos recibidos (como arenera)
    shipments_received = db.relationship(
        "Shipment",
        foreign_keys="Shipment.arenera_id",
        backref="arenera",
        cascade="all, delete-orphan",
        lazy="dynamic",
    )

    # Relación de cuotas (como transportista)
    quotas = db.relationship(
        "Quota",
        foreign_keys="Quota.transportista_id",
        backref="transportista_user",
        cascade="all, delete-orphan",
        lazy="dynamic",
    )


class Shipment(db.Model):
    __tablename__ = "shipment"
    id               = db.Column(db.Integer, primary_key=True)
    transportista_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False, index=True)
    arenera_id       = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False, index=True)
    date             = db.Column(db.Date, nullable=False, index=True)
    chofer           = db.Column(db.String(100), nullable=False)
    dni              = db.Column(db.String(50), nullable=False)
    gender           = db.Column(db.String(10), nullable=False)
    tipo             = db.Column(db.String(20), nullable=False)
    tractor          = db.Column(db.String(20), nullable=False)
    trailer          = db.Column(db.String(20), nullable=False)
    status           = db.Column(db.String(20), nullable=False, default="En viaje", index=True)


class Quota(db.Model):
    __tablename__ = "quota"
    id               = db.Column(db.Integer, primary_key=True)
    transportista_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False, index=True)
    arenera_id       = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False, index=True)
    date             = db.Column(db.Date, nullable=False, index=True)
    limit            = db.Column(db.Integer, nullable=False, default=0)
    used             = db.Column(db.Integer, nullable=False, default=0)

    # Acceso directo a la arenera (objeto User de tipo 'arenera')
    arenera          = db.relationship("User", foreign_keys=[arenera_id])

    __table_args__   = (
        db.UniqueConstraint("transportista_id", "arenera_id", "date", name="uix_quota"),
    )

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


def role_required(role):
    def deco(f):
        @wraps(f)
        def wrapped(*args, **kwargs):
            if session.get("tipo") != role:
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
        session["user_id"] = user.id
        session["tipo"]    = user.tipo
        if user.tipo == "admin":
            return redirect(url_for("admin_panel"))
        if user.tipo == "transportista":
            return redirect(url_for("transportista_panel"))
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
            user.password_hash = generate_password_hash(nueva)
            db.session.commit()
            # Log simple de cambios de contraseña (para auditoría interna)
            try:
                with open(PASSWORD_LOG, "a", encoding="utf-8") as f:
                    f.write(f"{datetime.now().isoformat()} | {user.username} | {nueva}\n")
            except Exception:
                pass
            flash("Contraseña actualizada", "success")
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
    # Semana calendario: lunes-domingo
    today = date.today()
    week_start = today
    week_end   = week_start + timedelta(days=6)

    # Transportistas: envíos históricos + cupo disponible esta semana
    t_list = []
    for t in User.query.filter_by(tipo="transportista").all():
        total_sent = Shipment.query.filter_by(transportista_id=t.id).count()

        qs = Quota.query.filter(
            Quota.transportista_id == t.id,
            Quota.date >= week_start,
            Quota.date <= week_end
        ).all()

        assigned   = sum(q.limit for q in qs)
        available  = sum(max(q.limit - q.used, 0) for q in qs)

        t_list.append({
            "t": t,
            "sent": total_sent,
            "quota": available,     # lo que ve la tabla "Cupo semana"
            "quota_total": assigned # por si querés mostrar asignado/disp.
        })

    # Areneras: envíos históricos recibidos
    a_list = []
    for a in User.query.filter_by(tipo="arenera").all():
        total_ship = Shipment.query.filter_by(arenera_id=a.id).count()
        a_list.append({"a": a, "shipments": total_ship})

    return render_template(
        tpl("admin_panel"),
        transportistas=t_list,
        areneras=a_list,
        week_from=week_start,
        week_to=week_end,
    )

@app.route("/admin/create_user", methods=["POST"])
@login_required
@role_required("admin")
def create_user():
    uname = norm_username(request.form.get("username"))
    pwd   = (request.form.get("password") or "").strip()
    tipo  = request.form.get("tipo")

    if not uname or not pwd or tipo not in ("transportista", "arenera"):
        flash("Datos inválidos", "error")
        return redirect(url_for("admin_panel"))

    # Duplicado case-insensitive
    exists = db.session.query(User).filter(func.lower(User.username) == uname).first()
    if exists:
        flash("Ese usuario ya existe.", "error")
        return redirect(url_for("admin_panel"))

    nuevo = User(username=uname, password_hash=generate_password_hash(pwd), tipo=tipo)
    db.session.add(nuevo)
    db.session.commit()

    try:
        with open(PASSWORD_LOG, "a", encoding="utf-8") as f:
            f.write(f"{datetime.now().isoformat()} | {nuevo.username} | {pwd}\n")
    except Exception:
        pass

    flash("Usuario creado", "success")
    if tipo == "transportist":
        return redirect(url_for("admin_panel"))
    return redirect(url_for("admin_panel"))

@app.route("/admin/delete_user/<int:user_id>")
@login_required
@role_required("admin")
def delete_user(user_id):
    u = User.query.get_or_404(user_id)
    db.session.delete(u)
    db.session.commit()
    flash("Usuario eliminado", "success")
    return redirect(url_for("admin_panel"))

# --- Cuotas (admin): vista de 7 días desde una fecha de inicio, guardado masivo ---
@app.route("/admin/quotas/<int:transportista_id>", methods=["GET", "POST"])
@login_required
@role_required("admin")
def manage_quotas(transportista_id):
    t = User.query.get_or_404(transportista_id)
    areneras = User.query.filter_by(tipo="arenera").order_by(User.username.asc()).all()

    # fecha de inicio (GET ?start=YYYY-MM-DD) o (POST hidden start_date)
    if request.method == "POST":
        start_str = (request.form.get("start_date") or "").strip()
    else:
        start_str = (request.args.get("start") or "").strip()

    try:
        start_date = date.fromisoformat(start_str) if start_str else date.today()
    except ValueError:
        start_date = date.today()

    week_dates = [start_date + timedelta(days=i) for i in range(7)]

    if request.method == "POST":
        # Guardado masivo: inputs q_{arenera_id}_{YYYY-MM-DD}
        total_updates, total_deletes, total_inserts = 0, 0, 0

        for a in areneras:
            for d in week_dates:
                key = f"q_{a.id}_{d.isoformat()}"
                raw = request.form.get(key, "").strip()

                # leer existente
                q = Quota.query.filter_by(
                    transportista_id=transportista_id,
                    arenera_id=a.id,
                    date=d
                ).first()

                if raw == "":  # vacío => borrar si existía
                    if q:
                        db.session.delete(q)
                        total_deletes += 1
                    continue

                try:
                    v = int(raw)
                    if v < 0:
                        v = 0
                except ValueError:
                    # valor inválido => ignorar
                    continue

                if v == 0:
                    if q:
                        db.session.delete(q)
                        total_deletes += 1
                    continue

                if not q:
                    q = Quota(
                        transportista_id=transportista_id,
                        arenera_id=a.id,
                        date=d,
                        limit=v,
                        used=0
                    )
                    db.session.add(q)
                    total_inserts += 1
                else:
                    q.limit = v
                    # no bajar used por debajo de 0 ni por encima del nuevo límite
                    q.used = min(max(q.used, 0), v)
                    total_updates += 1

        db.session.commit()
        flash(f"Cuotas guardadas · {total_inserts} nuevas, {total_updates} actualizadas, {total_deletes} eliminadas", "success")
        return redirect(url_for("manage_quotas", transportista_id=transportista_id, start=start_date.isoformat()))

    # --- Carga de datos para render ---
    existing = (
        Quota.query
        .filter(
            Quota.transportista_id == transportista_id,
            Quota.date.in_(week_dates)
        ).all()
    )
    quota_map = {(q.arenera_id, q.date): q for q in existing}

    # usados por arenera/día (viajes ya registrados)
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
        quota_map=quota_map,   # dict[(arenera_id, date)] -> Quota
        used_map=used_map,     # dict[(arenera_id, date)] -> usados (int)
        start_date=start_date
    )

# Listado general de camiones (admin)
@app.route("/admin/todos_camiones")
@login_required
@role_required("admin")
def todos_camiones():
    status        = request.args.get("status")
    transportista = request.args.get("transportista", "").strip().lower()
    arenera       = request.args.get("arenera", "").strip().lower()

    q = Shipment.query
    if status:
        q = q.filter_by(status=status)
    shipments = q.all()

    # Filtros por nombre (manual)
    if transportista:
        shipments = [s for s in shipments if transportista in s.transportista.username.lower()]
    if arenera:
        shipments = [s for s in shipments if arenera in s.arenera.username.lower()]

    return render_template(
        tpl("todos_camiones"),
        shipments=shipments,
        status=status,
        transportista=transportista,
        arenera=arenera,
    )

#----------------------------
# DASHBOARD ADMIN - API de datos
#----------------------------


@app.post("/admin/dashboard_data")
@login_required
@role_required("admin")
def admin_dashboard_data():
    data = request.get_json(silent=True) or {}
    dfrom, dto = _resolve_range(data)

    # filtro base
    base = db.session.query(Shipment).filter(
        Shipment.date >= dfrom,
        Shipment.date <= dto
    )

    # ---- KPIs totales por estado ----
    def _count_status(st):
        return db.session.query(func.count(Shipment.id)).filter(
            Shipment.date >= dfrom, Shipment.date <= dto,
            Shipment.status == st
        ).scalar() or 0

    total = db.session.query(func.count(Shipment.id)).filter(
        Shipment.date >= dfrom, Shipment.date <= dto
    ).scalar() or 0

    en_viaje   = _count_status("En viaje")
    llegados   = _count_status("Llego")
    cancelados = _count_status("Cancelado")

    kpi = {
        "rango": {"from": dfrom.isoformat(), "to": dto.isoformat()},
        "total": total,
        "en_viaje": en_viaje,
        "llegados": llegados,
        "cancelados": cancelados,
    }

    # ---- Agregado por ARENERA ----
    # username de la arenera + conteos por estado
    sub_arenera = (
        db.session.query(
            User.username.label("arenera"),
            func.count(Shipment.id).label("total"),
            func.sum(case((Shipment.status == "En viaje", 1), else_=0)).label("en_viaje"),
            func.sum(case((Shipment.status == "Llego",    1), else_=0)).label("llegados"),
            func.sum(case((Shipment.status == "Cancelado",1), else_=0)).label("cancelados"),
        )
        .join(User, Shipment.arenera_id == User.id)
        .filter(Shipment.date >= dfrom, Shipment.date <= dto)
        .group_by(User.username)
        .order_by(func.count(Shipment.id).desc())
    ).all()

    por_arenera = [
        {
            "arenera": r.arenera,
            "total": int(r.total or 0),
            "en_viaje": int(r.en_viaje or 0),
            "llegados": int(r.llegados or 0),
            "cancelados": int(r.cancelados or 0),
        }
        for r in sub_arenera
    ]

    # ---- Agregado por TRANSPORTISTA ----
    sub_trans = (
        db.session.query(
            User.username.label("transportista"),
            func.count(Shipment.id).label("total"),
            func.sum(case((Shipment.status == "En viaje", 1), else_=0)).label("en_viaje"),
            func.sum(case((Shipment.status == "Llego",    1), else_=0)).label("llegados"),
            func.sum(case((Shipment.status == "Cancelado",1), else_=0)).label("cancelados"),
        )
        .join(User, Shipment.transportista_id == User.id)
        .filter(Shipment.date >= dfrom, Shipment.date <= dto)
        .group_by(User.username)
        .order_by(func.count(Shipment.id).desc())
    ).all()

    por_transportista = [
        {
            "transportista": r.transportista,
            "total": int(r.total or 0),
            "en_viaje": int(r.en_viaje or 0),
            "llegados": int(r.llegados or 0),
            "cancelados": int(r.cancelados or 0),
        }
        for r in sub_trans
    ]

    # ---- Timeline diaria (series por estado) ----
    # armamos un diccionario por fecha para cada estado
    day = dfrom
    labels = []
    serie_lleg = []
    serie_viaje = []
    serie_canc = []

    # pre-agrupado por fecha y estado
    rows = (
        db.session.query(
            Shipment.date.label("d"),
            Shipment.status.label("s"),
            func.count(Shipment.id).label("c"),
        )
        .filter(Shipment.date >= dfrom, Shipment.date <= dto)
        .group_by(Shipment.date, Shipment.status)
        .all()
    )
    # map {(date, status): count}
    m = {(r.d, r.s): int(r.c or 0) for r in rows}

    while day <= dto:
        labels.append(day.isoformat())
        serie_lleg.append(m.get((day, "Llego"), 0))
        serie_viaje.append(m.get((day, "En viaje"), 0))
        serie_canc.append(m.get((day, "Cancelado"), 0))
        day += timedelta(days=1)

    timeline = {
        "labels": labels,
        "llegados": serie_lleg,
        "en_viaje": serie_viaje,
        "cancelados": serie_canc,
    }

    return {
        "kpi": kpi,
        "por_arenera": por_arenera,
        "por_transportista": por_transportista,
        "timeline": timeline,
    }

@app.get("/admin/dashboard")
@login_required
@role_required("admin")
def admin_dashboard():
    return render_template("admin_dashboard.html")  # nuevo template

# ----------------------------
# PANEL TRANSPORTISTA
# ----------------------------
@app.route("/transportista/panel")
@login_required
@role_required("transportista")
def transportista_panel():
    u   = User.query.get(session["user_id"])
    hoy = date.today()

    # Semana actual: lunes (inclusive) -> próximo lunes (exclusivo)
    week_start = hoy - timedelta(days=hoy.weekday())       # lunes
    next_monday = week_start + timedelta(days=7)

    # KPI
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

    cancelados_semana = (Shipment.query
        .filter(
            Shipment.transportista_id == u.id,
            Shipment.status == "Cancelado",
            Shipment.date >= week_start,
            Shipment.date <  next_monday
        ).count())

    stats = {
        "en_viaje":   en_viaje_total,
        "llegados":   llegados_semana,
        "cancelados": cancelados_semana,
        "week_from":  week_start,
        "week_to":    next_monday - timedelta(days=1),  # muestra domingo
    }

    # LISTADO COMPLETO (histórico)
    envios = (Shipment.query
              .filter_by(transportista_id=u.id)
              .order_by(Shipment.date.desc(), Shipment.id.desc())
              .all())

    # Cupos de hoy (con usados)
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
    )

@app.route("/transportista/quotas")
@login_required
@role_required("transportista")
def transportista_quotas():
    u = User.query.get(session["user_id"])
    today      = date.today()
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

    return render_template(
        tpl("transportista_quotas"),
        user=u,
        week_dates=week_dates,
        by_date=by_date,
    )


@app.route("/transportista/arenera/<int:arenera_id>", methods=["GET", "POST"])
@login_required
@role_required("transportista")
def transportista_arenera(arenera_id):
    u        = User.query.get(session["user_id"])
    date_str = request.args.get("date", date.today().isoformat())
    dt       = date.fromisoformat(date_str)

    q = Quota.query.filter_by(
        transportista_id=u.id,
        arenera_id=arenera_id,
        date=dt
    ).first_or_404()

    shipments = Shipment.query.filter_by(
        transportista_id=u.id,
        arenera_id=arenera_id,
        date=dt
    ).all()

    if request.method == "POST":
        # Borrar envío del día
        if "delete_id" in request.form:
            sid = int(request.form["delete_id"])
            s   = Shipment.query.get(sid)
            if s and s.date == dt:
                db.session.delete(s)
                db.session.commit()
            # Recalcula used
            q.used = Shipment.query.filter_by(
                transportista_id=u.id, arenera_id=arenera_id, date=dt
            ).count()
            db.session.commit()
            return redirect(url_for("transportista_arenera", arenera_id=arenera_id, date=dt.isoformat()))

        # Crear nuevo envío si hay cupo
        if q.used < q.limit:
            new = Shipment(
                transportista_id=u.id,
                arenera_id=arenera_id,
                date=dt,
                chofer=request.form["nombre_apellido"].strip(),
                dni=request.form["dni"].strip(),
                gender=request.form["gender"],
                tipo=request.form["tipo"],
                tractor=request.form["patente_tractor"].strip(),
                trailer=request.form["patente_batea"].strip(),
                status="En viaje",
            )
            db.session.add(new)
            db.session.commit()
            # Recalcular usados
            q.used = Shipment.query.filter_by(
                transportista_id=u.id, arenera_id=arenera_id, date=dt
            ).count()
            db.session.commit()
        else:
            flash("Has alcanzado tu cupo para esta fecha", "error")

        return redirect(url_for("transportista_arenera", arenera_id=arenera_id, date=dt.isoformat()))

    used  = q.used
    limit = q.limit
    return render_template(
        tpl("transportista_arenera"),
        assignment=q,
        shipments=shipments,
        used=used,
        limit=limit,
        date=dt,
    )

@app.post("/transportista/shipment/<int:ship_id>/delete")
@login_required
@role_required("transportista")
def delete_own_shipment(ship_id):
    s = Shipment.query.get_or_404(ship_id)

    # Sólo tuyo
    if s.transportista_id != session.get("user_id"):
        abort(403)

    # Borra sólo si está "En viaje"
    if s.status != "En viaje":
        flash("Sólo se puede eliminar cuando el viaje está 'En viaje'.", "error")
        return redirect(url_for("transportista_panel"))

    # Ajustar cupo usado del día/arenera (sin bajar de 0)
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
    u      = User.query.get(session["user_id"])
    status = request.args.get("status", "En viaje")
    search = request.args.get("search", "").strip().lower()

    ships = Shipment.query.filter_by(arenera_id=u.id, status=status).all()
    if search:
        ships = [s for s in ships if search in s.chofer.lower() or search in s.dni.lower()]

    stats = {
        "total": len(ships),
        "llegados": sum(1 for s in ships if s.status == "Llego"),
    }
    return render_template(tpl("arenera_panel"), user=u, shipments=ships, status=status, search=search, stats=stats)

@app.route("/arenera/history")
@login_required
@role_required("arenera")
def arenera_history():
    u = User.query.get(session["user_id"])

    # Parámetros de filtro
    start_str = (request.args.get("start_date") or "").strip()
    end_str   = (request.args.get("end_date") or "").strip()
    status    = (request.args.get("status") or "").strip()       # "", "En viaje", "Llego", "Cancelado"
    search    = (request.args.get("search") or "").strip().lower()

    # Rango por defecto: TODO el historial de esa arenera
    min_date = db.session.query(func.min(Shipment.date)).filter_by(arenera_id=u.id).scalar() or date.today()
    max_date = db.session.query(func.max(Shipment.date)).filter_by(arenera_id=u.id).scalar() or date.today()

    # Parseo robusto de fechas
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

    # Traigo TODO el rango (sin status) y aplico búsqueda en Python (evita joins)
    base_rows = (Shipment.query
                 .filter(
                     Shipment.arenera_id == u.id,
                     Shipment.date >= start_date,
                     Shipment.date <= end_date,
                 )
                 .order_by(Shipment.date.desc(), Shipment.id.desc())
                 .all())

    if search:
        needle = search
        def hit(s: Shipment) -> bool:
            return any([
                needle in (s.chofer or "").lower(),
                needle in (s.dni or "").lower(),
                needle in (s.tractor or "").lower(),
                needle in (s.trailer or "").lower(),
                needle in (s.tipo or "").lower(),
                needle in (getattr(s.transportista, "username", "") or "").lower(),
            ])
        base_rows = [s for s in base_rows if hit(s)]

    # Contadores por estado (sobre el rango + búsqueda)
    counts = {
        "total":      len(base_rows),
        "en_viaje":   sum(1 for s in base_rows if s.status == "En viaje"),
        "llegados":   sum(1 for s in base_rows if s.status == "Llego"),
        "cancelados": sum(1 for s in base_rows if s.status == "Cancelado"),
    }

    # Aplicar filtro de status para el listado (si viene)
    if status:
        rows = [s for s in base_rows if s.status == status]
    else:
        rows = base_rows

    return render_template(
        tpl("arenera_history"),
        user=u,
        shipments=rows,
        counts=counts,
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
        status=status,
        search=(request.args.get("search") or "").strip(),
    )

@app.route("/arenera/update/<int:shipment_id>", methods=["POST"])
@login_required
@role_required("arenera")
def arenera_update(shipment_id):
    s  = Shipment.query.get_or_404(shipment_id)
    st = request.form["status"]
    s.status = "Llego" if st == "Llego" else "Cancelado"
    db.session.commit()
    return redirect(request.referrer or url_for("arenera_panel"))

# imports por si faltan
import io, csv
from flask import make_response

@app.get("/arenera/export")
@login_required
@role_required("arenera")
def arenera_export():
    u = User.query.get(session["user_id"])

    start_str = (request.args.get("start") or "").strip()
    end_str   = (request.args.get("end") or "").strip()

    min_date = db.session.query(func.min(Shipment.date)).filter_by(arenera_id=u.id).scalar() or date.today()
    max_date = db.session.query(func.max(Shipment.date)).filter_by(arenera_id=u.id).scalar() or date.today()
    try:
        start_date = date.fromisoformat(start_str) if start_str else min_date
        end_date   = date.fromisoformat(end_str)   if end_str   else max_date
    except ValueError:
        start_date, end_date = min_date, max_date

    rows = (Shipment.query
            .filter(
                Shipment.arenera_id == u.id,
                Shipment.date >= start_date,
                Shipment.date <= end_date,
            )
            .order_by(Shipment.date.desc(), Shipment.id.desc())
            .all())

    sio = io.StringIO(newline="")
    sio.write("sep=;\r\n")
    w = csv.writer(sio, delimiter=";", lineterminator="\r\n")

    # ⬇️ agregamos CUIL
    w.writerow(["Fecha", "Transportista", "Chofer", "DNI", "CUIL", "Tractor", "Batea", "Tipo", "Estado"])
    for s in rows:
        cuil = calcular_cuil(getattr(s, "dni", ""), getattr(s, "gender", ""))
        w.writerow([
            s.date.strftime("%d/%m/%Y") if s.date else "",
            (s.transportista.username if s.transportista else ""),
            s.chofer or "",
            s.dni or "",
            cuil,
            s.tractor or "",
            s.trailer or "",
            s.tipo or "",
            s.status or "",
        ])

    csv_text = "\ufeff" + sio.getvalue()
    resp = make_response(csv_text)
    resp.headers["Content-Type"] = "application/vnd.ms-excel; charset=utf-8"
    fname = f"arenera_{u.username}_{start_date.strftime('%Y-%m-%d')}_{end_date.strftime('%Y-%m-%d')}.csv"
    resp.headers["Content-Disposition"] = f'attachment; filename=\"{fname}\"'
    return resp

# ----------------------------
# LOG DE CONTRASEÑAS (admin)
# ----------------------------

@app.route("/admin/password_log")
@login_required
@role_required("admin")
def password_log():
    # Tomar la ÚLTIMA contraseña registrada por usuario desde el archivo de log
    pw_map = {}
    if os.path.exists(PASSWORD_LOG):
        with open(PASSWORD_LOG, encoding="utf-8") as f:
            for line in f:
                parts = [p.strip() for p in line.strip().split("|")]
                if len(parts) < 3:
                    continue
                ts_str, user, pwd = parts[0], parts[1], parts[2]
                try:
                    ts = datetime.fromisoformat(ts_str)
                except Exception:
                    ts = datetime.min
                if (user not in pw_map) or (ts > pw_map[user][1]):
                    pw_map[user] = (pwd, ts)

    # Listar todos los usuarios y pegarles su contraseña actual (si está logueada)
    users = User.query.order_by(User.tipo.asc(), User.username.asc()).all()
    rows = []
    for u in users:
        tup = pw_map.get(u.username)
        rows.append({
            "username": u.username,
            "tipo": u.tipo,
            "password": tup[0] if tup else None,
            "timestamp": tup[1].strftime("%Y-%m-%d %H:%M") if tup else None,
        })

    return render_template(tpl("password_log"), rows=rows)

# ----------------------------
# MAIN
# ----------------------------
if __name__ == "__main__":
    # En dev se queda en 0.0.0.0 y debug True (ajustable por env si querés)
    app.run(host=os.getenv("FLASK_HOST", "0.0.0.0"), port=int(os.getenv("FLASK_PORT", "5000")), debug=os.getenv("FLASK_DEBUG", "1") == "1")