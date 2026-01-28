# SBE - Transportistas (Flask + PostgreSQL) — Guía Plug & Play

## Resumen ejecutivo
Aplicación web desarrollada en **Flask** con **SQLAlchemy** y base de datos **PostgreSQL**. Provee paneles por rol (Admin / Transportista / Arenera), gestión de viajes (shipments), cupos (quotas), choferes y tarifas, con integración a **Microsoft Graph** para:
- Envío de correos (Graph `/sendMail`)
- Descarga de planillas desde SharePoint (sincronización SBE) vía MSAL

El hosting productivo actual corre en **Render** conectado a una **repo privada** en Git.

---

## Índice de documentación
1. [Arquitectura y componentes](docs/01-arquitectura.md)
2. [Alojamiento en Render](docs/02-alojamiento-render.md)
3. [Puesta en producción y rollback](docs/03-puesta-en-produccion.md)
4. [Operación diaria (runbook)](docs/04-operacion.md)
5. [Mantenimiento técnico (backups, updates, controles)](docs/05-mantenimiento-tecnico.md)

---

## Estructura del proyecto (alto nivel)
- `app.py` : aplicación Flask, modelos DB, rutas, autenticación, paneles, generación de PDF, envío de mails, scheduler interno (alertas).
- `sync_service.py` : sincronización contra SharePoint (Graph) + deduplicación/normalización + cruce con viajes pendientes en DB.
- `cron_sync_runner.py` : entrypoint para ejecutar el sync manualmente/por cron.
- `emergency_sync_patente.py` : sync de emergencia (cruce por patente) para casos puntuales.
- `templates/` : vistas HTML por rol + templates de PDF.
- `Data/` : archivos auxiliares (ej. `users.json` y logs). **En hosting sin disco persistente, esto es efímero.**

---

## Requisitos
- Python 3.10+ recomendado (ver Render: `PYTHON_VERSION`) :contentReference[oaicite:0]{index=0}
- PostgreSQL
- Credenciales Microsoft Graph (Tenant/Client/Secret)
- Links SharePoint habilitados para descarga (histórico y online)

---

## Variables de entorno (obligatorias y opcionales)

### Obligatoria
- `DATABASE_URL` (**obligatoria**). La app aborta si no está definida.

> La app ajusta automáticamente `postgres://` o `postgresql://` a `postgresql+psycopg://` y agrega `search_path` por `DB_SCHEMA`.

### Recomendadas (seguridad/operación)
- `FLASK_SECRET_KEY` (o `SECRET_KEY`) — **obligatoria en producción**.
- `DB_SCHEMA` (default: `transportistas`)
- `FLASK_ENV` = `production`
- `USE_HTTPS` = `True` (activa cookie secure)
- `ADMIN_USER` / `ADMIN_PASS` (se crea/asegura admin al boot)
- `PYTHON_VERSION` (Render) :contentReference[oaicite:1]{index=1}

### Microsoft Graph (mails y SharePoint)
- `GRAPH_TENANT_ID`
- `GRAPH_CLIENT_ID`
- `GRAPH_CLIENT_SECRET`
- `MAIL_SENDER_EMAIL` (usuario del tenant que envía correos)
- `SHAREPOINT_LINK_1` = Base logos historica SBE 1
- `SHAREPOINT_LINK_2` = Base logos hitorica SBE 2
- `SHAREPOINT_LINK_ONLINE_1`= Base logos diaria SBE 1
- `SHAREPOINT_LINK_ONLINE_2` = Base logos diaria SBE 2

### Mail (config adicional utilizada por app)
- `MAIL_SERVER`, `MAIL_PORT`, `MAIL_USE_TLS`, `MAIL_USERNAME`, `MAIL_PASSWORD`, `MAIL_DEFAULT_SENDER`

---

## Quickstart local (plug & play)

### 1) Clonar y preparar entorno
```bash
git clone <repo>
cd <repo>
python -m venv .venv
# Windows
.venv\Scripts\activate
# Linux/Mac
source .venv/bin/activate