
---

## 2) docs/01-arquitectura.md

```md
# 01 — Documentación de programas/archivos y cómo se integran

## 1. Objetivo del documento
Este documento describe:
- Qué piezas componen la aplicación (front, backend, DB, integraciones)
- Qué rol cumple cada archivo principal
- Cómo viaja la información entre componentes (flujos)
- Dependencias e impacto ante cambios

---

## 2. Componentes de la solución

### 2.1 Frontend (UI)
- Tecnología: HTML renderizado por servidor (Jinja2)
- Ubicación: `templates/`
- Rol: pantallas por perfil (admin / transportista / arenera) + template de PDF

Templates principales:
- `login.html`: login
- `admin_panel.html`, `admin_dashboard.html`: panel y tableros admin
- `transportista_panel.html`, `transportista_history.html`, `transportista_choferes.html`: operación transportista
- `arenera_panel.html`, `arenera_history.html`: operación arenera
- `pdf_template.html`: template PDF

**Punto de integración:** Los templates consumen variables provistas por las rutas Flask y form submissions que vuelven al backend.

---

### 2.2 Backend (lógica de negocio)
Archivo principal: `app.py`

Responsabilidades:
- Inicializa Flask y configuración por variables de entorno
- Inicializa SQLAlchemy (DB)
- Define modelos de datos (tablas)
- Implementa autenticación y autorización (sesión + roles)
- Implementa rutas HTTP (pantallas + endpoints)
- Integra envío de correos por Microsoft Graph
- Genera reportes (Excel/PDF) según vistas
- Scheduler interno (APScheduler) para alertas programadas

---

### 2.3 Base de datos (PostgreSQL)
- Acceso vía `SQLAlchemy` usando `DATABASE_URL`
- Usa esquema configurable `DB_SCHEMA` (default: `transportistas`)
- Tablas principales (modelos en `app.py`):
  1. `user`
  2. `shipment`
  3. `quota`
  4. `chofer`
  5. `system_config`
  6. `tariff`

**Bootstrapping:** En el arranque del servicio, se ejecuta:
- `CREATE SCHEMA IF NOT EXISTS <DB_SCHEMA>`
- `db.create_all()` (crea tablas si no existen)
- Asegura usuario admin (según `ADMIN_USER` / `ADMIN_PASS`)

---

### 2.4 Integraciones externas
#### Microsoft Graph — Autenticación (MSAL)
- Usada en `sync_service.py` y en `app.py` (mails)
- Variables:
  - `GRAPH_TENANT_ID`
  - `GRAPH_CLIENT_ID`
  - `GRAPH_CLIENT_SECRET`

#### SharePoint — Descarga de reportes SBE
- Usada en `sync_service.py`
- Variables:
  - `SHAREPOINT_LINK_1`, `SHAREPOINT_LINK_2` (histórico)
  - `SHAREPOINT_LINK_ONLINE_1`, `SHAREPOINT_LINK_ONLINE_2` (online)

#### Microsoft Graph — Envío de mails
- Usada en `app.py` con endpoint `/sendMail`
- Variable:
  - `MAIL_SENDER_EMAIL` (usuario “from” en Graph)

---

## 3. Archivos del repositorio y función

### `app.py`
**Rol:** entrypoint de la app web. Define:
- Configuración Flask/DB
- Modelos y creación inicial de tablas
- Login/logout, change password
- Paneles por rol
- Admin: gestión de usuarios, cupos, tarifas, tableros, certificación, sincronización SBE
- PDF generation
- Envío de mails
- Scheduler semanal (viernes 09:00 AR) para alertas

**Dependencias:** `sync_service.py` (para sync), `xhtml2pdf`, `openpyxl`, `requests`, `msal`, `apscheduler`, etc.

---

### `sync_service.py`
**Rol:** ETL y cruce SBE.
Pipeline conceptual:
1) Obtener token Graph (MSAL)
2) Descargar Excel(s) desde SharePoint
3) Preparar dataframe (limpieza, parseo de fechas, normalización)
4) Deduplicación inteligente (solo elimina duplicados idénticos; suma pesajes parciales)
5) Agrupar por claves funcionales (remito/fecha/patente/origen)
6) Cruzar con `Shipment` pendientes en DB
7) Actualizar campos SBE (peso, fechas, remito, patentes) aplicando tolerancias

**Puntos de riesgo:**
- Cambios en formato de Excel SharePoint impactan parseo.
- Duplicación de procesos si se ejecuta concurrentemente (ver controles en docs/04 y docs/05).

---

### `cron_sync_runner.py`
**Rol:** wrapper para ejecutar `run_sbe_sync()` con contexto Flask.
Uso típico: Cron Job de Render (recomendado) o ejecución manual.

---

### `emergency_sync_patente.py`
**Rol:** procedimiento excepcional para recuperar “huérfanos” (viajes sin cruce SBE) cruzando por patente y descartando remitos ya usados.

**Uso controlado:** solo ante incidentes operativos. Requiere registro de ejecución y validación posterior.

---

### `debug_spy.py`
**Rol:** herramienta auxiliar (debug). No debe formar parte del flujo productivo.

---

### `templates/`
**Rol:** interfaz. Acoplamiento principal: nombres de campos POST/GET deben coincidir con lo esperado por `app.py`.

---

### `Data/`
**Rol:** datos auxiliares y logs.
**Advertencia de hosting:** si no hay disco persistente, estos archivos son efímeros y se pierden con redeploy/restart (ver docs/02 y docs/05).

---

## 4. Flujos de información (cómo se conectan las piezas)

### Flujo A — Login y navegación por roles
1) Usuario entra a `/`
2) POST `/login`
3) Backend valida contra tabla `user`
4) Guarda en sesión: `user_id` y `tipo`
5) Redirige a panel correspondiente:
   - Admin: `/admin`
   - Transportista: `/transportista/panel`
   - Arenera: `/arenera`

---

### Flujo B — Operación de viajes (Shipment)
1) Transportista carga/gestiona viajes desde su panel
2) Se persiste `Shipment` en DB
3) Arenera ve y actualiza información relacionada
4) Admin supervisa, audita, certifica y reporta

---

### Flujo C — Sincronización SBE (SharePoint → DB)
1) Admin ejecuta `/admin/sync_sbe` o se ejecuta job programado
2) `sync_service.run_sbe_sync(db, Shipment)`
3) Descarga de SharePoint (Histórico + Online)
4) Normalización/deduplicación/agrupación
5) Cruce con `Shipment` pendientes
6) Update en DB
7) Auditoría en logs (Render logs)

---

### Flujo D — Reportería (PDF/Excel) y mails
1) Admin genera reporte (PDF/Excel)
2) Backend arma contenido/adjunto
3) Envía mail vía Graph `sendMail`
4) Queda trazabilidad en logs y (si aplica) en `Data/passwords.log` (cambios de pass)

---

## 5. Dependencias e impacto de cambios

### Cambiar `Shipment` (campos/reglas)
Impacta:
- `sync_service.py` (cruce y updates)
- Templates de paneles e historial
- Reportes PDF/Excel
- Reglas de certificación (tolerancias, estados)

### Cambiar SharePoint Excel (columnas/formatos)
Impacta:
- `prepare_dataframe()` en `sync_service.py`
- Deduplicación/agrupación
Requiere:
- Actualizar parseo y agregar tests de regresión con archivos reales.

### Cambiar scheduler (alertas viernes)
En producción multi-worker puede duplicar envíos.
Recomendación: mover a Cron Job de Render (docs/03, docs/05).