# 02 — Documentación descriptiva del alojamiento (Render)

## 1. Objetivo del documento
Explica dónde vive la aplicación, cómo se sostiene operativamente en Render y qué componentes del entorno garantizan disponibilidad, seguridad básica y escalabilidad.

---

## 2. Componentes de infraestructura en Render

### 2.1 Web Service (aplicación Flask)
- Tipo: Render Web Service (runtime Python)
- Fuente: repo privada GitHub
- Build: instala dependencias y prepara entorno
- Run: ejecuta Gunicorn levantando `app:app`

Render guía de despliegue y campos Build/Start Command: :contentReference[oaicite:5]{index=5}

**Recomendación de comandos**
- Build Command:
  - `pip install -r requirements.txt`
- Start Command:
  - `gunicorn app:app`

> Si se agregan workers/threads (ej. `gunicorn -w 2 -k gthread --threads 4 app:app`) debe revisarse el scheduler interno para evitar duplicación de alertas.

---

### 2.2 Base de datos (Render PostgreSQL)
- Tipo: Render Managed PostgreSQL (o servicio equivalente)
- Conexión: `DATABASE_URL`
- Respaldo: export/restore desde “Recovery” en Render :contentReference[oaicite:6]{index=6}
- Riesgo: Render indica que **no retiene backups/snapshots** si se elimina la DB → descargar backups antes de borrar :contentReference[oaicite:7]{index=7}

---

### 2.3 Conectividad y exposición
- Expuesto a internet:
  - Web Service (HTTP/HTTPS)
- No expuesto públicamente:
  - PostgreSQL (idealmente usar internal URL o restringir acceso externo)
- Secretos:
  - Variables de entorno en Render (no en repo)

Documentación de variables de entorno en Render: :contentReference[oaicite:8]{index=8}

---

## 3. Capacidad, disponibilidad y crecimiento

### 3.1 Capacidad
Depende del “Instance Type” del Web Service.
- Escala vertical: aumentar plan (RAM/CPU)
- Escala horizontal: múltiples instancias (requiere revisar:
  - sesiones si fueran server-side
  - scheduler interno
  - jobs concurrentes)

### 3.2 Disponibilidad
- Render realiza deploys automatizados y admite deploy manual :contentReference[oaicite:11]{index=11}
- Punto de falla típico:
  - DB no disponible
  - Variables de entorno incompletas
  - Cambios de formato SharePoint

### 3.3 Crecimiento (usuarios/datos)
- Crece por:
  - volumen de `Shipment`
  - reportes históricos
  - frecuencia de sync
- Medidas:
  - Índices (ya hay `index=True` en varios campos)
  - Limpieza/archivado (política de retención)
  - Revisión de queries pesadas (admin dashboards)

---

## 4. Tareas programadas (cron)
Render soporta “Cron Jobs” para ejecutar comandos en forma periódica :contentReference[oaicite:13]{index=13}

Recomendación:
- Mover alertas programadas y/o sync automático a un Cron Job dedicado, en lugar de APScheduler embebido en el web service.
Motivo:
- Gunicorn/auto-scaling puede generar instancias duplicadas → doble ejecución.

---

## 5. Seguridad básica del entorno
- Secretos por environment variables (nunca commitear `.env`)
- `FLASK_SECRET_KEY` fuerte y rotado
- `ADMIN_PASS` fuerte, cambio post-instalación
- HTTPS habilitado; cookie secure (`USE_HTTPS=True`)
- Principio de mínimo privilegio en GitHub y Render
