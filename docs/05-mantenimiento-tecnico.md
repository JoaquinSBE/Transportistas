# 05 — Administración técnica (backups, updates, controles)

## 1. Objetivo
Garantizar continuidad operacional mediante:
- Backups consistentes
- Restore probado (no solo “guardar”)
- Actualizaciones y controles preventivos
- Seguridad técnica básica

---

## 2. Backups — qué se respalda
### 2.1 Datos críticos
1) **Base PostgreSQL** (principal fuente de verdad)
2) **Variables de entorno** (secretos y configuración) — respaldadas fuera de Render (vault seguro)
3) **Repo Git** (código + templates + docs)

---

## 3. Backups de PostgreSQL en Render
Render provee export/restore desde la sección “Recovery” de la DB. :contentReference[oaicite:24]{index=24}

### 3.1 Frecuencia recomendada
- **Diario** (si hay operación diaria)
- **Antes de cada deploy relevante** (cambio de reglas, sync, modelos)
- **Retención** (sugerida): 30 días + 3 backups mensuales

### 3.2 Procedimiento de backup (operativo)
1) Render Dashboard → Database → Recovery
2) Descargar export `.dir.tar.gz` (archivo incluye timestamp) :contentReference[oaicite:25]{index=25}
3) Guardar en repositorio seguro (no en el repo Git) con control de acceso
4) Registrar en bitácora:
   - fecha/hora
   - operador
   - motivo (diario/pre-deploy/etc.)
   - checksum (opcional pero recomendado)

### 3.3 Restore (procedimiento estándar)
Objetivo: poder restaurar tanto a:
- una DB nueva en Render
- un entorno local para validación

Render detalla el flujo general de restauración con export. :contentReference[oaicite:26]{index=26}

Pasos (conceptual):
1) Provisionar DB destino (nueva o existente)
2) Obtener su connection string (preferible external/administrativa)
3) Restaurar el backup (herramienta estándar de Postgres)
4) Validar integridad (ver sección 4)

**Advertencia crítica**
Render indica que si se elimina la DB, no retiene backups/snapshots. Descargar backups antes de borrar. :contentReference[oaicite:27]{index=27}

---

## 4. Verificación de restore (obligatoria)
Un backup solo “vale” si se puede restaurar.

Checklist de verificación:
- [ ] La DB levanta y acepta conexiones
- [ ] Existen tablas: `user`, `shipment`, `quota`, `chofer`, `system_config`, `tariff`
- [ ] Conteos razonables:
  - `select count(*) from shipment;`
  - `select count(*) from user;`
- [ ] Login admin funciona con credenciales esperadas
- [ ] Operación crítica (listar/crear shipment) funciona

Frecuencia de prueba:
- Mensual (mínimo)
- Inmediata si hubo incidentes de DB

---

## 5. Actualizaciones y seguridad técnica
### 5.1 Gestión de dependencias
- Dedicada: revisar CVEs y actualizaciones de:
  - Flask / SQLAlchemy / psycopg
  - msal / requests
  - pandas
- Política recomendada:
  - Sprint mensual de mantenimiento
  - Deploy con ventana controlada
  - Smoke tests (docs/03)

### 5.2 Python version (Render)
Definir `PYTHON_VERSION` para reproducibilidad. :contentReference[oaicite:28]{index=28}

### 5.3 Secretos
- Rotación trimestral para:
  - `FLASK_SECRET_KEY`
  - `GRAPH_CLIENT_SECRET`
  - Credenciales mail
- Guardar secretos en:
  - Render env vars
  - vault externo (backup)

---

## 6. Controles técnicos preventivos
### 6.1 Concurrencia de sync
Riesgo: dos ejecuciones simultáneas del sync → inconsistencias o updates duplicados.
Mitigaciones recomendadas (prioridad alta si se automatiza):
- Lock por DB (tabla `sync_lock` o advisory lock)
- Evitar APScheduler en multi-worker (ver sección 6.2)
- Ejecutar sync desde Cron Job único

### 6.2 Scheduler interno (APScheduler) — riesgo en producción
La app programa `enviar_alertas_viernes` dentro del proceso web.
En escenarios con múltiples workers/instancias, puede ejecutarse más de una vez.

Recomendación:
- Mover a Cron Job en Render (servicio dedicado) :contentReference[oaicite:29]{index=29}
- Dejar el web service sin scheduler embebido (o protegido con lock DB)

---

## 7. Cambios estructurales (DB)
Actualmente se usa `db.create_all()` en arranque (sin migraciones).
Implicancia:
- No hay control formal de versiones de esquema.
Recomendación (evolución):
- Introducir Alembic/Flask-Migrate
- Pipeline de migrations en deploy (pre-start)

---

## 8. Bitácora y auditoría
Mantener registro de:
- Backups (fecha/hora, quién, dónde)
- Restores de prueba (resultado)
- Rotación de secretos
- Deploys y rollbacks
- Incidentes SEV1/SEV2
