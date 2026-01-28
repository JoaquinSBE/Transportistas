# 03 — Proceso de puesta en producción (deploy) + rollback

## 1. Objetivo
Definir un proceso repetible, auditable y reversible para publicar nuevas versiones.

Render permite deploy automático por push/merge y deploy manual desde dashboard. :contentReference[oaicite:14]{index=14}

---

## 2. Estrategia de ramas (propuesta)
- `main`: producción
- `develop`: integración
- `feature/*`: cambios puntuales

Política:
- Todo cambio entra por Pull Request
- 1 reviewer mínimo
- Checklist obligatorio (sección 4)

---

## 3. Pre-requisitos antes de deploy
### 3.1 Control de configuración
Verificar variables de entorno en Render:
- `DATABASE_URL` definida
- `FLASK_SECRET_KEY` definida
- `DB_SCHEMA` definido (si no es default)
- Graph: `GRAPH_*` completos si hay sync/mails
- SharePoint links si corre sync

### 3.2 Integridad de dependencias
- Validar instalación local: `pip install -r requirements.txt`

### 3.3 Pruebas mínimas recomendadas (smoke test)
- Login admin
- Navegación a paneles
- Alta/edición de un registro (en entorno de staging si existe)
- Ejecución controlada de sync (sin afectar prod si no corresponde)

---

## 4. Checklist de release (go/no-go)
Antes de merge a `main`:
- [ ] Cambio documentado (qué, por qué, impacto)
- [ ] No se incluyen secretos en commits
- [ ] Se revisaron rutas y permisos (role_required)
- [ ] Se validó generación de PDF si se tocó `pdf_template.html`
- [ ] Se validó `sync_service` si se tocó el pipeline o SharePoint
- [ ] Se revisó performance de queries en tableros admin si se tocaron agregaciones
- [ ] Se actualizó `docs/` si cambió arquitectura o variables

---

## 5. Deploy en Render (paso a paso)
### 5.1 Conectar repo privada
- Render ↔ GitHub (conectar cuenta)
- Instalar Render GitHub App sobre repo/org (si es privada) :contentReference[oaicite:15]{index=15}

### 5.2 Crear Web Service
- Runtime: Python
- Build Command: `pip install -r requirements.txt`
- Start Command: `gunicorn app:app` :contentReference[oaicite:16]{index=16}
- Definir `PYTHON_VERSION` (recomendado) :contentReference[oaicite:17]{index=17}
- Configurar env vars (sección 3.1)

### 5.3 Deploy
- Opción A (recomendada): merge a `main` → auto-deploy (si habilitado)
- Opción B: manual deploy desde dashboard

Validación post-deploy (obligatoria):
- Revisar logs de arranque: sin `DATABASE_URL no está definido`
- Login admin
- Ejecución de flujo crítico (crear/consultar shipment)
- Si hay mails: ejecutar envío de prueba controlado

---

## 6. Plan de reversión (rollback) — “marcha atrás”
Objetivo: volver a versión anterior rápido sin detener el negocio.

### 6.1 Rollback de aplicación (código)
En Render:
- Seleccionar el deploy anterior (desde historial de deploys) y redeploy.
Render documenta deploys y redeploys en dashboard. :contentReference[oaicite:18]{index=18}

**Cuándo aplicar**
- Error de runtime (500 recurrente)
- Bug funcional crítico (carga/consulta imposible)
- Impacto en integraciones (Graph/SharePoint)

### 6.2 Rollback de base de datos
**Criterio:** solo si el deploy afectó datos de forma incorrecta y no es reversible por script.

En Render Postgres:
- Usar Recovery → descargar export y restaurar según procedimiento :contentReference[oaicite:19]{index=19}

**Advertencia**
- Render indica que si se elimina la DB, no quedan backups retenidos. :contentReference[oaicite:20]{index=20}
- Siempre validar restore en instancia separada si el tiempo lo permite.

### 6.3 Rollback de archivos (si hubiera disco persistente)
- Restaurar snapshot de disk (si está configurado) :contentReference[oaicite:21]{index=21}
- Operación irreversible: sobreescribe estado actual del disk.

---

## 7. Comunicación ante incidentes durante deploy
- Abrir incidente interno con:
  - versión desplegada
  - hora
  - síntoma
  - logs relevantes
  - decisión (rollback sí/no)
- Notificar a stakeholders según severidad (ver docs/04)
