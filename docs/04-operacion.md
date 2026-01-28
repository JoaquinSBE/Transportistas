# 04 — Administración operativa (día a día)

## 1. Objetivo
Definir tareas rutinarias, monitoreo, respuesta a incidentes y responsables.

---

## 2. Roles operativos y responsabilidades (RACI simplificado)
- **Owner técnico**: define cambios, aprueba releases, gestiona credenciales críticas.
- **Operación**: monitorea logs, valida disponibilidad, ejecuta procedimientos estándar.
- **Soporte funcional**: valida casos de negocio, reporta inconsistencias.
- **Stakeholders**: reciben reportes y estado.

---

## 3. Rutina diaria (checklist)
### 3.1 Monitoreo (Render)
- Revisar logs del Web Service (errores 500, fallas DB, timeouts)
- Revisar eventos de deploy recientes (si hubo auto-deploy)
- Revisar disponibilidad de la DB (latencia/conexión)

### 3.2 Operación de negocio (aplicación)
- Validar acceso (login admin)
- Validar paneles (admin/transportista/arenera)
- Validar que la carga/consulta de viajes funciona
- Validar que no haya “cuellos de botella” (pantallas que demoran)

---

## 4. Gestión de incidentes
### 4.1 Clasificación por severidad
- **SEV1 (Crítico):** sistema caído / login imposible / operación principal bloqueada
  - Objetivo: restaurar servicio en el menor tiempo posible
  - Acciones: rollback app inmediato o failover DB según evidencia
- **SEV2 (Alto):** funcionalidades críticas degradadas (sync no funciona, reportes fallan)
  - Objetivo: workaround + fix planificado
- **SEV3 (Medio):** errores puntuales, UI, reportes menores
- **SEV4 (Bajo):** mejoras, requests, deuda técnica

### 4.2 Procedimiento SEV1 (paso a paso)
1) Confirmar alcance: ¿todos los usuarios o un rol?
2) Revisar logs Render (traceback y eventos de deploy)
3) Verificar variables de entorno (errores de config)
4) Si el incidente coincide con deploy reciente:
   - ejecutar rollback de deploy (docs/03)
5) Validar recuperación:
   - login
   - flujo crítico (listar/cargar shipment)
6) Registrar incidente:
   - causa raíz preliminar
   - acciones tomadas
   - próximos pasos

---

## 5. Operación de sincronización SBE
### 5.1 Modos de ejecución
- Manual (admin): endpoint de sync desde panel
- Automático (recomendado): Cron Job en Render llamando `python cron_sync_runner.py` :contentReference[oaicite:22]{index=22}

### 5.2 Controles operativos del sync
- Verificar que Graph token se obtiene correctamente
- Verificar que SharePoint links están vigentes
- Verificar que updates no duplican datos (deduplicación inteligente)
- Verificar que la corrida se registra en logs (timestamp + “envíos actualizados”)

### 5.3 Incidentología típica del sync
- Cambió el formato del Excel: falla el parseo → corregir `prepare_dataframe()`
- Credenciales Graph vencidas/rotadas: falla token → actualizar secretos
- Doble ejecución concurrente: actualizaciones duplicadas → implementar lock (ver docs/05)

---

## 6. Procedimiento de atención a usuarios
- Recolectar:
  - usuario, rol, pantalla
  - hora aproximada
  - pasos para reproducir
- Triaging:
  - es data issue (DB/sync) o UI
- Resolución:
  - corrección operativa (ejecutar sync controlado)
  - corrección técnica (hotfix + deploy)
