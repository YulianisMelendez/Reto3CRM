# Plataforma Multi-Agente CRM — Reto 3

Sistema distribuido de automatización de leads sobre SuiteCRM. El flujo corre sobre RabbitMQ como broker, Redis para estado en caliente y PostgreSQL para trazabilidad persistente. Seis contenedores de agentes (coordinator + 5 especializados) se orquestan vía Docker Compose junto a toda la infraestructura de soporte.

---

## Prerrequisitos

| Herramienta | Versión mínima | Notas |
|---|---|---|
| Docker Desktop | 4.x | con Docker Compose v2 integrado |
| Python | 3.10+ | para correr las pruebas desde el host |
| Git | cualquiera | opcional, solo para clonar |

> En Windows verificar que Docker Desktop esté corriendo y que WSL2 esté habilitado. Con `docker info` confirman que el daemon responde.

---

## Levantar la infraestructura

```bash
docker compose up --build -d
```

La primera vez construye todas las imágenes (~3-5 min según la red). En arranques posteriores el build es cache y tarda segundos.

Esperar hasta que todos los servicios reporten `healthy`:

```bash
docker compose ps
```

El tiempo de estabilización completo es de **~2 minutos** desde que los contenedores reportan `Up`. RabbitMQ es el que más tarda en estar listo; el coordinator y los agentes tienen retry automático al broker.

---

## Interfaces disponibles

| Servicio | URL | Usuario | Contraseña |
|---|---|---|---|
| Webhook API (Swagger) | http://localhost:8080/docs | — | — |
| Dashboard operativo | http://localhost:8888 | — | — |
| SuiteCRM mock | http://localhost:8081 | — | — |
| SuiteCRM Swagger | http://localhost:8081/docs | — | — |
| RabbitMQ Management | http://localhost:15672 | `agentuser` | `agentpass` |

> **RabbitMQ pide login dos veces**: primero un popup del navegador (HTTP Basic Auth) y luego la pantalla de login de RabbitMQ. Usar `agentuser` / `agentpass` en ambos.

---

## Arquitectura del flujo

```
Webhook API (puerto 8080)
        |
        v  POST /webhook/lead
   [Coordinator]  <-- unico publicador hacia las colas de agentes
        |
        |---> [Analysis]    -> clasifica sector, tipo de cliente y prioridad
        |---> [Planning]    -> asigna asesor y define plan de acciones
        |---> [Executor]    -> ejecuta en SuiteCRM via REST API v4.1
        |---> [Validator]   -> verifica coherencia y reglas de negocio
        `---> [Supervisor]  -> audita el ciclo y gestiona excepciones
```

- **Broker**: RabbitMQ, exchange `leads.direct` con DLX para mensajes sin consumidor.
- **Estado**: Redis, clave `lead:context:{lead_id}` con el contexto completo del lead en cada etapa.
- **Trazabilidad**: PostgreSQL, tablas `decision_log` y `message_log`, vista `lead_trace`.
- **Idempotencia**: Redis key `idem:{agent}:{lead_id}:{msg_id}` — cada agente rechaza mensajes ya procesados.
- **Heartbeat**: Todos los agentes (incluido el coordinador) escriben en Redis cada 30 segundos. Visible en el dashboard.

---

## Configurar el entorno de pruebas

Las pruebas corren desde el host, no dentro de Docker, así que necesitan las dependencias instaladas localmente.

**Activar el venv (ya creado):**

```bash
.\venv\Scripts\Activate.ps1
```

Si da error de política de ejecución, ejecutar primero:

```bash
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

Si necesitan recrear el venv desde cero:

```bash
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements_tests.txt
```

---

## Ejecutar el plan de pruebas

### Opcion A — Suite completa (27 casos)

```bash
python run_tests.py
```

Genera un archivo `resultados_pruebas_YYYYMMDD_HHMMSS.txt` con el detalle de cada caso. Umbral de aprobacion: **88% (24/27 PASS como minimo)**. Los resultados quedan en Redis y se muestran automaticamente en el dashboard.

### Opcion B — Caso individual con pytest

```bash
python -m pytest tests/08_concurrencia_idem_trazabilidad/test_CP_IDEM_001.py -v
```

```bash
python -m pytest tests/01_coordinador/ -v
```

Los tests estan organizados en 8 carpetas numeradas segun el modulo del plan de pruebas:

```
tests/
|-- 01_coordinador/                      CP-COORD-001 al 006
|-- 02_analisis/                         CP-ANAL-001 al 003
|-- 03_planificacion/                    CP-PLAN-001 al 003
|-- 04_executor/                         CP-EXEC-001 al 002
|-- 05_validador/                        CP-VALI-001 al 002
|-- 06_supervisor/                       CP-SUPE-001 al 002
|-- 07_infraestructura/                  CP-INFRA-001 al 006
`-- 08_concurrencia_idem_trazabilidad/   CP-CONCUR-001, CP-IDEM-001, CP-TRAZ-001
```

---

## Prueba manual (demostracion en vivo)

Flujo completo observable desde las interfaces sin tocar pytest.

### Paso 1 — Limpiar datos previos (opcional)

Desde PowerShell con el stack corriendo:

```bash
docker compose exec redis redis-cli FLUSHDB
```

```bash
docker compose exec postgres psql -U agentuser -d agentdb -c "TRUNCATE decision_log, message_log RESTART IDENTITY CASCADE;"
```

```bash
Invoke-WebRequest -Method DELETE -Uri "http://localhost:8081/admin/reset" -UseBasicParsing
```

### Paso 2 — Inyectar el lead via Webhook API

Abrir **http://localhost:8080/docs** → seccion `POST /webhook/lead` → **Try it out** → pegar el siguiente body y ejecutar:

```json
{
  "lead_id": "LDEMO01",
  "company": "Tech Solutions SAS",
  "sector": "Tecnologia",
  "annual_revenue": 850000,
  "email": "contacto@techsolutions.com",
  "phone": "3001234567",
  "description": "Empresa interesada en soluciones CRM para el area de ventas"
}
```

La respuesta esperada es `202 Accepted` con el `message_id` del mensaje publicado al broker.

> Para repetir la prueba usar un `lead_id` distinto cada vez: `LDEMO02`, `LDEMO03`, etc. El sistema es idempotente — un mismo ID ya procesado no dispara el pipeline de nuevo.

### Paso 3 — Observar el pipeline en el Dashboard

Abrir **http://localhost:8888** y ubicar el lead en la tabla.

Lo que se vera en tiempo real (~15 segundos):

- **Estado**: `recibido` → `analizado` → `planificado` → `ejecutado` → `validado` → `completado`
- **Agente actual**: cambia conforme cada agente procesa y responde al coordinador
- **Heartbeats**: todos los agentes muestran punto verde con timestamp reciente

### Paso 4 — Verificar el resultado en SuiteCRM

Abrir **http://localhost:8081** → aparece el lead en la tabla con:

- **Estado**: `Converted` (actualizado por el Executor)
- **Prioridad**: `Alta` / `Media` / `Baja` segun el revenue (asignado por Analysis)
- **Asesor Comercial**: asignado automaticamente segun el sector del lead
- **Actividades**: hacer clic en el lead → seccion *Actividades de Seguimiento* muestra la llamada registrada

### Paso 5 — Confirmar en RabbitMQ que no quedaron mensajes pendientes

Abrir **http://localhost:15672** → ingresar `agentuser` / `agentpass` (el navegador lo pide dos veces) → pestana **Queues and Streams**.

Todas las colas deben tener `0 Ready` y `0 Unacked`. Si hay mensajes en `leads.dlq` hay un agente con error.

---

## Apagar el stack

```bash
docker compose down
```

Para limpiar volumenes (borra datos de Redis y PostgreSQL):

```bash
docker compose down -v
```

> Despues de `down -v` los tests de trazabilidad arrancan con BD limpia, que es el estado esperado para una corrida limpia del plan de pruebas.
