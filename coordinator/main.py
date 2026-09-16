import json
import logging
import os
import signal
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Optional

import pika

sys.path.insert(0, "/app/shared")
from messaging import (
    get_rabbitmq_connection, declare_topology, build_message, publish,
    log_message, log_decision, get_redis, get_context, save_context, update_context
)

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [COORDINADOR] %(levelname)s %(message)s"
)
log = logging.getLogger("coordinator")

# Orden del pipeline de agentes
PIPELINE = ["analysis", "planning", "executor", "validator", "supervisor"]

# Mapeo de agente → estado en Redis
STATE_MAP = {
    "analysis":  "analisis_pendiente",
    "planning":  "planificacion_pendiente",
    "executor":  "ejecucion_pendiente",
    "validator": "validacion_pendiente",
    "supervisor":"supervision_pendiente",
}

RESULT_STATE_MAP = {
    "analysis":  "analisis_completado",
    "planning":  "planificacion_completada",
    "executor":  "ejecucion_completada",
    "validator": "validacion_completada",
    "supervisor":"completado",
}

ERROR_STATE_MAP = {
    "analysis":  "fallido_analisis",
    "planning":  "fallido_planificacion",
    "executor":  "fallido_ejecucion",
    "validator": "fallido_validacion",
    "supervisor":"fallido_supervision",
}

TIMEOUT_RETRIES = int(os.getenv("AGENT_TIMEOUT_RETRIES", "2"))


class Coordinator:
    def __init__(self):
        self.redis = get_redis()
        self.conn: Optional[pika.BlockingConnection] = None
        self.channel = None
        self._running = True
        # Registro de mensajes enviados → {message_id: (lead_id, agent, timestamp, retries)}
        self._pending: dict = {}

    def connect(self):
        self.conn = get_rabbitmq_connection()
        self.channel = self.conn.channel()
        declare_topology(self.channel)
        self.channel.basic_qos(prefetch_count=10)

        # Consumir eventos entrantes de webhook/SuiteCRM
        self.channel.basic_consume(
            queue="coordinator_queue",
            on_message_callback=self._on_event,
            auto_ack=False,
        )
        # Consumir respuestas de los agentes especializados
        self.channel.basic_consume(
            queue="coordinator_responses",
            on_message_callback=self._on_agent_response,
            auto_ack=False,
        )
        log.info("Coordinador listo. Escuchando coordinator_queue y coordinator_responses")

    # ── Recepción de evento inicial ───────────────────────────────────────────

    def _on_event(self, ch, method, props, body):
        try:
            msg = json.loads(body)
            lead_id    = msg.get("lead_id", "unknown")
            event_type = msg.get("task_type", "lead.created")
            payload    = msg.get("payload", {})
            log.info(f"[EVENT] lead={lead_id} type={event_type}")

            # Guardar/actualizar contexto
            ctx = get_context(self.redis, lead_id) or {}
            if not ctx:
                ctx = {
                    "lead_id":          lead_id,
                    "datos_lead":       payload.get("lead", payload),
                    "timestamp_inicio": datetime.now(timezone.utc).isoformat(),
                }

            # Verificar idempotencia: si el lead ya está completado, ignorar
            if ctx.get("estado") == "completado":
                log.info(f"Lead {lead_id} ya completado, ignorando duplicado")
                ch.basic_ack(method.delivery_tag)
                return

            # Verificar si ya está en proceso (idempotencia)
            if ctx.get("estado") not in (None, "", "recibido"):
                log.info(f"Lead {lead_id} ya en proceso ({ctx.get('estado')}), ignorando")
                ch.basic_ack(method.delivery_tag)
                return

            # Detectar datos insuficientes antes de iniciar pipeline
            lead_data = payload.get("lead", payload)
            if not lead_data.get("sector") and not lead_data.get("annual_revenue"):
                # Enviar de todos modos al análisis para que detecte el error
                log.warning(f"Lead {lead_id} sin sector ni annual_revenue — se envía a análisis para manejo de error")

            # Iniciar pipeline: primer agente = analysis
            self._dispatch_to_agent(lead_id, "analysis", ctx, event_type)
            ch.basic_ack(method.delivery_tag)

        except Exception as e:
            log.error(f"Error procesando evento: {e}", exc_info=True)
            ch.basic_nack(method.delivery_tag, requeue=False)

    # ── Recepción de respuesta de agente ──────────────────────────────────────

    def _on_agent_response(self, ch, method, props, body):
        try:
            resp = json.loads(body)
            lead_id    = resp.get("lead_id", "unknown")
            agent      = resp.get("agent", "unknown")
            status     = resp.get("status", "unknown")
            result     = resp.get("result", {})
            error_code = resp.get("error_code")
            msg_id     = resp.get("message_id")

            log.info(f"[RESPONSE] lead={lead_id} agent={agent} status={status}")

            # Remover de pendientes
            self._pending.pop(msg_id, None)

            # Obtener contexto actual
            ctx = get_context(self.redis, lead_id) or {}

            if status == "error":
                # Detener pipeline y registrar error
                self._handle_agent_error(lead_id, agent, error_code, result, ctx)
            else:
                # Guardar resultado del agente en contexto
                ctx[f"resultado_{agent}"] = result
                log_decision(lead_id, agent, result)

                # Determinar siguiente agente
                next_agent = self._next_agent(agent)
                if next_agent:
                    self._dispatch_to_agent(lead_id, next_agent, ctx, "pipeline")
                else:
                    # Pipeline completo
                    estado_final = "completado"
                    ctx["estado"]        = estado_final
                    ctx["agente_actual"] = "ninguno"
                    save_context(self.redis, lead_id, ctx)
                    log_message(lead_id, "coordinador", "sistema", "pipeline.completed",
                                ctx, "completed")
                    log.info(f"✓ Pipeline completado para lead={lead_id}")

            ch.basic_ack(method.delivery_tag)

        except Exception as e:
            log.error(f"Error procesando respuesta: {e}", exc_info=True)
            ch.basic_nack(method.delivery_tag, requeue=False)

    # ── Lógica de despacho (ÚNICO punto de publicación a colas de agentes) ────

    def _dispatch_to_agent(self, lead_id: str, agent: str, ctx: dict, reason: str):
        """El Coordinador es el ÚNICO que publica en las colas de los agentes."""
        state = STATE_MAP.get(agent, f"{agent}_pendiente")
        ctx["estado"]        = state
        ctx["agente_actual"] = agent
        save_context(self.redis, lead_id, ctx)

        msg = build_message(
            lead_id   = lead_id,
            task_type = f"task.{agent}",
            payload   = {
                "datos_lead":   ctx.get("datos_lead", {}),
                "context":      ctx,
                "pipeline_step": PIPELINE.index(agent),
            },
            origin = "coordinador",
        )

        publish(self.channel, agent, msg)

        # Registrar despacho en PostgreSQL
        log_message(lead_id, "coordinador", f"agente_{agent}", reason,
                    msg["payload"], "dispatched", message_id=msg["message_id"])

        # Registrar en pendientes para detección de timeout
        self._pending[msg["message_id"]] = {
            "lead_id":   lead_id,
            "agent":     agent,
            "timestamp": time.time(),
            "retries":   0,
            "ctx":       ctx,
        }
        log.info(f"[DISPATCH] lead={lead_id} → agente={agent} state={state}")

    def _next_agent(self, current: str) -> Optional[str]:
        try:
            idx = PIPELINE.index(current)
            return PIPELINE[idx + 1] if idx + 1 < len(PIPELINE) else None
        except ValueError:
            return None

    def _handle_agent_error(self, lead_id: str, agent: str, error_code: str, result: dict, ctx: dict):
        error_state = ERROR_STATE_MAP.get(agent, f"fallido_{agent}")
        ctx["estado"]       = error_state
        ctx["error_codigo"] = error_code
        ctx["error_detalle"]= result
        save_context(self.redis, lead_id, ctx)

        log_message(lead_id, "coordinador", f"agente_{agent}", "agent.error",
                    result, "error", error_codigo=error_code)
        log.warning(f"[ERROR] lead={lead_id} agent={agent} code={error_code} → pipeline detenido")

    # ── Detección de timeout ──────────────────────────────────────────────────

    def _check_timeouts(self):
        now = time.time()
        timeout_secs = float(os.getenv("AGENT_TIMEOUT_SECS", "60"))
        expired = [
            (mid, info) for mid, info in list(self._pending.items())
            if now - info["timestamp"] > timeout_secs
        ]
        for msg_id, info in expired:
            lead_id = info["lead_id"]
            agent   = info["agent"]
            retries = info["retries"]
            ctx     = info["ctx"]
            log.warning(f"[TIMEOUT] lead={lead_id} agent={agent} retries={retries}")

            if retries < TIMEOUT_RETRIES:
                log.info(f"Reintentando lead={lead_id} → {agent} (intento {retries+1})")
                info["retries"]   += 1
                info["timestamp"]  = now
                self._dispatch_to_agent(lead_id, agent, ctx, "retry")
                self._pending.pop(msg_id, None)

                log_message(lead_id, "coordinador", f"agente_{agent}", "timeout.retry",
                            {"retry": retries+1}, "retry")
                update_context(self.redis, lead_id, {"estado": f"reintentando_{agent}"})
            else:
                log.error(f"Max reintentos agotados para lead={lead_id} agent={agent} → DLQ")
                self._pending.pop(msg_id, None)
                update_context(self.redis, lead_id, {
                    "estado":        f"fallido_{agent}_timeout",
                    "error_codigo":  "AGENT_TIMEOUT",
                    "agente_actual": "ninguno",
                })
                log_message(lead_id, "coordinador", f"agente_{agent}", "timeout.max_retries",
                            {"agent": agent, "retries": retries}, "error", error_codigo="AGENT_TIMEOUT")

    # ── Loop principal ────────────────────────────────────────────────────────

    def run(self):
        # Crear PID file para healthcheck
        with open("/tmp/coordinator.pid", "w") as f:
            f.write(str(os.getpid()))

        signal.signal(signal.SIGTERM, self._shutdown)
        signal.signal(signal.SIGINT,  self._shutdown)

        while self._running:
            try:
                self.connect()
                log.info("Coordinador corriendo...")
                while self._running:
                    self.conn.process_data_events(time_limit=5)
                    self._check_timeouts()
            except pika.exceptions.AMQPConnectionError as e:
                log.warning(f"Conexión RabbitMQ perdida: {e}. Reconectando en 5s...")
                time.sleep(5)
            except Exception as e:
                log.error(f"Error inesperado: {e}", exc_info=True)
                time.sleep(5)

    def _shutdown(self, *_):
        log.info("Coordinador deteniendo...")
        self._running = False
        if self.conn and not self.conn.is_closed:
            self.conn.close()


if __name__ == "__main__":
    Coordinator().run()
