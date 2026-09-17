import json
import logging
import os
import signal
import sys
import threading
import time
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Optional

import pika

sys.path.insert(0, "/app/shared")
from messaging import (
    get_rabbitmq_connection, declare_topology, publish,
    log_message, get_redis
)

log = logging.getLogger("base_agent")

HEARTBEAT_INTERVAL = int(os.getenv("HEARTBEAT_INTERVAL", "30"))


class BaseAgent(ABC):
    """
    Agente especializado base.
    Subclases implementan `process(lead_id, payload, ctx) -> (result_dict, error_code_or_None)`.
    """

    def __init__(self, agent_name: str, queue_name: str):
        self.name    = agent_name
        self.queue   = queue_name
        self.redis   = get_redis()
        self.conn: Optional[pika.BlockingConnection] = None
        self.channel = None
        self._running = True

    # ── Heartbeat ─────────────────────────────────────────────────────────────

    def _heartbeat_loop(self):
        """Escribe heartbeat en Redis cada HEARTBEAT_INTERVAL segundos."""
        while self._running:
            try:
                self.redis.set(
                    f"heartbeat:{self.name}",
                    json.dumps({
                        "agent":     self.name,
                        "status":    "online",
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "pid":       os.getpid(),
                    }),
                    ex=HEARTBEAT_INTERVAL * 3,   # expira si el agente muere
                )
            except Exception:
                pass
            time.sleep(HEARTBEAT_INTERVAL)

    def _start_heartbeat(self):
        t = threading.Thread(target=self._heartbeat_loop, daemon=True)
        t.start()

    # ── Conexión y consumo ────────────────────────────────────────────────────

    def connect(self):
        self.conn    = get_rabbitmq_connection()
        self.channel = self.conn.channel()
        declare_topology(self.channel)
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(
            queue               = self.queue,
            on_message_callback = self._on_message,
            auto_ack            = False,
        )
        log.info(f"[{self.name}] Listo. Escuchando: {self.queue}")

    def _on_message(self, ch, method, props, body):
        msg_id = None
        try:
            msg     = json.loads(body)
            lead_id = msg.get("lead_id", "unknown")
            payload = msg.get("payload", {})
            msg_id  = msg.get("message_id")
            ctx     = payload.get("context", {})
            origin  = msg.get("origin", "unknown")

            log.info(f"[{self.name}] Procesando lead={lead_id} origin={origin}")

            # Solo acepta mensajes del coordinador
            if origin != "coordinador":
                log.warning(f"[{self.name}] Origen no-coordinador ({origin}), rechazando")
                ch.basic_nack(method.delivery_tag, requeue=False)
                return

            # Idempotencia
            idem_key = f"idem:{self.name}:{lead_id}:{msg_id}"
            if msg_id and self.redis.exists(idem_key):
                log.info(f"[{self.name}] Duplicado ignorado: {msg_id}")
                ch.basic_ack(method.delivery_tag)
                return
            if msg_id:
                self.redis.set(idem_key, "1", ex=3600)

            result, error_code = self.process(lead_id, payload, ctx)

            # Respuesta SIEMPRE al coordinador (nunca a otros agentes)
            response = {
                "message_id": msg_id or "",
                "lead_id":    lead_id,
                "agent":      self.name,
                "status":     "error" if error_code else "success",
                "result":     result,
                "error_code": error_code,
                "timestamp":  datetime.now(timezone.utc).isoformat(),
            }

            self.channel.basic_publish(
                exchange    = "leads.direct",
                routing_key = "coordinator_response",
                body        = json.dumps(response).encode(),
                properties  = pika.BasicProperties(
                    delivery_mode=2,
                    content_type="application/json",
                ),
            )

            # El registro de la decisión y la persistencia del contexto quedan
            # centralizados en el Coordinador (único responsable del estado
            # autorizado del lead) para no duplicar escrituras en decision_log.

            log_message(
                lead_id, f"agente_{self.name}", "coordinador",
                f"task.{self.name}.result",
                response,
                "error" if error_code else "success",
                error_codigo=error_code,
                message_id=msg_id,
            )

            log.info(f"[{self.name}] Respuesta enviada lead={lead_id} "
                     f"status={'error' if error_code else 'success'}")
            ch.basic_ack(method.delivery_tag)

        except Exception as e:
            log.error(f"[{self.name}] Error: {e}", exc_info=True)
            try:
                ch.basic_nack(method.delivery_tag, requeue=False)
            except Exception:
                pass

    @abstractmethod
    def process(self, lead_id: str, payload: dict, ctx: dict):
        """Retorna (result_dict, error_code_or_None)."""
        pass

    # ── Ciclo principal ───────────────────────────────────────────────────────

    def run(self):
        with open("/tmp/agent.pid", "w") as f:
            f.write(str(os.getpid()))

        self._start_heartbeat()

        signal.signal(signal.SIGTERM, self._shutdown)
        signal.signal(signal.SIGINT,  self._shutdown)

        while self._running:
            try:
                self.connect()
                while self._running:
                    self.conn.process_data_events(time_limit=5)
            except pika.exceptions.AMQPConnectionError as e:
                log.warning(f"[{self.name}] RabbitMQ perdido: {e}. Reconectando en 5s…")
                time.sleep(5)
            except Exception as e:
                log.error(f"[{self.name}] Error: {e}", exc_info=True)
                time.sleep(5)

    def _shutdown(self, *_):
        log.info(f"[{self.name}] Deteniendo…")
        self._running = False
        if self.conn and not self.conn.is_closed:
            self.conn.close()
