import logging
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, "/app")
sys.path.insert(0, "/app/shared")

from base_agent import BaseAgent

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [EXECUTOR] %(levelname)s %(message)s"
)
log = logging.getLogger("agent.executor")

SUITECRM_URL = os.getenv("SUITECRM_URL", "http://suitecrm:8080")
FORCE_SIM    = os.getenv("SUITECRM_SIM", "false").lower() == "true"

# Mapeo de prioridad a valores SuiteCRM
PRIORIDAD_MAP = {
    "critica": "High", "alta": "High",
    "media":   "Medium",
    "baja":    "Low",
}

# Estado de oportunidad según tipo de cliente
ESTADO_MAP = {
    "enterprise":  "Value Proposition",
    "mid-market":  "In Process",
    "smb":         "Prospecting",
}


def _crm_session():
    """Intenta login en SuiteCRM. Retorna session_id o None (fallback simulación)."""
    if FORCE_SIM:
        return None
    try:
        import suitecrm_client as crm
        return crm.login(SUITECRM_URL)
    except Exception as e:
        log.warning(f"SuiteCRM no disponible — modo simulado activado: {e}")
        return None


class ExecutorAgent(BaseAgent):
    def __init__(self):
        super().__init__("executor", "executor_queue")

    def process(self, lead_id: str, payload: dict, ctx: dict):
        plan     = ctx.get("resultado_planning", {}) or {}
        analisis = ctx.get("resultado_analysis",  {}) or {}
        lead     = payload.get("datos_lead", {}) or ctx.get("datos_lead", {})

        asesor        = plan.get("asesor_asignado", "Sin asignar")
        prioridad_raw = plan.get("prioridad", "media")
        prioridad_crm = PRIORIDAD_MAP.get(prioridad_raw.lower(), "Medium")
        tipo_cliente  = analisis.get("tipo_cliente", "smb")
        estado_crm    = ESTADO_MAP.get(tipo_cliente, "Prospecting")

        session_id   = _crm_session()
        modo         = "real" if session_id else "simulado"
        suitecrm_id  = None
        acciones     = []

        # ── 1. Registrar / encontrar lead en SuiteCRM ─────────────────────────
        if session_id:
            try:
                import suitecrm_client as crm
                suitecrm_id = crm.ensure_lead(SUITECRM_URL, session_id, lead_id, lead)
                self.redis.set(f"suitecrm_id:{lead_id}", suitecrm_id, ex=86400)
                acciones.append({"accion": "registrar_lead_crm",
                                  "valor": suitecrm_id, "ok": True, "modo": modo})
            except Exception as e:
                log.error(f"Error creando lead en SuiteCRM: {e}")
                session_id = None
                modo       = "simulado"
                acciones.append({"accion": "registrar_lead_crm",
                                  "ok": False, "error": str(e), "modo": modo})
        else:
            acciones.append({"accion": "registrar_lead_crm",
                              "valor": f"SIM_{lead_id}", "ok": True, "modo": modo})

        # ── 2. Actualizar prioridad del lead ──────────────────────────────────
        if session_id and suitecrm_id:
            try:
                import suitecrm_client as crm
                ok = crm.update_lead(SUITECRM_URL, session_id, suitecrm_id,
                                     {"rating": prioridad_crm})
                acciones.append({"accion": "actualizar_prioridad",
                                  "valor": prioridad_crm, "ok": ok, "modo": modo})
            except Exception as e:
                acciones.append({"accion": "actualizar_prioridad",
                                  "valor": prioridad_crm, "ok": False,
                                  "error": str(e), "modo": modo})
        else:
            log.info(f"  [SIM] Prioridad {lead_id} → {prioridad_crm}")
            acciones.append({"accion": "actualizar_prioridad",
                              "valor": prioridad_crm, "ok": True, "modo": modo})

        # ── 3. Asignar asesor comercial ───────────────────────────────────────
        if session_id and suitecrm_id:
            try:
                import suitecrm_client as crm
                ok = crm.update_lead(SUITECRM_URL, session_id, suitecrm_id,
                                     {"assigned_user_name": asesor})
                acciones.append({"accion": "asignar_asesor",
                                  "valor": asesor, "ok": ok, "modo": modo})
            except Exception as e:
                acciones.append({"accion": "asignar_asesor",
                                  "valor": asesor, "ok": False,
                                  "error": str(e), "modo": modo})
        else:
            log.info(f"  [SIM] Asesor {lead_id} → {asesor}")
            acciones.append({"accion": "asignar_asesor",
                              "valor": asesor, "ok": True, "modo": modo})

        # ── 4. Registrar actividad de seguimiento ─────────────────────────────
        nombre_actividad = (
            f"Seguimiento — {lead.get('company', lead_id)} | Asesor: {asesor}"
        )
        if session_id and suitecrm_id:
            try:
                import suitecrm_client as crm
                ok = crm.create_call(SUITECRM_URL, session_id, suitecrm_id,
                                     nombre_actividad)
                acciones.append({"accion": "registrar_actividad",
                                  "valor": nombre_actividad, "ok": ok, "modo": modo})
            except Exception as e:
                acciones.append({"accion": "registrar_actividad",
                                  "ok": False, "error": str(e), "modo": modo})
        else:
            log.info(f"  [SIM] Actividad {lead_id}: llamada_seguimiento")
            acciones.append({"accion": "registrar_actividad",
                              "valor": nombre_actividad, "ok": True, "modo": modo})

        # ── 5. Modificar estado de oportunidad ────────────────────────────────
        if session_id and suitecrm_id:
            try:
                import suitecrm_client as crm
                ok = crm.update_lead(SUITECRM_URL, session_id, suitecrm_id,
                                     {"status": "In Process"})
                acciones.append({"accion": "actualizar_estado",
                                  "valor": estado_crm, "ok": ok, "modo": modo})
            except Exception as e:
                acciones.append({"accion": "actualizar_estado",
                                  "valor": estado_crm, "ok": False,
                                  "error": str(e), "modo": modo})
        else:
            log.info(f"  [SIM] Estado {lead_id} → {estado_crm}")
            acciones.append({"accion": "actualizar_estado",
                              "valor": estado_crm, "ok": True, "modo": modo})

        exitosas = sum(1 for a in acciones if a.get("ok"))
        total    = len(acciones)

        result = {
            "lead_id":               lead_id,
            "suitecrm_id":           suitecrm_id or f"SIM_{lead_id}",
            "suitecrm_url":          SUITECRM_URL,
            "actions_mode":          modo,
            "modo":                  modo,
            "acciones_ejecutadas":   acciones,
            "resumen":               f"{exitosas}/{total} acciones exitosas",
            "asesor_asignado":       asesor,
            "prioridad_actualizada": prioridad_crm,
            "timestamp_ejecucion":   datetime.now(timezone.utc).isoformat(),
        }

        log.info(f"Lead {lead_id} ejecutado: {exitosas}/{total} OK (modo={modo})")
        return result, None


if __name__ == "__main__":
    ExecutorAgent().run()
