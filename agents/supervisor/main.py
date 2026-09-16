import logging
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, "/app")
sys.path.insert(0, "/app/shared")

from base_agent import BaseAgent

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [SUPERVISOR] %(levelname)s %(message)s"
)
log = logging.getLogger("agent.supervisor")

SUITECRM_URL = os.getenv("SUITECRM_URL", "http://suitecrm:8080")
FORCE_SIM    = os.getenv("SUITECRM_SIM", "false").lower() == "true"


def _crm_session():
    if FORCE_SIM:
        return None
    try:
        import suitecrm_client as crm
        return crm.login(SUITECRM_URL)
    except Exception as e:
        log.warning(f"SuiteCRM no disponible en supervisor: {e}")
        return None


class SupervisorAgent(BaseAgent):
    def __init__(self):
        super().__init__("supervisor", "supervisor_queue")

    def process(self, lead_id: str, payload: dict, ctx: dict):
        analisis  = ctx.get("resultado_analysis",  {}) or {}
        plan      = ctx.get("resultado_planning",  {}) or {}
        executor  = ctx.get("resultado_executor",  {}) or {}
        validador = ctx.get("resultado_validator", {}) or {}
        lead      = payload.get("datos_lead", {}) or ctx.get("datos_lead", {})

        timestamp = datetime.now(timezone.utc).isoformat()

        # ── Consolidar resultados ─────────────────────────────────────────────
        consolidado = {
            "lead_id":      lead_id,
            "empresa":      lead.get("company", "N/A"),
            "sector":       analisis.get("sector",            "N/A"),
            "tipo_cliente": analisis.get("tipo_cliente",       "N/A"),
            "interes":      analisis.get("interes_comercial",  "N/A"),
            "prioridad":    plan.get("prioridad",              "N/A"),
            "asesor":       plan.get("asesor_asignado",        "N/A"),
            "validado":     validador.get("validado",          False),
            "alertas":      validador.get("alertas",           []),
            "acciones_crm": executor.get("acciones_ejecutadas", []),
            "modo_crm":     executor.get("actions_mode",       "simulado"),
        }

        # ── Evaluación para intervención humana ───────────────────────────────
        requiere_humano = False
        razones_humano  = []

        if validador.get("requiere_intervencion_humana"):
            requiere_humano = True
            razones_humano.append("Validación: campos obligatorios faltantes")

        if validador.get("errores"):
            requiere_humano = True
            razones_humano.extend(validador["errores"])

        acciones_fallidas = [a for a in executor.get("acciones_ejecutadas", [])
                             if not a.get("ok")]
        if acciones_fallidas:
            requiere_humano = True
            razones_humano.append(f"{len(acciones_fallidas)} acciones de CRM fallaron")

        if analisis.get("tipo_cliente") == "enterprise" and not validador.get("validado"):
            requiere_humano = True
            razones_humano.append("Cliente enterprise requiere validación manual adicional")

        # ── Decisión final ────────────────────────────────────────────────────
        if requiere_humano:
            decision    = "REQUIERE_INTERVENCION_HUMANA"
            estado_final = "pendiente_revision_humana"
            log.warning(f"Lead {lead_id} → INTERVENCIÓN HUMANA: {razones_humano}")
        else:
            decision    = "APROBADO"
            estado_final = "completado"
            log.info(f"Lead {lead_id} → APROBADO por Supervisor")

        # ── Actualización final en SuiteCRM ───────────────────────────────────
        crm_update = self._actualizar_crm_final(lead_id, estado_final, consolidado)

        result = {
            "decision":              decision,
            "estado_final":          estado_final,
            "consolidado":           consolidado,
            "requiere_humano":       requiere_humano,
            "razones_humano":        razones_humano,
            "actualizacion_crm":     crm_update,
            "timestamp_supervision": timestamp,
            "pipeline_completo":     True,
        }
        return result, None

    def _actualizar_crm_final(self, lead_id: str, estado: str, consolidado: dict) -> dict:
        session_id = _crm_session()

        if not session_id:
            log.info(f"  [SIM] Actualización final CRM {lead_id} → {estado}")
            return {
                "ok":                True,
                "modo":              "simulado",
                "estado":            estado,
                "campos_actualizados": ["status", "assigned_user_name", "rating"],
            }

        try:
            import suitecrm_client as crm

            # Recuperar suitecrm_id del Redis (lo guarda el executor)
            raw = self.redis.get(f"suitecrm_id:{lead_id}")
            if not raw:
                # Intentar buscar en SuiteCRM
                suitecrm_id = crm.ensure_lead(
                    SUITECRM_URL, session_id, lead_id,
                    {"company": consolidado.get("empresa", lead_id)}
                )
            else:
                suitecrm_id = raw.decode() if isinstance(raw, bytes) else raw

            estado_suitecrm = "Converted" if estado == "completado" else "In Process"
            ok = crm.update_lead(SUITECRM_URL, session_id, suitecrm_id, {
                "status":              estado_suitecrm,
                "assigned_user_name":  consolidado.get("asesor", ""),
                "rating":              consolidado.get("prioridad", "medium").capitalize(),
                "description":         (
                    f"[AGENT_ID:{lead_id}] Estado final: {estado} | "
                    f"Decision: {consolidado.get('tipo_cliente','N/A')}"
                ),
            })
            log.info(f"Lead {lead_id} actualizado en SuiteCRM: {estado_suitecrm}")
            return {"ok": ok, "modo": "real", "estado": estado_suitecrm,
                    "suitecrm_id": suitecrm_id}

        except Exception as e:
            log.error(f"Error en actualización final SuiteCRM: {e}")
            return {"ok": False, "modo": "real", "error": str(e), "estado": estado}


if __name__ == "__main__":
    SupervisorAgent().run()
