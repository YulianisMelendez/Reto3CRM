import logging
import os
import sys

sys.path.insert(0, "/app")
sys.path.insert(0, "/app/shared")

from base_agent import BaseAgent

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [PLANNING] %(levelname)s %(message)s"
)
log = logging.getLogger("agent.planning")

# Pool de asesores por segmento (en producción vendría de SuiteCRM).
# La asignación rota round-robin dentro de cada segmento usando un contador
# en Redis, para que leads consecutivos del mismo tipo_cliente no siempre
# caigan en el mismo asesor.
ASESORES = {
    "enterprise": ["Jhon Mendez",    "Ana Rodriguez"],
    "mid_market": ["Sofia Vargas",   "Miguel Torres"],
    "smb":        ["Laura Castillo", "Jose Herrera"],
    "desconocido":["Jhon Mendez",    "Soporte Básico"],
}

PRIORITY_SCORE = {
    "alta":  3,
    "media": 2,
    "baja":  1,
}


class PlanningAgent(BaseAgent):
    def __init__(self):
        super().__init__("planning", "planning_queue")

    def process(self, lead_id: str, payload: dict, ctx: dict):
        analisis = ctx.get("resultado_analysis", {})
        lead     = payload.get("datos_lead", {}) or ctx.get("datos_lead", {})

        if not analisis:
            log.warning(f"Lead {lead_id}: no hay resultado de análisis en contexto")
            analisis = {}

        tipo_cliente      = analisis.get("tipo_cliente",       "desconocido")
        sector            = (analisis.get("sector") or "").lower()
        interes           = analisis.get("interes_comercial",  "general")
        prioridad_suger   = analisis.get("prioridad_sugerida", "media")

        # ── Asignación de asesor (round-robin por segmento) ───────────────────
        pool = ASESORES.get(tipo_cliente) or ASESORES["desconocido"]
        contador = self.redis.incr(f"asesor_rotacion:{tipo_cliente}")
        asesor = pool[(contador - 1) % len(pool)]

        # Asesor asignado en SuiteCRM (si ya venía asignado, respetar)
        asesor_actual = lead.get("assigned_user")
        if asesor_actual:
            asesor = asesor_actual

        # ── Prioridad final ───────────────────────────────────────────────────
        revenue = lead.get("annual_revenue", 0)
        score   = PRIORITY_SCORE.get(prioridad_suger, 2)
        if revenue and revenue > 1_000_000:
            score += 1
        prioridad_final = {3: "alta", 2: "media"}.get(min(score, 3), "alta") if score >= 3 else (
            "media" if score == 2 else "baja"
        )

        # ── Plan de tareas ────────────────────────────────────────────────────
        tareas_secuenciales = [
            {"id": "T01", "nombre": "Validar datos de contacto",    "agente": "validator",  "orden": 1},
            {"id": "T02", "nombre": "Actualizar prioridad en CRM",  "agente": "executor",   "orden": 2},
            {"id": "T03", "nombre": "Asignar asesor comercial",     "agente": "executor",   "orden": 3},
            {"id": "T04", "nombre": "Registrar actividad seguimiento","agente": "executor",  "orden": 4},
        ]

        tareas_paralelas = [
            {"id": "TP01", "nombre": "Generar propuesta comercial",  "agente": "executor"},
            {"id": "TP02", "nombre": "Notificar asesor asignado",    "agente": "executor"},
        ]

        # Ajustar según interés
        if interes == "modulo_inventario":
            tareas_secuenciales.append(
                {"id": "T05", "nombre": "Crear demo módulo inventario", "agente": "executor", "orden": 5}
            )
        elif interes == "implementacion_crm":
            tareas_secuenciales.append(
                {"id": "T05", "nombre": "Agendar reunión técnica CRM", "agente": "executor", "orden": 5}
            )

        result = {
            "asesor_asignado":     asesor,
            "prioridad":           prioridad_final,
            "tareas_secuenciales": tareas_secuenciales,
            "tareas_paralelas":    tareas_paralelas,
            "sla_horas":           {"alta": 4, "media": 24, "baja": 72}.get(prioridad_final, 24),
            "notas":               f"Plan generado para cliente {tipo_cliente} en sector {sector}",
        }

        log.info(f"Lead {lead_id} planificado: asesor={asesor} prioridad={prioridad_final} tareas={len(tareas_secuenciales)}")
        return result, None


if __name__ == "__main__":
    PlanningAgent().run()
