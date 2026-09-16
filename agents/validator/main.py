import logging
import os
import sys

sys.path.insert(0, "/app")
sys.path.insert(0, "/app/shared")

from base_agent import BaseAgent

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [VALIDATOR] %(levelname)s %(message)s"
)
log = logging.getLogger("agent.validator")

# Campos obligatorios según tipo de cliente
REQUIRED_FIELDS_BY_TYPE = {
    "enterprise": ["sector", "annual_revenue", "email", "company"],
    "mid_market": ["sector", "email", "company"],
    "smb":        ["email", "company"],
    "desconocido":["company"],
}

# Reglas de negocio
BUSINESS_RULES = [
    {"id": "BR001", "nombre": "Asesor asignado",       "campo": "asesor_asignado"},
    {"id": "BR002", "nombre": "Prioridad definida",    "campo": "prioridad"},
    {"id": "BR003", "nombre": "Tareas planificadas",   "campo": "tareas_secuenciales"},
    {"id": "BR004", "nombre": "Acciones ejecutadas",   "campo": "acciones_ejecutadas"},
]


class ValidatorAgent(BaseAgent):
    def __init__(self):
        super().__init__("validator", "validator_queue")

    def process(self, lead_id: str, payload: dict, ctx: dict):
        lead     = payload.get("datos_lead", {}) or ctx.get("datos_lead", {})
        analisis = ctx.get("resultado_analysis",  {}) or {}
        plan     = ctx.get("resultado_planning",  {}) or {}
        executor = ctx.get("resultado_executor",  {}) or {}

        errores_validacion  = []
        alertas_validacion  = []
        reglas_verificadas  = []

        tipo_cliente = analisis.get("tipo_cliente", "desconocido")

        # ── 1. Campos obligatorios del lead ───────────────────────────────────
        required = REQUIRED_FIELDS_BY_TYPE.get(tipo_cliente, REQUIRED_FIELDS_BY_TYPE["desconocido"])
        campos_faltantes = []
        for campo in required:
            val = lead.get(campo)
            if not val:
                campos_faltantes.append(campo)
                errores_validacion.append(f"Campo obligatorio faltante: {campo}")

        # ── 2. Coherencia de análisis ─────────────────────────────────────────
        if analisis.get("sector") and analisis.get("tipo_cliente"):
            reglas_verificadas.append({"regla": "Análisis completo", "ok": True})
        else:
            alertas_validacion.append("Análisis incompleto: sector o tipo_cliente faltante")
            reglas_verificadas.append({"regla": "Análisis completo", "ok": False})

        # ── 3. Reglas de negocio del plan ─────────────────────────────────────
        for rule in BUSINESS_RULES:
            campo = rule["campo"]
            val   = plan.get(campo) or executor.get(campo)
            ok    = bool(val)
            reglas_verificadas.append({"regla": rule["nombre"], "ok": ok, "id": rule["id"]})
            if not ok:
                alertas_validacion.append(f"{rule['nombre']} no completado")

        # ── 4. Coherencia ejecutor ────────────────────────────────────────────
        acciones = executor.get("acciones_ejecutadas", [])
        acciones_ok = sum(1 for a in acciones if a.get("ok"))
        if acciones and acciones_ok < len(acciones):
            alertas_validacion.append(f"{len(acciones) - acciones_ok} acciones del executor fallaron")
            reglas_verificadas.append({"regla": "Executor exitoso", "ok": False})
        elif acciones:
            reglas_verificadas.append({"regla": "Executor exitoso", "ok": True})

        # ── 5. Coherencia de prioridad ────────────────────────────────────────
        prio_analisis = analisis.get("prioridad_sugerida")
        prio_plan     = plan.get("prioridad")
        if prio_analisis and prio_plan and prio_analisis != prio_plan:
            alertas_validacion.append(
                f"Prioridad diverge: análisis={prio_analisis} plan={prio_plan}"
            )

        # ── Resultado final ───────────────────────────────────────────────────
        validado = len(errores_validacion) == 0
        reglas_ok = sum(1 for r in reglas_verificadas if r.get("ok"))
        total_reglas = len(reglas_verificadas)

        result = {
            "validado":           validado,
            "tipo_cliente":       tipo_cliente,
            "campos_faltantes":   campos_faltantes,
            "errores":            errores_validacion,
            "alertas":            alertas_validacion,
            "reglas_verificadas": reglas_verificadas,
            "resumen":            f"{reglas_ok}/{total_reglas} reglas OK",
            "requiere_intervencion_humana": not validado,
        }

        if errores_validacion:
            log.warning(f"Lead {lead_id} FALLA validación: {errores_validacion}")
            return result, "VALIDATION_FAILED"

        log.info(f"Lead {lead_id} validado OK: {reglas_ok}/{total_reglas} reglas. Alertas: {len(alertas_validacion)}")
        return result, None


if __name__ == "__main__":
    ValidatorAgent().run()
