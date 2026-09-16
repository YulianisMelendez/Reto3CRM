import logging
import os
import sys

sys.path.insert(0, "/app")
sys.path.insert(0, "/app/shared")

from base_agent import BaseAgent

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [ANALYSIS] %(levelname)s %(message)s"
)
log = logging.getLogger("agent.analysis")

# Mapeo de sectores a prioridades base
SECTOR_PRIORITY = {
    "tecnologia": "alta",
    "technology": "alta",
    "salud":      "alta",
    "health":     "alta",
    "farmaceutico":"alta",
    "pharmaceutical":"alta",
    "manufactura": "media",
    "manufacture": "media",
    "retail":      "media",
    "servicios":   "media",
    "educacion":   "baja",
    "education":   "baja",
}

REVENUE_TIER = {
    "enterprise": 1_000_000,
    "mid_market":   100_000,
    "smb":            10_000,
}


class AnalysisAgent(BaseAgent):
    def __init__(self):
        super().__init__("analysis", "analysis_queue")

    def process(self, lead_id: str, payload: dict, ctx: dict):
        lead = payload.get("datos_lead", {}) or ctx.get("datos_lead", {})

        sector         = (lead.get("sector") or "").strip().lower()
        annual_revenue = lead.get("annual_revenue") or 0
        description    = (lead.get("description") or "").lower()
        company        = lead.get("company") or ""
        email          = lead.get("email") or ""

        # ── Validación de datos insuficientes ─────────────────────────────────
        if not sector and not annual_revenue:
            log.warning(f"Lead {lead_id}: datos insuficientes (sin sector ni annual_revenue)")
            return {
                "sector":            None,
                "tipo_cliente":      None,
                "interes_comercial": None,
                "datos_relevantes":  {},
                "error":             "INSUFFICIENT_DATA",
            }, "INSUFFICIENT_DATA"

        # ── Clasificación del sector ──────────────────────────────────────────
        sector_norm = sector.replace("í","i").replace("é","e").replace("á","a")
        priority_base = SECTOR_PRIORITY.get(sector_norm, "media")

        # ── Clasificación por tamaño ──────────────────────────────────────────
        if annual_revenue >= REVENUE_TIER["enterprise"]:
            tipo_cliente = "enterprise"
        elif annual_revenue >= REVENUE_TIER["mid_market"]:
            tipo_cliente = "mid_market"
        elif annual_revenue > 0:
            tipo_cliente = "smb"
        else:
            tipo_cliente = "desconocido"

        # ── Interés comercial (inferido de descripción o sector) ──────────────
        interes = "general"
        keywords = {
            "inventario":  "modulo_inventario",
            "inventory":   "modulo_inventario",
            "ventas":      "modulo_ventas",
            "sales":       "modulo_ventas",
            "crm":         "implementacion_crm",
            "soporte":     "soporte_tecnico",
            "support":     "soporte_tecnico",
            "facturacion": "modulo_facturacion",
            "billing":     "modulo_facturacion",
            "erp":         "implementacion_erp",
            "automatiz":   "automatizacion_procesos",
        }
        for kw, interes_val in keywords.items():
            if kw in description:
                interes = interes_val
                break

        # ── Datos relevantes ──────────────────────────────────────────────────
        datos_relevantes = {
            "empresa":         company,
            "email_valido":    "@" in email,
            "sector_original": lead.get("sector"),
            "revenue_tier":    tipo_cliente,
            "prioridad_base":  priority_base,
        }

        result = {
            "sector":            sector or "no_especificado",
            "tipo_cliente":      tipo_cliente,
            "interes_comercial": interes,
            "datos_relevantes":  datos_relevantes,
            "prioridad_sugerida": priority_base,
            "confianza":          "alta" if sector and annual_revenue else "media",
        }

        log.info(f"Lead {lead_id} analizado: sector={result['sector']} tipo={tipo_cliente} interes={interes}")
        return result, None


if __name__ == "__main__":
    AnalysisAgent().run()
