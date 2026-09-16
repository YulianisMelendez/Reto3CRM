import hashlib
import json
import logging
import os
import urllib.parse
import urllib.request

log = logging.getLogger("suitecrm_client")

_NOMBRES   = ["Carlos","María","Andrés","Laura","Daniel","Valentina","Santiago","Camila","Felipe","Natalia"]
_APELLIDOS = ["García","López","Martínez","Rodríguez","Herrera","Torres","Vargas","Castillo","Mendoza","Ruiz"]

def _gen_contact_name(lead_id: str, email: str = "") -> tuple:
    if email and "@" in email:
        import re
        prefix = email.split("@")[0]
        parts = [p.capitalize() for p in re.sub(r'[0-9._\-]', ' ', prefix).split() if len(p) > 1]
        if parts:
            apellido = _APELLIDOS[abs(hash(lead_id)) % len(_APELLIDOS)]
            return parts[0], apellido
    idx = abs(hash(lead_id)) % len(_NOMBRES)
    idx2 = (abs(hash(lead_id)) + 3) % len(_APELLIDOS)
    return _NOMBRES[idx], _APELLIDOS[idx2]

SUITECRM_USER = os.getenv("SUITECRM_USER", "admin")
SUITECRM_PASS = os.getenv("SUITECRM_PASS", "Admin1234")


def _endpoint(base_url: str) -> str:
    return f"{base_url.rstrip('/')}/service/v4_1/rest.php"


def _call(base_url: str, method: str, rest_data: dict, timeout: int = 10) -> dict:
    params = urllib.parse.urlencode({
        "method":        method,
        "input_type":    "JSON",
        "response_type": "JSON",
        "rest_data":     json.dumps(rest_data),
    }).encode()
    req  = urllib.request.Request(_endpoint(base_url), data=params)
    resp = urllib.request.urlopen(req, timeout=timeout)
    return json.loads(resp.read())


def login(base_url: str) -> str:
    """Retorna session_id o lanza RuntimeError."""
    pw_hash = hashlib.md5(SUITECRM_PASS.encode()).hexdigest()
    result  = _call(base_url, "login", {
        "user_auth":       {"user_name": SUITECRM_USER, "password": pw_hash},
        "application_name": "multi-agent-reto3",
    })
    sid = result.get("id", "")
    if not sid:
        raise RuntimeError(f"Login SuiteCRM fallido: {result}")
    log.info("Sesión SuiteCRM establecida")
    return sid


def ensure_lead(base_url: str, session_id: str, lead_id: str, datos_lead: dict) -> str:
    """Busca lead por marcador interno; lo crea si no existe. Retorna suitecrm_id (UUID)."""
    marker = f"[AGENT_ID:{lead_id}]"

    # Buscar lead existente
    search = _call(base_url, "get_entry_list", {
        "session":                 session_id,
        "module_name":             "Leads",
        "query":                   f"leads.description LIKE '%{marker}%'",
        "order_by":                "",
        "offset":                  0,
        "select_fields":           ["id"],
        "link_name_to_fields_array": [],
        "max_results":             1,
        "deleted":                 0,
    })
    entries = search.get("entry_list", [])
    if entries:
        return entries[0]["id"]

    # Crear lead
    company = datos_lead.get("company") or lead_id
    email   = datos_lead.get("email", "")
    sector  = datos_lead.get("sector", "")
    desc    = f"{marker} Sector:{sector} | {datos_lead.get('description', 'Lead multi-agente')}"

    # Usar el nombre de la empresa como contacto para que se muestre correctamente
    company_words = company.split()
    first_name = company_words[0] if company_words else lead_id
    last_name  = " ".join(company_words[1:]) if len(company_words) > 1 else ""

    create = _call(base_url, "set_entry", {
        "session":     session_id,
        "module_name": "Leads",
        "name_value_list": [
            {"name": "first_name",   "value": first_name},
            {"name": "last_name",    "value": last_name},
            {"name": "company",      "value": company},
            {"name": "email1",       "value": email},
            {"name": "status",       "value": "New"},
            {"name": "description",  "value": desc},
            {"name": "lead_source",  "value": "Web Site"},
            {"name": "industry",     "value": sector},
        ]
    })
    suitecrm_id = create.get("id", "")
    if not suitecrm_id:
        raise RuntimeError(f"No se pudo crear lead en SuiteCRM: {create}")
    log.info(f"Lead creado en SuiteCRM: {lead_id} → {suitecrm_id}")
    return suitecrm_id


def update_lead(base_url: str, session_id: str, suitecrm_id: str, fields: dict) -> bool:
    """Actualiza campos de un lead. Retorna True si éxito."""
    nvl = [{"name": "id", "value": suitecrm_id}]
    nvl += [{"name": k, "value": v} for k, v in fields.items()]
    result = _call(base_url, "set_entry", {
        "session":         session_id,
        "module_name":     "Leads",
        "name_value_list": nvl,
    })
    return bool(result.get("id"))


def create_call(base_url: str, session_id: str, suitecrm_lead_id: str, name: str) -> bool:
    """Registra una llamada de seguimiento asociada al lead."""
    create = _call(base_url, "set_entry", {
        "session":     session_id,
        "module_name": "Calls",
        "name_value_list": [
            {"name": "name",             "value": name},
            {"name": "status",           "value": "Planned"},
            {"name": "direction",        "value": "Outbound"},
            {"name": "duration_hours",   "value": "0"},
            {"name": "duration_minutes", "value": "30"},
        ]
    })
    call_id = create.get("id", "")
    if not call_id:
        return False
    # Relacionar la llamada con el lead
    _call(base_url, "set_relationship", {
        "session":         session_id,
        "module_name":     "Calls",
        "module_id":       call_id,
        "link_field_name": "leads",
        "related_ids":     [suitecrm_lead_id],
        "name_value_list": [],
        "delete":          0,
    })
    return True
