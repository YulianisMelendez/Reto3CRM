-- ============================================================
-- Plataforma Multi-Agente - Base de Datos
-- Docker PostgreSQL crea la BD via POSTGRES_DB y ejecuta este
-- script ya conectado a ella con ON_ERROR_STOP=1.
-- ============================================================

-- Tabla de log de mensajes (comunicaciones entre coordinador y agentes)
CREATE TABLE IF NOT EXISTS message_log (
    id            SERIAL PRIMARY KEY,
    lead_id       VARCHAR(50)  NOT NULL,
    origen        VARCHAR(100) NOT NULL,
    agente_destino VARCHAR(100),
    tipo_evento   VARCHAR(100),
    message_id    UUID,
    mensaje       JSONB,
    resultado     VARCHAR(50),
    error_codigo  VARCHAR(100),
    fecha_hora    TIMESTAMP DEFAULT NOW()
);

-- Tabla de decisiones tomadas por cada agente
CREATE TABLE IF NOT EXISTS decision_log (
    id          SERIAL PRIMARY KEY,
    lead_id     VARCHAR(50)  NOT NULL,
    agente      VARCHAR(100) NOT NULL,
    decision    JSONB,
    fecha_hora  TIMESTAMP DEFAULT NOW()
);

-- Índices para consultas rápidas
CREATE INDEX IF NOT EXISTS idx_message_log_lead_id   ON message_log(lead_id);
CREATE INDEX IF NOT EXISTS idx_message_log_origen    ON message_log(origen);
CREATE INDEX IF NOT EXISTS idx_decision_log_lead_id  ON decision_log(lead_id);
CREATE INDEX IF NOT EXISTS idx_decision_log_agente   ON decision_log(agente);

-- Vista de trazabilidad completa por lead
CREATE OR REPLACE VIEW lead_trace AS
SELECT
    ml.lead_id,
    ml.fecha_hora        AS evento_timestamp,
    ml.origen,
    ml.agente_destino,
    ml.tipo_evento,
    ml.resultado,
    ml.error_codigo,
    dl.agente            AS agente_decision,
    dl.decision,
    dl.fecha_hora        AS decision_timestamp
FROM message_log ml
LEFT JOIN decision_log dl
    ON ml.lead_id = dl.lead_id
    AND ml.agente_destino = dl.agente
ORDER BY ml.lead_id, ml.fecha_hora;

-- Grants
GRANT ALL PRIVILEGES ON ALL TABLES    IN SCHEMA public TO agentuser;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO agentuser;
GRANT SELECT ON lead_trace TO agentuser;
