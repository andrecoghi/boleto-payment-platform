-- Reconciliation store: local substitute for Aurora Serverless v2.
-- Populated from DynamoDB Streams by the stream_to_reconciliation Lambda.
CREATE TABLE IF NOT EXISTS boleto_reconciliation (
    boleto_id             TEXT PRIMARY KEY,
    status                TEXT NOT NULL,
    amount_cents          BIGINT NOT NULL,
    due_date              TEXT,
    bank_registration_id  TEXT,
    paid_at               TEXT,
    paid_amount_cents     BIGINT,
    amount_mismatch       BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at            TEXT NOT NULL,
    synced_at             TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_boleto_reconciliation_status ON boleto_reconciliation (status);

-- Where a human reconciliation analyst would actually look first: boletos
-- the bank says are paid but for the wrong amount.
CREATE INDEX IF NOT EXISTS idx_boleto_reconciliation_amount_mismatch
    ON boleto_reconciliation (amount_mismatch) WHERE amount_mismatch;
