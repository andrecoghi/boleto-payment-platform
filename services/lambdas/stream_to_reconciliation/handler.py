import os
from decimal import Decimal

import pg8000.dbapi as pg8000


def _connect():
    return pg8000.connect(
        host=os.environ.get("PGHOST", "pgbouncer"),
        port=int(os.environ.get("PGPORT", "6432")),
        database=os.environ.get("PGDATABASE", "reconciliation"),
        user=os.environ.get("PGUSER", "reconciliation"),
        password=os.environ.get("PGPASSWORD", "reconciliation"),
        timeout=10,
    )


def _to_python(attr: dict):
    if attr is None:
        return None
    if "S" in attr:
        return attr["S"]
    if "N" in attr:
        return Decimal(attr["N"])
    if "BOOL" in attr:
        return attr["BOOL"]
    return None


def handler(event, _context):
    """DynamoDB Streams trigger: mirrors Boletos into the SQL reconciliation
    store (Aurora Serverless v2 in production, Postgres locally), the same
    "operational store feeds the analytical/reporting store via CDC" pattern
    from the architecture doc.
    """
    records = event.get("Records", [])
    if not records:
        return {"batchItemFailures": []}

    conn = _connect()
    try:
        cur = conn.cursor()
        for record in records:
            if record["eventName"] == "REMOVE":
                continue
            image = record["dynamodb"]["NewImage"]
            cur.execute(
                """
                INSERT INTO boleto_reconciliation
                    (boleto_id, status, amount_cents, due_date, bank_registration_id, paid_at,
                     paid_amount_cents, amount_mismatch, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (boleto_id) DO UPDATE SET
                    status = EXCLUDED.status,
                    amount_cents = EXCLUDED.amount_cents,
                    due_date = EXCLUDED.due_date,
                    bank_registration_id = EXCLUDED.bank_registration_id,
                    paid_at = EXCLUDED.paid_at,
                    paid_amount_cents = EXCLUDED.paid_amount_cents,
                    amount_mismatch = EXCLUDED.amount_mismatch,
                    updated_at = EXCLUDED.updated_at
                """,
                (
                    _to_python(image.get("boleto_id")),
                    _to_python(image.get("status")),
                    _to_python(image.get("amount_cents")),
                    _to_python(image.get("due_date")),
                    _to_python(image.get("bank_registration_id")),
                    _to_python(image.get("paid_at")),
                    _to_python(image.get("paid_amount_cents")),
                    _to_python(image.get("amount_mismatch")) or False,
                    _to_python(image.get("updated_at")),
                ),
            )
        conn.commit()
    finally:
        conn.close()

    return {"batchItemFailures": []}
