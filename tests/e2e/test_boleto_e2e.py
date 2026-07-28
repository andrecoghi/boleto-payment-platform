"""End-to-end tests against the live docker-compose stack.

These hit the same edge entry point (nginx, standing in for CloudFront +
API Gateway + WAF) a real client would, and verify the two claims the
architecture doc calls out as the ones that actually matter: the issuance
pipeline works, and payment confirmation is idempotent by design.
"""
import time
import uuid

import requests

from conftest import (
    BANK_SIMULATOR_URL,
    collect_messages_for,
    create_boleto,
    drain_queue,
    get_boleto,
    wait_for_status,
)


def test_full_issuance_flow_generates_and_stores_pdf(s3, shared_config):
    created = create_boleto()
    assert created["status"] == "PENDING"

    final = wait_for_status(created["boleto_id"], {"ISSUED", "FAILED"})
    assert final["status"] == "ISSUED", final
    assert final["bank_registration_id"]
    assert final["pdf_url"]
    assert final["payer_document_masked"].endswith("8901")

    head = s3.head_object(Bucket=shared_config["pdf_bucket"], Key=f"boletos/{created['boleto_id']}.pdf")
    assert head["ContentLength"] > 0

    obj = s3.get_object(Bucket=shared_config["pdf_bucket"], Key=f"boletos/{created['boleto_id']}.pdf")
    pdf_bytes = obj["Body"].read()
    assert pdf_bytes.startswith(b"%PDF-1.4")
    # Regression check: the PDF must show the payer's actual name, not their
    # masked CPF/CNPJ mislabeled as the name.
    assert b"Maria da Silva" in pdf_bytes


def test_get_unknown_boleto_returns_404():
    resp = requests.get(f"http://nginx:8080/boletos/{uuid.uuid4()}", timeout=10)
    assert resp.status_code == 404


def test_payment_webhook_marks_boleto_paid_and_fans_out_to_downstream(sqs, shared_config):
    created = create_boleto()
    wait_for_status(created["boleto_id"], {"ISSUED", "FAILED"})

    for queue_url in (
        shared_config["billing_queue_url"],
        shared_config["crm_queue_url"],
        shared_config["analytics_queue_url"],
    ):
        drain_queue(sqs, queue_url)

    trigger = requests.post(
        f"{BANK_SIMULATOR_URL}/trigger-payment/{created['boleto_id']}",
        json={"amount_cents": 15000},
        timeout=15,
    )
    assert trigger.status_code == 200
    assert trigger.json()["webhook_status_code"] == 202

    final = wait_for_status(created["boleto_id"], {"PAID"})
    assert final["status"] == "PAID"
    assert final["paid_at"]

    for queue_url in (
        shared_config["billing_queue_url"],
        shared_config["crm_queue_url"],
        shared_config["analytics_queue_url"],
    ):
        messages = collect_messages_for(sqs, queue_url, created["boleto_id"], timeout=15, grace=1)
        assert len(messages) == 1, f"expected exactly one fan-out message on {queue_url}, got {messages}"


def test_forged_payment_webhook_is_rejected():
    created = create_boleto()
    wait_for_status(created["boleto_id"], {"ISSUED", "FAILED"})

    tampered = requests.post(f"{BANK_SIMULATOR_URL}/trigger-payment-tampered/{created['boleto_id']}", timeout=15)
    assert tampered.status_code == 200
    assert tampered.json()["webhook_status_code"] == 401

    # A forged event must never flip the boleto to PAID.
    time.sleep(3)
    still = get_boleto(created["boleto_id"])
    assert still["status"] != "PAID"


def test_duplicate_payment_webhook_is_processed_exactly_once(sqs, shared_config):
    created = create_boleto()
    wait_for_status(created["boleto_id"], {"ISSUED", "FAILED"})
    drain_queue(sqs, shared_config["billing_queue_url"])

    event_id = f"evt-{uuid.uuid4()}"
    for _ in range(2):
        resp = requests.post(
            f"{BANK_SIMULATOR_URL}/trigger-payment/{created['boleto_id']}",
            json={"event_id": event_id, "amount_cents": 15000},
            timeout=15,
        )
        assert resp.json()["webhook_status_code"] == 202

    wait_for_status(created["boleto_id"], {"PAID"})

    messages = collect_messages_for(sqs, shared_config["billing_queue_url"], created["boleto_id"], timeout=15, grace=3)
    assert len(messages) == 1, (
        "the same bank event_id was delivered twice (simulating a bank webhook retry); "
        f"downstream billing should have seen exactly one notification, got {messages}"
    )


def test_bank_registration_is_idempotent_on_retry():
    idempotency_key = f"idem-{uuid.uuid4()}"
    body = {"boleto_id": "manual-test-boleto", "amount_cents": 5000, "due_date": "2026-09-01"}

    first = requests.post(
        f"{BANK_SIMULATOR_URL}/register", json=body, headers={"Idempotency-Key": idempotency_key}, timeout=10
    )
    second = requests.post(
        f"{BANK_SIMULATOR_URL}/register", json=body, headers={"Idempotency-Key": idempotency_key}, timeout=10
    )

    assert first.status_code == 201
    assert second.status_code == 200  # already existed, returned as-is instead of re-created
    assert first.json()["bank_registration_id"] == second.json()["bank_registration_id"]
    assert first.json()["registered_at"] == second.json()["registered_at"]


def test_reconciliation_store_reflects_paid_status_via_dynamodb_streams(pg_conn):
    created = create_boleto()
    wait_for_status(created["boleto_id"], {"ISSUED", "FAILED"})
    requests.post(f"{BANK_SIMULATOR_URL}/trigger-payment/{created['boleto_id']}", json={}, timeout=15)
    wait_for_status(created["boleto_id"], {"PAID"})

    deadline = time.time() + 30
    row = None
    while time.time() < deadline:
        cur = pg_conn.cursor()
        cur.execute(
            "SELECT status, amount_cents, paid_amount_cents, amount_mismatch "
            "FROM boleto_reconciliation WHERE boleto_id = %s",
            (created["boleto_id"],),
        )
        row = cur.fetchone()
        pg_conn.commit()
        if row and row[0] == "PAID":
            break
        time.sleep(2)

    assert row is not None, "boleto never showed up in the reconciliation store (DynamoDB Streams -> Postgres)"
    assert row[0] == "PAID"
    assert row[1] == 15000
    assert row[2] == 15000  # paid_amount_cents matches the issued amount on the happy path
    assert row[3] is False  # amount_mismatch


def test_payment_with_wrong_amount_is_flagged_as_mismatch_not_silently_accepted():
    created = create_boleto(amount_cents=15000)
    wait_for_status(created["boleto_id"], {"ISSUED", "FAILED"})

    # Bank confirms payment, but for less than the boleto was actually
    # issued for -- a signed, authentic, but financially wrong event. It
    # must still show up as PAID (the bank/CIP says money moved) *and* be
    # flagged so a reconciliation review can't miss it.
    resp = requests.post(
        f"{BANK_SIMULATOR_URL}/trigger-payment/{created['boleto_id']}",
        json={"amount_cents": 500},
        timeout=15,
    )
    assert resp.json()["webhook_status_code"] == 202

    final = wait_for_status(created["boleto_id"], {"PAID"})
    assert final["status"] == "PAID"
    assert final["paid_amount_cents"] == 500
    assert final["amount_mismatch"] is True


def test_create_boleto_rejects_invalid_input_with_clean_400():
    bad_payloads = [
        {"amount_cents": "not-a-number", "due_date": "2026-08-15", "payer_document": "12345678901", "payer_name": "X"},
        {"amount_cents": -100, "due_date": "2026-08-15", "payer_document": "12345678901", "payer_name": "X"},
        {"amount_cents": 1000, "due_date": "not-a-date", "payer_document": "12345678901", "payer_name": "X"},
        {"amount_cents": 1000, "due_date": "2026-08-15", "payer_document": "123", "payer_name": "X"},
        {"amount_cents": 1000, "due_date": "2026-08-15", "payer_document": "12345678901", "payer_name": ""},
    ]
    for payload in bad_payloads:
        resp = requests.post("http://nginx:8080/boletos", json=payload, timeout=10)
        assert resp.status_code == 400, f"expected 400 for {payload}, got {resp.status_code}: {resp.text}"
        assert "errors" in resp.json()
