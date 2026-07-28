"""Stand-in for the bank / CIP integration.

In production this would be the actual bank API (which in turn registers
centrally with CIP, Brazil's interbank clearing house). Locally it's a tiny
Flask app that:

  * Accepts boleto registrations, deduplicated by an Idempotency-Key header
    -- exactly the discipline the architecture doc calls for on the
    issuance side, so a Step Functions retry never double-registers a
    boleto with the CIP.
  * Lets tests (or a human) trigger a signed payment-confirmation webhook
    call back into the platform, the same way a real bank would notify us
    that a boleto was paid.
"""
import hashlib
import hmac
import json
import os
import threading
import uuid
from datetime import datetime, timezone

import boto3
import requests
from flask import Flask, jsonify, request

app = Flask(__name__)
_lock = threading.Lock()
_registrations: dict[str, dict] = {}  # idempotency_key -> registration record

AWS_ENDPOINT_URL = os.environ.get("AWS_ENDPOINT_URL", "http://localstack:4566")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
WEBHOOK_SECRET_NAME = os.environ.get("WEBHOOK_SECRET_NAME", "bank/webhook/hmac-secret")
WEBHOOK_URL_ENV = os.environ.get("WEBHOOK_URL")  # optional override, else read from shared config
SHARED_CONFIG_PATH = os.environ.get("SHARED_CONFIG_PATH", "/shared/endpoints.json")

_secretsmanager = boto3.client("secretsmanager", region_name=AWS_REGION, endpoint_url=AWS_ENDPOINT_URL)
_secret_cache = None


def _hmac_secret() -> str:
    global _secret_cache
    if _secret_cache is None:
        value = _secretsmanager.get_secret_value(SecretId=WEBHOOK_SECRET_NAME)
        _secret_cache = json.loads(value["SecretString"])["hmac_secret"]
    return _secret_cache


def _webhook_url() -> str:
    if WEBHOOK_URL_ENV:
        return WEBHOOK_URL_ENV
    with open(SHARED_CONFIG_PATH) as fh:
        return json.load(fh)["webhook_url"]


@app.get("/health")
def health():
    return jsonify({"status": "ok"})


@app.post("/register")
def register():
    idempotency_key = request.headers.get("Idempotency-Key")
    if not idempotency_key:
        return jsonify({"error": "Idempotency-Key header is required"}), 400

    payload = request.get_json(force=True)

    with _lock:
        existing = _registrations.get(idempotency_key)
        if existing:
            return jsonify(existing), 200

        record = {
            "bank_registration_id": str(uuid.uuid4()),
            "cip_status": "REGISTERED",
            "boleto_id": payload.get("boleto_id"),
            "amount_cents": payload.get("amount_cents"),
            "due_date": payload.get("due_date"),
            "registered_at": datetime.now(timezone.utc).isoformat(),
        }
        _registrations[idempotency_key] = record

    return jsonify(record), 201


@app.get("/registrations/<idempotency_key>")
def get_registration(idempotency_key):
    record = _registrations.get(idempotency_key)
    if not record:
        return jsonify({"error": "not found"}), 404
    return jsonify(record)


@app.get("/registrations")
def list_registrations():
    return jsonify(list(_registrations.values()))


@app.post("/trigger-payment/<boleto_id>")
def trigger_payment(boleto_id):
    """Simulate the bank/CIP telling us a boleto was paid.

    Optional JSON body: {"event_id": "...", "amount_cents": 12345}. Reusing
    the same event_id across two calls simulates the bank retrying webhook
    delivery, which is expected/normal on their end -- the platform is
    responsible for treating it as a no-op the second time.
    """
    body = request.get_json(silent=True) or {}
    event_id = body.get("event_id") or str(uuid.uuid4())
    # 15000 matches the demo/test/README default boleto amount so the happy
    # path doesn't trip the amount_mismatch flag by accident. Pass an
    # explicit amount_cents to deliberately exercise that check.
    amount_cents = body.get("amount_cents", 15000)

    payload = {
        "boleto_id": boleto_id,
        "event_id": event_id,
        "amount_cents": amount_cents,
        "paid_at": datetime.now(timezone.utc).isoformat(),
    }
    raw_body = json.dumps(payload)
    signature = hmac.new(_hmac_secret().encode("utf-8"), raw_body.encode("utf-8"), hashlib.sha256).hexdigest()

    resp = requests.post(
        _webhook_url(),
        data=raw_body,
        headers={"Content-Type": "application/json", "X-Signature": signature},
        timeout=10,
    )
    return jsonify(
        {
            "sent_payload": payload,
            "webhook_status_code": resp.status_code,
            "webhook_response": _safe_json(resp),
        }
    ), 200


@app.post("/trigger-payment-tampered/<boleto_id>")
def trigger_payment_tampered(boleto_id):
    """Same as /trigger-payment but with a deliberately wrong signature --
    used by the E2E suite to prove forged 'paid' events are rejected."""
    payload = {
        "boleto_id": boleto_id,
        "event_id": str(uuid.uuid4()),
        "amount_cents": 10000,
        "paid_at": datetime.now(timezone.utc).isoformat(),
    }
    raw_body = json.dumps(payload)
    bogus_signature = hashlib.sha256(b"not-the-real-secret").hexdigest()

    resp = requests.post(
        _webhook_url(),
        data=raw_body,
        headers={"Content-Type": "application/json", "X-Signature": bogus_signature},
        timeout=10,
    )
    return jsonify({"webhook_status_code": resp.status_code, "webhook_response": _safe_json(resp)}), 200


def _safe_json(resp):
    try:
        return resp.json()
    except ValueError:
        return resp.text


if __name__ == "__main__":
    # threaded=True: Flask's dev server is single-request-at-a-time by
    # default, which would silently serialize (and eventually time out)
    # concurrent boleto registrations during a due-date spike -- exactly the
    # burst scenario the architecture doc is designed around.
    app.run(host="0.0.0.0", port=8080, threaded=True)
