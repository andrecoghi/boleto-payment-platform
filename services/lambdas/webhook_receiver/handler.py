import hashlib
import hmac
import json
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), "common"))

from boto_clients import client  # noqa: E402
from names import PAYMENT_EVENTS_QUEUE, WEBHOOK_SECRET_NAME  # noqa: E402

secretsmanager = client("secretsmanager")
sqs = client("sqs")

_secret_cache = None
_queue_url_cache = None


def _get_secret() -> str:
    global _secret_cache
    if _secret_cache is None:
        value = secretsmanager.get_secret_value(SecretId=WEBHOOK_SECRET_NAME)
        _secret_cache = json.loads(value["SecretString"])["hmac_secret"]
    return _secret_cache


def _queue_url() -> str:
    global _queue_url_cache
    if _queue_url_cache is None:
        _queue_url_cache = sqs.get_queue_url(QueueName=PAYMENT_EVENTS_QUEUE)["QueueUrl"]
    return _queue_url_cache


def _response(status: int, body: dict):
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body),
    }


def handler(event, _context):
    raw_body = event.get("body") or ""
    headers = {k.lower(): v for k, v in (event.get("headers") or {}).items()}
    signature = headers.get("x-signature", "")

    secret = _get_secret()
    expected = hmac.new(secret.encode("utf-8"), raw_body.encode("utf-8"), hashlib.sha256).hexdigest()

    # Only a signature computed with the shared secret proves this came from
    # the bank/CIP -- anyone can POST a JSON body claiming a boleto was paid.
    if not hmac.compare_digest(expected, signature):
        return _response(401, {"error": "invalid webhook signature"})

    try:
        payload = json.loads(raw_body)
    except json.JSONDecodeError:
        return _response(400, {"error": "invalid JSON body"})

    boleto_id = payload.get("boleto_id")
    event_id = payload.get("event_id")
    if not boleto_id or not event_id:
        return _response(400, {"error": "boleto_id and event_id are required"})

    dedup_id = hashlib.sha256(f"{boleto_id}#{event_id}".encode("utf-8")).hexdigest()

    sqs.send_message(
        QueueUrl=_queue_url(),
        MessageBody=raw_body,
        MessageGroupId=boleto_id,
        MessageDeduplicationId=dedup_id,
    )

    return _response(202, {"received": True})
