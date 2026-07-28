import json
import os
import time

import boto3
import pg8000.dbapi as pg8000
import pytest
import requests
from botocore.config import Config

EDGE_BASE_URL = os.environ.get("EDGE_BASE_URL", "http://nginx:8080")
BANK_SIMULATOR_URL = os.environ.get("BANK_SIMULATOR_URL", "http://bank-simulator:8080")
AWS_ENDPOINT_URL = os.environ.get("AWS_ENDPOINT_URL", "http://localstack:4566")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
SHARED_CONFIG_PATH = os.environ.get("SHARED_CONFIG_PATH", "/shared/endpoints.json")


def _aws_client(service: str):
    config = Config(s3={"addressing_style": "path"}) if service == "s3" else None
    return boto3.client(
        service,
        region_name=AWS_REGION,
        endpoint_url=AWS_ENDPOINT_URL,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        config=config,
    )


@pytest.fixture(scope="session")
def shared_config() -> dict:
    with open(SHARED_CONFIG_PATH) as fh:
        return json.load(fh)


@pytest.fixture(scope="session")
def sqs():
    return _aws_client("sqs")


@pytest.fixture(scope="session")
def s3():
    return _aws_client("s3")


@pytest.fixture(scope="session")
def dynamodb():
    return _aws_client("dynamodb")


@pytest.fixture(scope="session", autouse=True)
def wait_for_edge():
    """Make sure nginx + API Gateway are actually answering before any test runs."""
    deadline = time.time() + 60
    last_error = None
    while time.time() < deadline:
        try:
            resp = requests.get(f"{EDGE_BASE_URL}/healthz", timeout=3)
            if resp.status_code == 200:
                return
        except requests.RequestException as exc:
            last_error = exc
        time.sleep(2)
    raise RuntimeError(f"edge (nginx) never became reachable at {EDGE_BASE_URL}: {last_error}")


def create_boleto(**overrides) -> dict:
    payload = {
        "amount_cents": 15000,
        "due_date": "2026-08-15",
        "payer_document": "12345678901",
        "payer_name": "Maria da Silva",
    }
    payload.update(overrides)
    resp = requests.post(f"{EDGE_BASE_URL}/boletos", json=payload, timeout=10)
    resp.raise_for_status()
    return resp.json()


def get_boleto(boleto_id: str) -> dict:
    resp = requests.get(f"{EDGE_BASE_URL}/boletos/{boleto_id}", timeout=10)
    resp.raise_for_status()
    return resp.json()


def wait_for_status(boleto_id: str, expected_statuses, timeout: float = 40.0) -> dict:
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = get_boleto(boleto_id)
        if last["status"] in expected_statuses:
            return last
        time.sleep(1.5)
    raise AssertionError(f"boleto {boleto_id} never reached {expected_statuses}, last seen: {last}")


def drain_queue(sqs_client, queue_url: str, max_rounds: int = 3) -> None:
    empty_rounds = 0
    while empty_rounds < max_rounds:
        resp = sqs_client.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=1)
        messages = resp.get("Messages", [])
        if not messages:
            empty_rounds += 1
            continue
        empty_rounds = 0
        for msg in messages:
            sqs_client.delete_message(QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"])


def collect_messages_for(
    sqs_client, queue_url: str, boleto_id: str, timeout: float = 20.0, grace: float = 3.0
) -> list[dict]:
    """Poll a queue until at least one message for boleto_id shows up, then
    keep polling for `grace` more seconds to catch any accidental duplicate
    before giving up (used by the idempotency tests)."""
    found = []
    deadline = time.time() + timeout
    first_match_at = None
    while time.time() < deadline:
        resp = sqs_client.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=2)
        for msg in resp.get("Messages", []):
            body = json.loads(msg["Body"])
            if body.get("boleto_id") == boleto_id:
                found.append(body)
                first_match_at = first_match_at or time.time()
            sqs_client.delete_message(QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"])
        if first_match_at and time.time() - first_match_at > grace:
            break
    return found


@pytest.fixture()
def pg_conn():
    conn = pg8000.connect(
        host=os.environ.get("PGHOST", "pgbouncer"),
        port=int(os.environ.get("PGPORT", "6432")),
        database=os.environ.get("PGDATABASE", "reconciliation"),
        user=os.environ.get("PGUSER", "reconciliation"),
        password=os.environ.get("PGPASSWORD", "reconciliation"),
    )
    yield conn
    conn.close()
