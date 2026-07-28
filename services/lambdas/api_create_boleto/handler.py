import base64
import json
import os
import re
import sys
import uuid
from datetime import date, datetime, timezone

sys.path.append("/opt/python")
sys.path.append(os.path.join(os.path.dirname(__file__), "common"))

from boto_clients import client  # noqa: E402
from names import BOLETOS_TABLE, KMS_ALIAS, STATE_MACHINE_NAME  # noqa: E402

dynamodb = client("dynamodb")
kms = client("kms")
sfn = client("stepfunctions")


def _response(status: int, body: dict):
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body),
    }


_CPF_CNPJ_RE = re.compile(r"^\d{11}$|^\d{14}$")


def _validate(payload: dict) -> list[str]:
    """Basic shape/range validation. Not a full CPF/CNPJ check-digit
    validator -- a real implementation would verify those, this only checks
    the digit count -- but it's enough to turn "garbage in" into a clean 400
    instead of an unhandled DynamoDB ValidationException (500/502)."""
    errors = []

    amount_cents = payload.get("amount_cents")
    if not isinstance(amount_cents, int) or isinstance(amount_cents, bool) or amount_cents <= 0:
        errors.append("amount_cents must be a positive integer (value in cents)")

    due_date = payload.get("due_date")
    if not isinstance(due_date, str):
        errors.append("due_date must be a string in YYYY-MM-DD format")
    else:
        try:
            date.fromisoformat(due_date)
        except ValueError:
            errors.append("due_date must be a valid YYYY-MM-DD date")

    payer_document = payload.get("payer_document")
    if not isinstance(payer_document, str) or not _CPF_CNPJ_RE.match(payer_document):
        errors.append("payer_document must be an 11-digit CPF or 14-digit CNPJ (digits only)")

    payer_name = payload.get("payer_name")
    if not isinstance(payer_name, str) or not payer_name.strip():
        errors.append("payer_name must be a non-empty string")

    return errors


def handler(event, _context):
    try:
        payload = json.loads(event.get("body") or "{}")
    except json.JSONDecodeError:
        return _response(400, {"error": "invalid JSON body"})

    if not isinstance(payload, dict):
        return _response(400, {"error": "request body must be a JSON object"})

    errors = _validate(payload)
    if errors:
        return _response(400, {"errors": errors})

    amount_cents = payload["amount_cents"]
    due_date = payload["due_date"]
    payer_document = payload["payer_document"]
    payer_name = payload["payer_name"]

    boleto_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()

    # Encrypt the payer's CPF/CNPJ at the field level with KMS before it ever
    # touches DynamoDB -- this is the "sensitive fields in DynamoDB" control
    # from the architecture doc, not just table-level SSE.
    encrypted = kms.encrypt(KeyId=KMS_ALIAS, Plaintext=payer_document.encode("utf-8"))
    payer_document_ciphertext = base64.b64encode(encrypted["CiphertextBlob"]).decode("ascii")

    dynamodb.put_item(
        TableName=BOLETOS_TABLE,
        Item={
            "boleto_id": {"S": boleto_id},
            "status": {"S": "PENDING"},
            "amount_cents": {"N": str(amount_cents)},
            "due_date": {"S": due_date},
            "payer_name": {"S": payer_name},
            "payer_document_ciphertext": {"S": payer_document_ciphertext},
            "created_at": {"S": now},
            "updated_at": {"S": now},
        },
    )

    sfn.start_execution(
        stateMachineArn=_state_machine_arn(),
        name=f"issue-{boleto_id}",
        input=json.dumps({"boleto_id": boleto_id, "amount_cents": amount_cents, "due_date": due_date}),
    )

    return _response(202, {"boleto_id": boleto_id, "status": "PENDING"})


def _state_machine_arn() -> str:
    region = os.environ.get("AWS_REGION", "us-east-1")
    account_id = os.environ.get("AWS_ACCOUNT_ID", "000000000000")
    return f"arn:aws:states:{region}:{account_id}:stateMachine:{STATE_MACHINE_NAME}"
