import base64
import json
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), "common"))

from boto_clients import client  # noqa: E402
from names import BOLETOS_TABLE, PDF_BUCKET  # noqa: E402

dynamodb = client("dynamodb")
kms = client("kms")
s3 = client("s3")


def _response(status: int, body: dict):
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body),
    }


def _mask(document: str) -> str:
    if len(document) <= 4:
        return "*" * len(document)
    return "*" * (len(document) - 4) + document[-4:]


def handler(event, _context):
    boleto_id = (event.get("pathParameters") or {}).get("id")
    if not boleto_id:
        return _response(400, {"error": "missing boleto id"})

    result = dynamodb.get_item(TableName=BOLETOS_TABLE, Key={"boleto_id": {"S": boleto_id}})
    item = result.get("Item")
    if not item:
        return _response(404, {"error": "boleto not found"})

    payer_masked = None
    ciphertext = item.get("payer_document_ciphertext", {}).get("S")
    if ciphertext:
        plaintext = kms.decrypt(CiphertextBlob=base64.b64decode(ciphertext))["Plaintext"]
        payer_masked = _mask(plaintext.decode("utf-8"))

    pdf_url = None
    pdf_key = item.get("pdf_key", {}).get("S")
    if pdf_key:
        pdf_url = s3.generate_presigned_url(
            "get_object", Params={"Bucket": PDF_BUCKET, "Key": pdf_key}, ExpiresIn=3600
        )

    body = {
        "boleto_id": boleto_id,
        "status": item.get("status", {}).get("S"),
        "amount_cents": int(item.get("amount_cents", {}).get("N", "0")),
        "due_date": item.get("due_date", {}).get("S"),
        "payer_name": item.get("payer_name", {}).get("S"),
        "payer_document_masked": payer_masked,
        "bank_registration_id": item.get("bank_registration_id", {}).get("S"),
        "pdf_url": pdf_url,
        "paid_at": item.get("paid_at", {}).get("S"),
        "paid_amount_cents": (
            int(item["paid_amount_cents"]["N"]) if "paid_amount_cents" in item else None
        ),
        "amount_mismatch": item.get("amount_mismatch", {}).get("BOOL", False),
        "failure_reason": item.get("failure_reason", {}).get("S"),
    }
    return _response(200, body)
