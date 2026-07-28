import base64
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), "common"))

from boto_clients import client  # noqa: E402
from minipdf import build_boleto_pdf  # noqa: E402
from names import BOLETOS_TABLE, PDF_BUCKET  # noqa: E402

dynamodb = client("dynamodb")
kms = client("kms")
s3 = client("s3")


def _mask(document: str) -> str:
    if len(document) <= 4:
        return "*" * len(document)
    return "*" * (len(document) - 4) + document[-4:]


def handler(event, _context):
    """Step Functions task 1: render the boleto PDF and store it in S3."""
    boleto_id = event["boleto_id"]
    amount_cents = int(event["amount_cents"])
    due_date = event["due_date"]

    item = dynamodb.get_item(TableName=BOLETOS_TABLE, Key={"boleto_id": {"S": boleto_id}})["Item"]
    payer_name = item["payer_name"]["S"]
    ciphertext = item["payer_document_ciphertext"]["S"]
    plaintext = kms.decrypt(CiphertextBlob=base64.b64decode(ciphertext))["Plaintext"].decode("utf-8")

    pdf_bytes = build_boleto_pdf(
        boleto_id=boleto_id,
        amount_cents=amount_cents,
        due_date=due_date,
        payer_name=payer_name,
        payer_document_masked=_mask(plaintext),
    )

    pdf_key = f"boletos/{boleto_id}.pdf"
    s3.put_object(
        Bucket=PDF_BUCKET,
        Key=pdf_key,
        Body=pdf_bytes,
        ContentType="application/pdf",
        ServerSideEncryption="aws:kms",
    )

    return {"boleto_id": boleto_id, "amount_cents": amount_cents, "due_date": due_date, "pdf_key": pdf_key}
