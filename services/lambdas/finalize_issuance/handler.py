import os
import sys
from datetime import datetime, timezone

sys.path.append(os.path.join(os.path.dirname(__file__), "common"))

from boto_clients import client  # noqa: E402
from names import BOLETOS_TABLE  # noqa: E402

dynamodb = client("dynamodb")


def handler(event, _context):
    """Step Functions task 3: mark the boleto as ISSUED once PDF + bank registration exist."""
    boleto_id = event["boleto_id"]
    now = datetime.now(timezone.utc).isoformat()

    dynamodb.update_item(
        TableName=BOLETOS_TABLE,
        Key={"boleto_id": {"S": boleto_id}},
        UpdateExpression="SET #status = :status, pdf_key = :pdf_key, bank_registration_id = :bank_id, "
        "issued_at = :issued_at, updated_at = :updated_at",
        ExpressionAttributeNames={"#status": "status"},
        ExpressionAttributeValues={
            ":status": {"S": "ISSUED"},
            ":pdf_key": {"S": event["pdf_key"]},
            ":bank_id": {"S": event["bank_registration_id"]},
            ":issued_at": {"S": now},
            ":updated_at": {"S": now},
        },
    )

    return {"boleto_id": boleto_id, "status": "ISSUED"}
