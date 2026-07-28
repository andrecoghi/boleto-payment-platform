import os
import sys
from datetime import datetime, timezone

sys.path.append(os.path.join(os.path.dirname(__file__), "common"))

from boto_clients import client  # noqa: E402
from names import BOLETOS_TABLE  # noqa: E402

dynamodb = client("dynamodb")


def handler(event, _context):
    """Step Functions Catch handler: any failed issuance state lands here."""
    boleto_id = event["boleto_id"]
    error = event.get("error", {})
    reason = error.get("Cause") or error.get("Error") or "unknown error during issuance"
    now = datetime.now(timezone.utc).isoformat()

    dynamodb.update_item(
        TableName=BOLETOS_TABLE,
        Key={"boleto_id": {"S": boleto_id}},
        UpdateExpression="SET #status = :status, failure_reason = :reason, updated_at = :updated_at",
        ExpressionAttributeNames={"#status": "status"},
        ExpressionAttributeValues={
            ":status": {"S": "FAILED"},
            ":reason": {"S": str(reason)[:1000]},
            ":updated_at": {"S": now},
        },
    )

    return {"boleto_id": boleto_id, "status": "FAILED"}
