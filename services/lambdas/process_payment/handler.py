import json
import os
import sys
from datetime import datetime, timezone

from botocore.exceptions import ClientError

sys.path.append(os.path.join(os.path.dirname(__file__), "common"))

from boto_clients import client  # noqa: E402
from names import (  # noqa: E402
    BOLETOS_TABLE,
    EVENT_BUS,
    EVENT_SOURCE,
    PAYMENT_CONFIRMED_DETAIL_TYPE,
    PROCESSED_EVENTS_TABLE,
)

dynamodb = client("dynamodb")
events = client("events")


def handler(event, _context):
    """SQS FIFO trigger: the second, defense-in-depth layer of idempotency.

    SQS FIFO's MessageDeduplicationId already drops most duplicate
    deliveries at the queue level. This handler adds a conditional write to
    DynamoDB so that even a duplicate which slips past the queue (e.g. the
    5-minute FIFO dedup window expired on a very late bank retry) becomes a
    no-op instead of a double-processed payment / double notification.
    """
    for record in event.get("Records", []):
        _process_record(json.loads(record["body"]))
    return {"batchItemFailures": []}


def _process_record(payload: dict) -> None:
    boleto_id = payload["boleto_id"]
    event_id = payload["event_id"]
    amount_cents = payload["amount_cents"]
    paid_at = payload.get("paid_at") or datetime.now(timezone.utc).isoformat()
    processed_event_id = f"{boleto_id}#{event_id}"

    try:
        dynamodb.put_item(
            TableName=PROCESSED_EVENTS_TABLE,
            Item={
                "processed_event_id": {"S": processed_event_id},
                "boleto_id": {"S": boleto_id},
                "processed_at": {"S": datetime.now(timezone.utc).isoformat()},
            },
            ConditionExpression="attribute_not_exists(processed_event_id)",
        )
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
            print(f"duplicate payment event {processed_event_id}, skipping (idempotent no-op)")
            return
        raise

    # The webhook's amount is untrusted input even after signature
    # verification (a bug upstream, or a legitimate partial/interest-adjusted
    # payment, can both produce a value that doesn't match the boleto's
    # issued amount). We never silently swallow that: the boleto is still
    # marked PAID (the bank/CIP says money moved), but amount_mismatch is
    # surfaced everywhere downstream -- API response, EventBridge detail, and
    # the reconciliation store -- so it can't slip past a human reviewer.
    boleto = dynamodb.get_item(TableName=BOLETOS_TABLE, Key={"boleto_id": {"S": boleto_id}}).get("Item")
    expected_amount_cents = int(boleto["amount_cents"]["N"]) if boleto and "amount_cents" in boleto else None
    amount_mismatch = expected_amount_cents is not None and int(amount_cents) != expected_amount_cents

    dynamodb.update_item(
        TableName=BOLETOS_TABLE,
        Key={"boleto_id": {"S": boleto_id}},
        UpdateExpression=(
            "SET #status = :status, paid_at = :paid_at, updated_at = :updated_at, "
            "paid_amount_cents = :paid_amount_cents, amount_mismatch = :amount_mismatch"
        ),
        ExpressionAttributeNames={"#status": "status"},
        ExpressionAttributeValues={
            ":status": {"S": "PAID"},
            ":paid_at": {"S": paid_at},
            ":updated_at": {"S": datetime.now(timezone.utc).isoformat()},
            ":paid_amount_cents": {"N": str(int(amount_cents))},
            ":amount_mismatch": {"BOOL": amount_mismatch},
        },
    )

    if amount_mismatch:
        print(
            f"AMOUNT MISMATCH on {boleto_id}: expected {expected_amount_cents} cents, "
            f"bank reported {amount_cents} cents paid -- flagged for reconciliation review"
        )

    events.put_events(
        Entries=[
            {
                "Source": EVENT_SOURCE,
                "DetailType": PAYMENT_CONFIRMED_DETAIL_TYPE,
                "EventBusName": EVENT_BUS,
                "Detail": json.dumps(
                    {
                        "boleto_id": boleto_id,
                        "amount_cents": amount_cents,
                        "paid_at": paid_at,
                        "amount_mismatch": amount_mismatch,
                    }
                ),
            }
        ]
    )
