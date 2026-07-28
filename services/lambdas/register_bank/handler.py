import json
import os
import urllib.error
import urllib.request

BANK_SIMULATOR_URL = os.environ.get("BANK_SIMULATOR_URL", "http://bank-simulator:8080")


def handler(event, _context):
    """Step Functions task 2: register the boleto with the bank / CIP.

    The idempotency key sent to the bank simulator is the boleto_id itself,
    so a Step Functions retry (bank API flakiness, timeout, ...) can never
    result in two CIP registrations for the same boleto -- the simulator
    returns the original registration id instead of creating a new one.
    """
    boleto_id = event["boleto_id"]
    body = json.dumps(
        {
            "boleto_id": boleto_id,
            "amount_cents": int(event["amount_cents"]),
            "due_date": event["due_date"],
        }
    ).encode("utf-8")

    request = urllib.request.Request(
        f"{BANK_SIMULATOR_URL}/register",
        data=body,
        method="POST",
        headers={"Content-Type": "application/json", "Idempotency-Key": boleto_id},
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as resp:
            registration = json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        # Surface non-2xx as a Lambda error so the Step Functions Retry/Catch
        # configuration on this state can act on bank/CIP flakiness.
        raise RuntimeError(f"bank registration failed: HTTP {exc.code} {exc.read()!r}") from exc

    event["bank_registration_id"] = registration["bank_registration_id"]
    return event
