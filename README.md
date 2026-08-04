# Boleto Payment Platform — Local Reference Implementation

[![E2E idempotency suite](https://github.com/andrecoghi/boleto-payment-platform/actions/workflows/e2e.yml/badge.svg)](https://github.com/andrecoghi/boleto-payment-platform/actions/workflows/e2e.yml)

A fully local, runnable implementation of the architecture described in
[Building a Scalable, Cost-Aware Boleto Payment Architecture on AWS](https://www.linkedin.com/feed/update/urn:li:ugcPost:7486503636248883200/),
built with Docker Compose, [LocalStack](https://localstack.cloud) and a couple of open-source
substitutes for the AWS-managed pieces LocalStack's community edition doesn't emulate
(Aurora, RDS Proxy, CloudFront). Same architectural roles as the AWS design, same
idempotency guarantees, running entirely on your machine.

> **This is an educational reference implementation**, built to accompany the article
> above and demonstrate the architecture's ideas end to end. It is not affiliated with,
> endorsed by, or produced by Amazon Web Services, LocalStack, or any bank/CIP. It has not
> had a security review and is not production-ready — see "Divergences from production"
> below for what's deliberately simplified or left out. Don't point it at real payments or
> real personal data. Licensed under MIT (see [`LICENSE`](LICENSE)); provided as-is, with
> no warranty, per that license's terms.

## Architecture mapping

| Architecture doc (AWS)              | This repo (local)                                   | Notes |
|--------------------------------------|------------------------------------------------------|-------|
| CloudFront + API Gateway + WAF       | `nginx` (edge, port 8090) + LocalStack API Gateway    | nginx does basic rate-limiting (WAF stand-in) and GET-only caching; API Gateway REST API runs for real in LocalStack |
| Lambda                               | LocalStack Lambda (Python 3.12, real Docker-executed functions) | `services/lambdas/*` |
| Step Functions (Standard)            | LocalStack Step Functions                             | `infra/stepfunctions/boleto_issuance.asl.json` |
| DynamoDB (+ Streams)                 | LocalStack DynamoDB                                   | `Boletos`, `ProcessedPaymentEvents` tables |
| SQS FIFO                             | LocalStack SQS FIFO                                   | `payment-events.fifo` + DLQ |
| EventBridge + SNS                    | LocalStack EventBridge + SNS                          | fans out to 3 SQS queues simulating billing/CRM/analytics consumers |
| S3 (KMS-encrypted, lifecycle to Glacier) | LocalStack S3 + LocalStack KMS                    | `boleto-pdfs` bucket |
| Aurora Serverless v2                 | **Postgres** (`postgres:16-alpine`)                   | LocalStack community doesn't emulate RDS/Aurora; real Postgres is the honest open-source substitute |
| RDS Proxy                            | **PgBouncer**                                         | connection pooling in front of Postgres |
| Secrets Manager / KMS                | LocalStack Secrets Manager / KMS                      | webhook HMAC secret + a field-encryption key for the payer's CPF/CNPJ |
| CloudWatch Logs                      | `docker compose logs` on each Lambda's container / LocalStack | works for real |
| X-Ray                                | **Not implemented** — LocalStack community doesn't emulate it | see "Divergences from production" below |
| DAX                                  | **Not implemented, on purpose** — the article itself argues against adding it here | |

## What's actually running

```
client
  │
  ▼
nginx (8090)  ───────────────►  LocalStack API Gateway  ───────────────►  Lambda
 (CloudFront+WAF stand-in)        (boleto-api, stage "local")            (business logic)
                                                                             │
                     ┌────────────────────────────────────────────────────┼───────────────────────┐
                     ▼                                                    ▼                        ▼
             Step Functions                                          DynamoDB                     S3
        (generate → register → finalize)                       (Boletos, ProcessedEvents)   (boleto-pdfs)
                     │                                                    │
                     ▼                                                    ▼ (Streams)
             bank-simulator (Flask)                          stream_to_reconciliation Lambda
        (mock bank/CIP, signs webhooks)                                  │
                     │                                                    ▼
                     ▼                                          PgBouncer → Postgres
         POST /webhooks/payment (HMAC-signed)                     (reconciliation store)
                     │
                     ▼
          webhook_receiver Lambda → SQS FIFO → process_payment Lambda → EventBridge → SNS
                                                                              │
                                                            ┌─────────────────┼─────────────────┐
                                                            ▼                 ▼                 ▼
                                                     billing queue      crm queue        analytics queue
```

## Prerequisites

- Docker + Docker Compose v2 (`docker compose version` ≥ 2.20)
- ~4 GB of free RAM for the containers (LocalStack + Postgres + Lambda containers)
- Internet access on first run (pulling images, `pip install`-ing Lambda dependencies, and
  LocalStack pulling the `public.ecr.aws/lambda/python:3.12` runtime image the first time a
  Lambda is invoked)
- Host ports **4566** (LocalStack), **8090** (edge/nginx) and **8091** (bank simulator)
  free. These were deliberately picked off the beaten path (not 8080/8081) since those are
  common defaults other local services grab first; change the `ports:` mappings in
  `docker-compose.yml` if you still collide with something.

## Running it

```bash
make up
```

This builds every custom image, starts LocalStack/Postgres/PgBouncer, then runs the
**bootstrap** container, which provisions every AWS resource (tables, queues, topics,
bucket, KMS key, secret, Lambdas, the Step Functions state machine, and the API Gateway
REST API) via `infra/bootstrap.py` — the local equivalent of a Terraform apply. `make up`
blocks until bootstrap finishes, then nginx and the bank simulator (which both wait on
bootstrap's output) start.

Once it returns, the platform is reachable at **http://localhost:8090**.

Check everything is healthy:

```bash
make ps
```

## Trying it by hand

Create a boleto:

```bash
curl -s -X POST http://localhost:8090/boletos \
  -H 'Content-Type: application/json' \
  -d '{"amount_cents": 15000, "due_date": "2026-08-15", "payer_document": "12345678901", "payer_name": "Maria da Silva"}'
# => {"boleto_id": "...", "status": "PENDING"}
```

Poll its status (issuance runs asynchronously through Step Functions — generate PDF →
register with the bank/CIP simulator → finalize):

```bash
curl -s http://localhost:8090/boletos/<boleto_id> | python3 -m json.tool
```

Once `status` is `ISSUED`, simulate the bank telling us it got paid (this hits the bank
simulator, which signs the payload with the shared HMAC secret and POSTs it to
`/webhooks/payment`, exactly like a real bank/CIP callback would):

```bash
curl -s -X POST http://localhost:8091/trigger-payment/<boleto_id> | python3 -m json.tool
```

Poll status again — it should flip to `PAID` within a couple of seconds (the webhook goes
through SQS FIFO and an async Lambda, same as it would in production). Try tampering with
the signature to see it get rejected:

```bash
curl -s -X POST http://localhost:8091/trigger-payment-tampered/<boleto_id> | python3 -m json.tool
# => webhook_status_code: 401
```

A validly signed webhook that reports the *wrong amount* is a different failure mode —
not a forgery, just data that doesn't reconcile (a bug upstream, or a legitimately
discounted/interest-adjusted payment). `process_payment` still marks the boleto `PAID`
(the bank/CIP says money moved) but sets `amount_mismatch: true` and records the actual
`paid_amount_cents`, both in the `GET /boletos/{id}` response and in the reconciliation
store, instead of silently treating a mismatched amount as a clean payment:

```bash
curl -s -X POST http://localhost:8091/trigger-payment/<boleto_id> \
  -H 'Content-Type: application/json' -d '{"amount_cents": 500}'
curl -s http://localhost:8090/boletos/<boleto_id> | python3 -m json.tool
# => "paid_amount_cents": 500, "amount_mismatch": true
```

The generated PDF lives in S3. `pdf_url` in the status response is a presigned URL, but
it's only resolvable from *inside* the docker network (see "Known local-only quirks"
below) — from your host machine, pull it out through the `localstack` container instead,
which already ships the `awslocal` CLI (no `pip install` needed — installing Python
packages system-wide fails outright on modern Debian/Ubuntu/Fedora with an
"externally-managed-environment" error unless you fight it with a venv or
`--break-system-packages`, so don't bother):

```bash
docker compose exec localstack awslocal s3 cp s3://boleto-pdfs/boletos/<boleto_id>.pdf /tmp/boleto.pdf
docker compose cp localstack:/tmp/boleto.pdf ./boleto.pdf
```

## Running the E2E tests

```bash
make test
```

This starts the stack (if not already up) and runs `tests/e2e` in its own container,
against the real nginx/API Gateway/Lambda/SQS/EventBridge/Postgres pipeline. The suite
covers exactly the claims the architecture doc makes:

- `test_full_issuance_flow_generates_and_stores_pdf` — Step Functions issuance pipeline
  runs end to end and a real PDF lands in S3.
- `test_payment_webhook_marks_boleto_paid_and_fans_out_to_downstream` — a validly signed
  payment webhook marks the boleto `PAID` and fans out to all three downstream queues
  exactly once.
- `test_forged_payment_webhook_is_rejected` — an incorrectly signed webhook is rejected
  with 401 and never changes boleto state.
- `test_duplicate_payment_webhook_is_processed_exactly_once` — the same bank event
  delivered twice (simulating a bank webhook retry) still results in exactly one
  downstream notification.
- `test_bank_registration_is_idempotent_on_retry` — retrying a bank registration with the
  same idempotency key never creates a second CIP registration.
- `test_reconciliation_store_reflects_paid_status_via_dynamodb_streams` — DynamoDB
  Streams → Lambda → Postgres actually lands the paid status in the reconciliation store.
- `test_payment_with_wrong_amount_is_flagged_as_mismatch_not_silently_accepted` — a
  validly signed webhook reporting the wrong amount is still marked `PAID` but flagged
  with `amount_mismatch: true` instead of being silently treated as a clean payment.
- `test_create_boleto_rejects_invalid_input_with_clean_400` — malformed input (bad
  amount type, negative amount, invalid date, invalid CPF/CNPJ, empty name) gets a clean
  `400` with an `errors` list, not an unhandled-exception `502`.

Run them again anytime with `docker compose run --rm e2e` once the stack is up.

This exact suite also runs in CI — see [`.github/workflows/e2e.yml`](.github/workflows/e2e.yml) —
on every push/PR that touches `services/`, `infra/`, `nginx/`, `reconciliation-db/`,
`tests/`, `docker-compose.yml` or `Makefile`. It's the same `make test` command, just
invoked by a GitHub-hosted runner instead of a human; no AWS credentials or LocalStack
auth token are needed, for the same version-pin reason described below.

## Cleaning up

```bash
make down    # stop containers, keep data volumes
make clean   # stop containers and delete all volumes (LocalStack state, Postgres data)
```

## Divergences from production (read this before trusting the demo too much)

- **CloudFront** isn't emulated (LocalStack Pro-only); `nginx` stands in for its
  edge-caching/custom-domain role, and for WAF's rate-limiting role. It does *not*
  replicate real CDN behavior (multi-region edge, TLS termination, etc).
- **X-Ray** tracing isn't implemented — LocalStack's community tier doesn't emulate it.
  CloudWatch Logs does work for real, though: `docker compose logs <service>` per Lambda.
- **DAX is intentionally absent**, per the architecture doc's own reasoning: DynamoDB's
  native latency was judged fast enough, so adding a cache layer here would just be
  speculative cost.
- **Provisioned Concurrency** isn't modeled — there's no meaningful "cold start" concept
  worth optimizing for in a local LocalStack Lambda container.
- **IAM is intentionally permissive** (`infra/bootstrap.py`'s `ensure_lambda_role`) for
  local-dev convenience. Production would scope a distinct role per Lambda, as the
  article itself calls for.
- **Presigned S3 URLs use the internal `localstack` hostname**, since that's the only
  address that resolves from *inside* the LocalStack Lambda containers that generate
  them. They work fine between containers on the `boleto-net` docker network (which is
  what the E2E suite uses); to fetch a PDF from your host machine, use the AWS CLI
  against `http://localhost:4566` instead (see above).
- **Aurora Serverless v2 and RDS Proxy** are represented by plain Postgres + PgBouncer —
  functionally equivalent for this demo, but without Aurora's storage-layer replication
  or PgBouncer's Aurora-Proxy-specific IAM auth integration.
- **No authentication/authorization on any API endpoint.** `POST /boletos`,
  `GET /boletos/{id}` and `/webhooks/payment` (protected only by the HMAC signature) are
  wide open on purpose, to keep the demo curl-able without a token dance. A real deployment
  needs an authorizer (Cognito/IAM/API key) in front of the customer-facing routes — this
  repo does not model that at all, don't treat it as a template for that part.
- **No CORS headers.** Fine for curl/pytest; if you point a browser-based frontend at this,
  you'll need to add `OPTIONS` methods and `Access-Control-Allow-*` headers yourself.
- **The bank/CIP simulator's state is in-memory** (`services/bank_simulator/app.py`).
  Restarting just that one container forgets every registration it has made, even though
  the boletos still exist (PAID/ISSUED) in DynamoDB — restart the whole stack (`make down`
  then `make up`) rather than `docker compose restart bank-simulator` if you need a clean
  slate, or you can end up with an inconsistent demo state.
- **The webhook HMAC secret is cached in-memory per Lambda/simulator container** with no
  TTL or rotation-awareness. A warm container keeps using whatever secret it first fetched
  from Secrets Manager, even if the secret is later rotated. Common real-world tradeoff,
  just flagging it since nothing here refreshes it.
- **No alerting on stuck/failed Step Functions executions.** If `MarkFailed` itself throws,
  or an execution gets stuck, the boleto just sits at its last status with nothing paging
  anyone — this repo has no CloudWatch Alarm equivalent wired up. In production you'd
  monitor `ExecutionsFailed`/`ExecutionsTimedOut` on the state machine.

## Repository layout

```
docker-compose.yml            # wires every service together
infra/
  bootstrap.py                 # provisions all LocalStack resources + deploys Lambdas
  stepfunctions/                # Amazon States Language definition for issuance
services/
  lambdas/                      # one folder per Lambda (handler.py [+ requirements.txt])
    common/                      # shared boto3 client factory, resource names, PDF writer
  bank_simulator/               # mock bank/CIP: idempotent registration + signed webhooks
nginx/                          # edge entrypoint: rate limiting + GET caching
reconciliation-db/init.sql      # Postgres schema for the reconciliation store
tests/e2e/                      # pytest suite exercising the live stack
```

## License & third-party terms

This repository's own code (Lambdas, `infra/bootstrap.py`, `nginx/`,
`services/bank_simulator/`, `tests/e2e/`) is licensed under the [MIT License](LICENSE).
Use it, fork it, adapt it for your own article or project.

Everything it *runs on top of* keeps its own license — none of it is copyleft, so nothing
here imposes obligations on your own code, but know what you're pulling in:

| Dependency | License |
|---|---|
| nginx | BSD-2-Clause |
| PostgreSQL | PostgreSQL License (permissive) |
| PgBouncer | ISC |
| Python, Flask, boto3, requests, pg8000, pytest | PSF / BSD / Apache-2.0 / MIT (all permissive) |
| Docker Engine + `docker compose` CLI | Apache-2.0 |

**LocalStack needs a specific callout.** As of the `2026.03.0` release (March 2026),
LocalStack requires a registered account and auth token to start, and its free ("Hobby")
tier is limited to non-commercial use — the old no-registration, Apache-2.0 "Community
Edition" this project was originally built against no longer exists in that form. This
repo pins `localstack/localstack:3.8.1` (from before that change) in
[`docker-compose.yml`](docker-compose.yml) specifically so it keeps running without an
auth token. If you bump that tag to `latest` or a newer version, you'll likely need to
create a LocalStack account and set `LOCALSTACK_AUTH_TOKEN`, and confirm your use still
qualifies for their free tier. Check [LocalStack's current licensing terms](https://docs.localstack.cloud/aws/licensing/)
before changing that pin, especially for anything beyond personal/educational use.

If you're on macOS or Windows and use Docker Desktop (not needed here on Linux — this
was built and tested with the native Docker Engine): Docker Desktop requires a paid
subscription for organizations with 250+ employees or $10M+ annual revenue. Doesn't apply
to `docker`/`docker compose` on Linux servers, only to the Desktop app.
