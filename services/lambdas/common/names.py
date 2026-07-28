"""Central registry of resource names shared between bootstrap.py and every Lambda.

Keeping these in one module means the provisioning script and the runtime
code can never drift apart on a table/queue/bucket name.
"""

BOLETOS_TABLE = "Boletos"
PROCESSED_EVENTS_TABLE = "ProcessedPaymentEvents"

PDF_BUCKET = "boleto-pdfs"

PAYMENT_EVENTS_QUEUE = "payment-events.fifo"
PAYMENT_EVENTS_DLQ = "payment-events-dlq.fifo"

NOTIFICATIONS_TOPIC = "boleto-notifications"
BILLING_QUEUE = "downstream-billing"
CRM_QUEUE = "downstream-crm"
ANALYTICS_QUEUE = "downstream-analytics"

EVENT_BUS = "boleto-events-bus"
PAYMENT_CONFIRMED_DETAIL_TYPE = "PaymentConfirmed"
EVENT_SOURCE = "boleto.payments"

WEBHOOK_SECRET_NAME = "bank/webhook/hmac-secret"
KMS_ALIAS = "alias/boleto-field-encryption"

STATE_MACHINE_NAME = "BoletoIssuance"

LAMBDA_EXEC_ROLE = "boleto-lambda-execution-role"
