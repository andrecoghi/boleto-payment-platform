"""Provisions the entire boleto platform inside LocalStack.

This is the local-dev stand-in for a Terraform/CloudFormation apply: it
creates every AWS resource the architecture doc describes (minus the ones
intentionally out of scope -- CloudFront, DAX, RDS/Aurora itself, see the
README) and wires Lambdas, Step Functions, API Gateway, SQS/SNS/EventBridge
fan-out and DynamoDB Streams together the same way the production design
does. It's meant to be idempotent: re-running it against an already
bootstrapped LocalStack is safe (resources are looked up/recreated rather
than erroring out on "already exists").
"""
from __future__ import annotations

import io
import json
import os
import secrets
import shutil
import subprocess
import sys
import tempfile
import time
import zipfile
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION = os.environ.get("AWS_REGION", "us-east-1")
ACCOUNT_ID = os.environ.get("AWS_ACCOUNT_ID", "000000000000")
ENDPOINT_URL = os.environ.get("AWS_ENDPOINT_URL", "http://localstack:4566")
REPO_ROOT = Path(__file__).resolve().parent.parent
LAMBDAS_DIR = REPO_ROOT / "services" / "lambdas"
COMMON_DIR = LAMBDAS_DIR / "common"
SHARED_CONFIG_PATH = Path(os.environ.get("SHARED_CONFIG_PATH", "/shared/endpoints.json"))

sys.path.append(str(COMMON_DIR))
import names  # noqa: E402

BOTO_CONFIG = Config(retries={"max_attempts": 10, "mode": "standard"})


def aws(service: str):
    return boto3.client(
        service,
        region_name=REGION,
        endpoint_url=ENDPOINT_URL,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        config=BOTO_CONFIG,
    )


def wait_for_localstack() -> None:
    print("[bootstrap] waiting for LocalStack to report healthy...")
    import urllib.request

    for attempt in range(60):
        try:
            with urllib.request.urlopen(f"{ENDPOINT_URL}/_localstack/health", timeout=3) as resp:
                if resp.status == 200:
                    print("[bootstrap] LocalStack is up")
                    return
        except Exception:
            pass
        time.sleep(3)
    raise RuntimeError("LocalStack did not become healthy in time")


# ---------------------------------------------------------------------------
# S3 + KMS
# ---------------------------------------------------------------------------

def ensure_kms_key() -> str:
    kms = aws("kms")
    try:
        existing = kms.describe_key(KeyId=names.KMS_ALIAS)
        return existing["KeyMetadata"]["KeyId"]
    except ClientError:
        pass

    key = kms.create_key(Description="Boleto platform field-level encryption + S3 SSE key")["KeyMetadata"]
    kms.create_alias(AliasName=names.KMS_ALIAS, TargetKeyId=key["KeyId"])
    print(f"[bootstrap] created KMS key {key['KeyId']} ({names.KMS_ALIAS})")
    return key["KeyId"]


def ensure_bucket() -> None:
    s3 = aws("s3")
    try:
        s3.head_bucket(Bucket=names.PDF_BUCKET)
    except ClientError:
        s3.create_bucket(Bucket=names.PDF_BUCKET)
        print(f"[bootstrap] created S3 bucket {names.PDF_BUCKET}")

    s3.put_bucket_encryption(
        Bucket=names.PDF_BUCKET,
        ServerSideEncryptionConfiguration={
            "Rules": [{"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "aws:kms"}}]
        },
    )
    s3.put_bucket_lifecycle_configuration(
        Bucket=names.PDF_BUCKET,
        LifecycleConfiguration={
            "Rules": [
                {
                    "ID": "archive-old-boletos",
                    "Status": "Enabled",
                    "Filter": {"Prefix": "boletos/"},
                    "Transitions": [{"Days": 90, "StorageClass": "GLACIER"}],
                }
            ]
        },
    )


# ---------------------------------------------------------------------------
# DynamoDB
# ---------------------------------------------------------------------------

def ensure_tables() -> str:
    ddb = aws("dynamodb")

    try:
        table = ddb.describe_table(TableName=names.BOLETOS_TABLE)["Table"]
    except ClientError:
        table = ddb.create_table(
            TableName=names.BOLETOS_TABLE,
            AttributeDefinitions=[{"AttributeName": "boleto_id", "AttributeType": "S"}],
            KeySchema=[{"AttributeName": "boleto_id", "KeyType": "HASH"}],
            BillingMode="PAY_PER_REQUEST",
            StreamSpecification={"StreamEnabled": True, "StreamViewType": "NEW_AND_OLD_IMAGES"},
        )["TableDescription"]
        print(f"[bootstrap] created DynamoDB table {names.BOLETOS_TABLE}")
        ddb.get_waiter("table_exists").wait(TableName=names.BOLETOS_TABLE)
        table = ddb.describe_table(TableName=names.BOLETOS_TABLE)["Table"]

    try:
        ddb.describe_table(TableName=names.PROCESSED_EVENTS_TABLE)
    except ClientError:
        ddb.create_table(
            TableName=names.PROCESSED_EVENTS_TABLE,
            AttributeDefinitions=[{"AttributeName": "processed_event_id", "AttributeType": "S"}],
            KeySchema=[{"AttributeName": "processed_event_id", "KeyType": "HASH"}],
            BillingMode="PAY_PER_REQUEST",
        )
        print(f"[bootstrap] created DynamoDB table {names.PROCESSED_EVENTS_TABLE}")
        ddb.get_waiter("table_exists").wait(TableName=names.PROCESSED_EVENTS_TABLE)

    return table["LatestStreamArn"]


# ---------------------------------------------------------------------------
# SQS / SNS / EventBridge
# ---------------------------------------------------------------------------

def ensure_queue(sqs, name: str, *, fifo: bool, redrive_target_arn: str | None = None) -> tuple[str, str]:
    attrs = {}
    if fifo:
        attrs["FifoQueue"] = "true"
        attrs["ContentBasedDeduplication"] = "false"
    if redrive_target_arn:
        attrs["RedrivePolicy"] = json.dumps({"deadLetterTargetArn": redrive_target_arn, "maxReceiveCount": "5"})

    try:
        url = sqs.get_queue_url(QueueName=name)["QueueUrl"]
    except ClientError:
        url = sqs.create_queue(QueueName=name, Attributes=attrs)["QueueUrl"]
        print(f"[bootstrap] created SQS queue {name}")

    if attrs:
        sqs.set_queue_attributes(QueueUrl=url, Attributes=attrs)

    arn = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    return url, arn


def ensure_messaging() -> dict:
    sqs = aws("sqs")
    sns = aws("sns")
    events = aws("events")

    _, dlq_arn = ensure_queue(sqs, names.PAYMENT_EVENTS_DLQ, fifo=True)
    payment_queue_url, payment_queue_arn = ensure_queue(
        sqs, names.PAYMENT_EVENTS_QUEUE, fifo=True, redrive_target_arn=dlq_arn
    )

    billing_url, billing_arn = ensure_queue(sqs, names.BILLING_QUEUE, fifo=False)
    crm_url, crm_arn = ensure_queue(sqs, names.CRM_QUEUE, fifo=False)
    analytics_url, analytics_arn = ensure_queue(sqs, names.ANALYTICS_QUEUE, fifo=False)

    try:
        topic_arn = sns.create_topic(Name=names.NOTIFICATIONS_TOPIC)["TopicArn"]
    except ClientError:
        topic_arn = [t["TopicArn"] for t in sns.list_topics()["Topics"] if names.NOTIFICATIONS_TOPIC in t["TopicArn"]][0]

    for queue_url, queue_arn in [(billing_url, billing_arn), (crm_url, crm_arn), (analytics_url, analytics_arn)]:
        sqs.set_queue_attributes(
            QueueUrl=queue_url,
            Attributes={
                "Policy": json.dumps(
                    {
                        "Version": "2012-10-17",
                        "Statement": [
                            {
                                "Effect": "Allow",
                                "Principal": {"Service": "sns.amazonaws.com"},
                                "Action": "sqs:SendMessage",
                                "Resource": queue_arn,
                                "Condition": {"ArnEquals": {"aws:SourceArn": topic_arn}},
                            }
                        ],
                    }
                )
            },
        )
        existing_subs = sns.list_subscriptions_by_topic(TopicArn=topic_arn)["Subscriptions"]
        if not any(s["Endpoint"] == queue_arn for s in existing_subs):
            sub = sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=queue_arn)
            sns.set_subscription_attributes(
                SubscriptionArn=sub["SubscriptionArn"], AttributeName="RawMessageDelivery", AttributeValue="true"
            )

    try:
        events.create_event_bus(Name=names.EVENT_BUS)
        print(f"[bootstrap] created EventBridge bus {names.EVENT_BUS}")
    except ClientError:
        pass

    events.put_rule(
        Name="payment-confirmed-to-notifications",
        EventBusName=names.EVENT_BUS,
        EventPattern=json.dumps({"detail-type": [names.PAYMENT_CONFIRMED_DETAIL_TYPE]}),
        State="ENABLED",
    )
    events.put_targets(
        Rule="payment-confirmed-to-notifications",
        EventBusName=names.EVENT_BUS,
        # InputPath unwraps EventBridge's envelope (version/id/detail-type/...)
        # so billing/CRM/analytics each just get the {boleto_id, amount_cents,
        # paid_at} payload they actually care about.
        Targets=[{"Id": "notifications-topic", "Arn": topic_arn, "InputPath": "$.detail"}],
    )
    try:
        sns.set_topic_attributes(
            TopicArn=topic_arn,
            AttributeName="Policy",
            AttributeValue=json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {"Service": "events.amazonaws.com"},
                            "Action": "sns:Publish",
                            "Resource": topic_arn,
                        }
                    ],
                }
            ),
        )
    except ClientError:
        pass

    return {
        "payment_queue_url": payment_queue_url,
        "payment_queue_arn": payment_queue_arn,
        "billing_queue_url": billing_url,
        "crm_queue_url": crm_url,
        "analytics_queue_url": analytics_url,
        "notifications_topic_arn": topic_arn,
    }


# ---------------------------------------------------------------------------
# Secrets Manager
# ---------------------------------------------------------------------------

def ensure_webhook_secret() -> None:
    secretsmanager = aws("secretsmanager")
    secret_value = json.dumps({"hmac_secret": secrets.token_hex(32)})
    try:
        secretsmanager.create_secret(Name=names.WEBHOOK_SECRET_NAME, SecretString=secret_value)
        print(f"[bootstrap] created secret {names.WEBHOOK_SECRET_NAME}")
    except ClientError as exc:
        if exc.response["Error"]["Code"] not in ("ResourceExistsException",):
            raise


# ---------------------------------------------------------------------------
# IAM
# ---------------------------------------------------------------------------

def ensure_lambda_role() -> str:
    iam = aws("iam")
    trust_policy = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Allow", "Principal": {"Service": "lambda.amazonaws.com"}, "Action": "sts:AssumeRole"}],
        }
    )
    try:
        role = iam.create_role(RoleName=names.LAMBDA_EXEC_ROLE, AssumeRolePolicyDocument=trust_policy)["Role"]
        print(f"[bootstrap] created IAM role {names.LAMBDA_EXEC_ROLE}")
    except ClientError:
        role = iam.get_role(RoleName=names.LAMBDA_EXEC_ROLE)["Role"]

    # Intentionally permissive for local dev only. Production uses IAM roles
    # scoped per Lambda (webhook receiver can't touch the reconciliation
    # data store, etc.) -- see README "Divergences from production".
    iam.put_role_policy(
        RoleName=names.LAMBDA_EXEC_ROLE,
        PolicyName="local-dev-permissive",
        PolicyDocument=json.dumps(
            {"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Action": "*", "Resource": "*"}]}
        ),
    )
    return role["Arn"]


# ---------------------------------------------------------------------------
# Lambda packaging + deployment
# ---------------------------------------------------------------------------

LAMBDA_SPECS = {
    "api_create_boleto": {"timeout": 15, "env": {}},
    "api_get_boleto": {"timeout": 15, "env": {}},
    "generate_boleto": {"timeout": 15, "env": {}},
    "register_bank": {"timeout": 20, "env": {"BANK_SIMULATOR_URL": "http://bank-simulator:8080"}},
    "finalize_issuance": {"timeout": 15, "env": {}},
    "mark_failed": {"timeout": 15, "env": {}},
    "webhook_receiver": {"timeout": 15, "env": {}},
    "process_payment": {"timeout": 20, "env": {}},
    "stream_to_reconciliation": {
        "timeout": 30,
        "env": {
            "PGHOST": "pgbouncer",
            "PGPORT": "6432",
            "PGDATABASE": "reconciliation",
            "PGUSER": "reconciliation",
            "PGPASSWORD": "reconciliation",
        },
    },
}


def build_zip(function_name: str) -> bytes:
    src_dir = LAMBDAS_DIR / function_name
    buffer = io.BytesIO()

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        requirements = src_dir / "requirements.txt"
        if requirements.exists():
            subprocess.run(
                [sys.executable, "-m", "pip", "install", "--quiet", "--target", str(tmp_path), "-r", str(requirements)],
                check=True,
            )

        with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.write(src_dir / "handler.py", "handler.py")
            for py_file in COMMON_DIR.glob("*.py"):
                zf.write(py_file, f"common/{py_file.name}")
            for root, _dirs, files in os.walk(tmp_path):
                for f in files:
                    full = Path(root) / f
                    arcname = full.relative_to(tmp_path)
                    zf.write(full, str(arcname))

    return buffer.getvalue()


def ensure_lambda(function_name: str, role_arn: str) -> str:
    lam = aws("lambda")
    spec = LAMBDA_SPECS[function_name]
    zip_bytes = build_zip(function_name)

    env_vars = {
        "AWS_ENDPOINT_URL": ENDPOINT_URL,
        "AWS_REGION": REGION,
        "AWS_ACCOUNT_ID": ACCOUNT_ID,
        **spec["env"],
    }

    try:
        lam.get_function(FunctionName=function_name)
        already_exists = True
    except ClientError:
        already_exists = False

    if already_exists:
        lam.update_function_code(FunctionName=function_name, ZipFile=zip_bytes)
        lam.get_waiter("function_updated_v2").wait(FunctionName=function_name)
        lam.update_function_configuration(FunctionName=function_name, Environment={"Variables": env_vars})
        lam.get_waiter("function_updated_v2").wait(FunctionName=function_name)
        print(f"[bootstrap] updated Lambda {function_name}")
    else:
        lam.create_function(
            FunctionName=function_name,
            Runtime="python3.12",
            Role=role_arn,
            Handler="handler.handler",
            Code={"ZipFile": zip_bytes},
            Timeout=spec["timeout"],
            MemorySize=256,
            Environment={"Variables": env_vars},
        )
        print(f"[bootstrap] created Lambda {function_name}")

    lam.get_waiter("function_active_v2").wait(FunctionName=function_name)
    return f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:{function_name}"


def ensure_event_source_mappings(payment_queue_arn: str, boletos_stream_arn: str) -> None:
    lam = aws("lambda")

    def mapping_exists(function_name: str, source_arn: str) -> bool:
        mappings = lam.list_event_source_mappings(FunctionName=function_name)["EventSourceMappings"]
        return any(m["EventSourceArn"] == source_arn for m in mappings)

    if not mapping_exists("process_payment", payment_queue_arn):
        lam.create_event_source_mapping(
            FunctionName="process_payment", EventSourceArn=payment_queue_arn, BatchSize=1, Enabled=True
        )
        print("[bootstrap] wired SQS FIFO payment-events -> process_payment")

    if not mapping_exists("stream_to_reconciliation", boletos_stream_arn):
        lam.create_event_source_mapping(
            FunctionName="stream_to_reconciliation",
            EventSourceArn=boletos_stream_arn,
            BatchSize=10,
            StartingPosition="TRIM_HORIZON",
            Enabled=True,
            # Without a bound, a persistent failure (Postgres down, a bad
            # record) retries the same batch forever and stalls the shard --
            # every later change to every boleto stops reaching the
            # reconciliation store. Cap retries and split the batch on
            # failure so one bad record can't block its batch-mates; in
            # production you'd also add an OnFailure destination (SQS/SNS)
            # so a human gets paged instead of the records just being
            # dropped after MaximumRetryAttempts is exhausted.
            MaximumRetryAttempts=3,
            BisectBatchOnFunctionError=True,
        )
        print("[bootstrap] wired DynamoDB Streams Boletos -> stream_to_reconciliation")


# ---------------------------------------------------------------------------
# Step Functions
# ---------------------------------------------------------------------------

def ensure_state_machine(role_arn: str, lambda_arns: dict) -> str:
    sfn = aws("stepfunctions")
    template = (REPO_ROOT / "infra" / "stepfunctions" / "boleto_issuance.asl.json").read_text()
    definition = (
        template.replace("${GenerateBoletoArn}", lambda_arns["generate_boleto"])
        .replace("${RegisterBankArn}", lambda_arns["register_bank"])
        .replace("${FinalizeIssuanceArn}", lambda_arns["finalize_issuance"])
        .replace("${MarkFailedArn}", lambda_arns["mark_failed"])
    )

    try:
        arn = f"arn:aws:states:{REGION}:{ACCOUNT_ID}:stateMachine:{names.STATE_MACHINE_NAME}"
        sfn.describe_state_machine(stateMachineArn=arn)
        sfn.update_state_machine(stateMachineArn=arn, definition=definition, roleArn=role_arn)
        print(f"[bootstrap] updated Step Functions state machine {names.STATE_MACHINE_NAME}")
    except ClientError:
        arn = sfn.create_state_machine(
            name=names.STATE_MACHINE_NAME,
            definition=definition,
            roleArn=role_arn,
            type="STANDARD",
        )["stateMachineArn"]
        print(f"[bootstrap] created Step Functions state machine {names.STATE_MACHINE_NAME}")

    return arn


# ---------------------------------------------------------------------------
# API Gateway
# ---------------------------------------------------------------------------

def ensure_api_gateway(lambda_arns: dict) -> tuple[str, str]:
    apigw = aws("apigateway")
    lam = aws("lambda")

    existing = [a for a in apigw.get_rest_apis()["items"] if a["name"] == "boleto-api"]
    if existing:
        api_id = existing[0]["id"]
        print(f"[bootstrap] reusing API Gateway {api_id}")
    else:
        api_id = apigw.create_rest_api(name="boleto-api")["id"]
        print(f"[bootstrap] created API Gateway {api_id}")

    root_id = next(r["id"] for r in apigw.get_resources(restApiId=api_id)["items"] if r["path"] == "/")

    def ensure_resource(parent_id: str, path_part: str) -> str:
        resources = apigw.get_resources(restApiId=api_id)["items"]
        for r in resources:
            if r.get("parentId") == parent_id and r.get("pathPart") == path_part:
                return r["id"]
        return apigw.create_resource(restApiId=api_id, parentId=parent_id, pathPart=path_part)["id"]

    boletos_id = ensure_resource(root_id, "boletos")
    boleto_item_id = ensure_resource(boletos_id, "{id}")
    webhooks_id = ensure_resource(root_id, "webhooks")
    payment_webhook_id = ensure_resource(webhooks_id, "payment")

    def wire_method(resource_id: str, http_method: str, function_name: str) -> None:
        uri = (
            f"arn:aws:apigateway:{REGION}:lambda:path/2015-03-31/functions/"
            f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:{function_name}/invocations"
        )
        try:
            apigw.put_method(
                restApiId=api_id, resourceId=resource_id, httpMethod=http_method, authorizationType="NONE"
            )
        except ClientError:
            pass
        apigw.put_integration(
            restApiId=api_id,
            resourceId=resource_id,
            httpMethod=http_method,
            type="AWS_PROXY",
            integrationHttpMethod="POST",
            uri=uri,
        )
        statement_id = f"apigw-{function_name}-{http_method}"
        try:
            lam.add_permission(
                FunctionName=function_name,
                StatementId=statement_id,
                Action="lambda:InvokeFunction",
                Principal="apigateway.amazonaws.com",
                SourceArn=f"arn:aws:execute-api:{REGION}:{ACCOUNT_ID}:{api_id}/*/{http_method}/*",
            )
        except ClientError:
            pass

    wire_method(boletos_id, "POST", "api_create_boleto")
    wire_method(boleto_item_id, "GET", "api_get_boleto")
    wire_method(payment_webhook_id, "POST", "webhook_receiver")

    apigw.create_deployment(restApiId=api_id, stageName="local")
    print("[bootstrap] deployed API Gateway stage 'local'")

    base_url = f"{ENDPOINT_URL}/restapis/{api_id}/local/_user_request_"
    return api_id, base_url


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main() -> None:
    wait_for_localstack()

    ensure_kms_key()
    ensure_bucket()
    boletos_stream_arn = ensure_tables()
    messaging = ensure_messaging()
    ensure_webhook_secret()
    role_arn = ensure_lambda_role()

    lambda_arns = {}
    for function_name in LAMBDA_SPECS:
        lambda_arns[function_name] = ensure_lambda(function_name, role_arn)

    ensure_event_source_mappings(messaging["payment_queue_arn"], boletos_stream_arn)
    ensure_state_machine(role_arn, lambda_arns)
    api_id, base_url = ensure_api_gateway(lambda_arns)

    config = {
        "api_id": api_id,
        "api_base_url": base_url,
        "edge_base_url": "http://nginx:8080",
        "webhook_url": "http://nginx:8080/webhooks/payment",
        "payment_queue_url": messaging["payment_queue_url"],
        "billing_queue_url": messaging["billing_queue_url"],
        "crm_queue_url": messaging["crm_queue_url"],
        "analytics_queue_url": messaging["analytics_queue_url"],
        "notifications_topic_arn": messaging["notifications_topic_arn"],
        "boletos_table": names.BOLETOS_TABLE,
        "pdf_bucket": names.PDF_BUCKET,
    }
    SHARED_CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    SHARED_CONFIG_PATH.write_text(json.dumps(config, indent=2))
    print(f"[bootstrap] wrote shared config to {SHARED_CONFIG_PATH}")
    print("[bootstrap] done")


if __name__ == "__main__":
    main()
