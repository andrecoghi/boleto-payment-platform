"""Shared boto3 client factory for all Lambdas running against LocalStack.

Inside LocalStack's Lambda containers, the endpoint is reachable via the
LOCALSTACK_HOSTNAME env var LocalStack injects automatically. We fall back to
AWS_ENDPOINT_URL (set explicitly in bootstrap) for local/manual invocation.
"""
import os
import boto3
from botocore.config import Config

REGION = os.environ.get("AWS_REGION", "us-east-1")


def _endpoint_url() -> str:
    explicit = os.environ.get("AWS_ENDPOINT_URL")
    if explicit:
        return explicit
    hostname = os.environ.get("LOCALSTACK_HOSTNAME", "localstack")
    port = os.environ.get("EDGE_PORT", "4566")
    return f"http://{hostname}:{port}"


def _config(service: str) -> Config | None:
    if service == "s3":
        # Force path-style (http://host:port/bucket/key) instead of
        # virtual-hosted-style (http://bucket.host:port/key) -- the latter
        # produces presigned URLs whose hostname (e.g. boleto-pdfs.localstack)
        # nothing on the docker network can actually resolve.
        return Config(s3={"addressing_style": "path"})
    return None


def client(service: str):
    return boto3.client(service, region_name=REGION, endpoint_url=_endpoint_url(), config=_config(service))


def resource(service: str):
    return boto3.resource(service, region_name=REGION, endpoint_url=_endpoint_url(), config=_config(service))
