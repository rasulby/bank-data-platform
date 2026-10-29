"""Shared connection settings; no network calls during import."""

import os
import re

import boto3
import clickhouse_connect
import psycopg2


def env(name):
    return os.environ[name]


def identifier(name):
    value = env(name)
    if not re.fullmatch(r"[a-z][a-z0-9_]*", value):
        raise ValueError(f"{name} must be a lowercase SQL identifier")
    return value


def postgres():
    return psycopg2.connect(
        host=env("SOURCE_DB_HOST"),
        port=env("SOURCE_DB_PORT"),
        user=env("SOURCE_DB_USER"),
        password=env("SOURCE_DB_PASSWORD"),
        dbname=env("SOURCE_DB_NAME"),
    )


def warehouse():
    return clickhouse_connect.get_client(
        host=env("CLICKHOUSE_HOST"),
        port=int(env("CLICKHOUSE_PORT")),
        username=env("CLICKHOUSE_USER"),
        password=env("CLICKHOUSE_PASSWORD"),
    )


def object_store():
    return boto3.client(
        "s3",
        endpoint_url=env("S3_ENDPOINT"),
        aws_access_key_id=env("MINIO_ROOT_USER"),
        aws_secret_access_key=env("MINIO_ROOT_PASSWORD"),
        region_name=env("AWS_REGION"),
    )
