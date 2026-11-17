"""Small immutable checkpoint and artifact objects on an S3-compatible store."""

import hashlib
import os
import re
from dataclasses import dataclass, field
from typing import Self
from urllib.parse import urlsplit

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from kusanagi_nerv.domain.execution import CheckpointRef

MAX_BYTES = 4 * 1024 * 1024


class StorageUnavailable(OSError):
    """The store cannot establish an object operation's outcome."""


@dataclass(frozen=True, slots=True)
class ObjectStoreSettings:
    endpoint: str
    bucket: str
    access_key: str = field(repr=False)
    secret_key: str = field(repr=False)

    def __post_init__(self) -> None:
        endpoint = urlsplit(self.endpoint)
        if (
            endpoint.scheme != "http"
            or endpoint.hostname not in {"127.0.0.1", "localhost", "::1"}
            or endpoint.port is None
            or endpoint.username is not None
            or endpoint.password is not None
            or endpoint.path
            or endpoint.fragment
            or endpoint.query
        ):
            raise ValueError(
                "S3 endpoint must be a loopback HTTP origin with an explicit port"
            )
        if not re.fullmatch(r"[a-z0-9][a-z0-9-]{1,61}[a-z0-9]", self.bucket):
            raise ValueError("Invalid recovery bucket name")
        if not self.access_key or not self.secret_key:
            raise ValueError("Explicit local S3 credentials are required")

    @classmethod
    def from_environment(cls) -> Self:
        try:
            return cls(
                *(
                    os.environ[f"KUSANAGI_S3_{name}"]
                    for name in ("ENDPOINT", "BUCKET", "ACCESS_KEY", "SECRET_KEY")
                )
            )
        except KeyError as exc:
            raise ValueError(f"Missing recovery configuration {exc.args[0]}") from None

    @property
    def identity(self) -> str:
        return hashlib.sha256(f"{self.endpoint}/{self.bucket}".encode()).hexdigest()


def attempt_prefix(job_id: str, submission_id: str) -> str:
    return f"jobs/{job_id}/attempts/{submission_id}"


def checkpoint_key(prefix: str, epoch: int) -> str:
    return f"{prefix}/checkpoints/{epoch:04d}.pt"


class ObjectStore:
    def __init__(self, settings: ObjectStoreSettings) -> None:
        self.settings = settings
        self.client = boto3.client(
            "s3",
            endpoint_url=settings.endpoint,
            region_name="us-east-1",
            aws_access_key_id=settings.access_key,
            aws_secret_access_key=settings.secret_key,
            config=Config(
                connect_timeout=2,
                read_timeout=3,
                retries={"total_max_attempts": 1},
                proxies={},
                s3={"addressing_style": "path"},
                request_checksum_calculation="when_required",
                response_checksum_validation="when_required",
            ),
        )

    def uri(self, key: str) -> str:
        return f"s3://{self.settings.bucket}/{key}"

    def _key(self, uri: str) -> str:
        prefix = self.uri("")
        if not uri.startswith(prefix) or not uri[len(prefix) :]:
            raise ValueError("Object URI does not belong to the configured bucket")
        return uri[len(prefix) :]

    def publish(self, key: str, data: bytes) -> tuple[str, str]:
        if not 0 < len(data) <= MAX_BYTES:
            raise ValueError("Object exceeds the recovery format size limit")
        digest = hashlib.sha256(data).hexdigest()
        try:
            # A single conditional PUT is atomic and prevents a competing
            # writer from replacing the bytes of an already committed object.
            self.client.put_object(
                Bucket=self.settings.bucket,
                Key=key,
                Body=data,
                IfNoneMatch="*",
                Metadata={"sha256": digest},
            )
        except ClientError as exc:
            if exc.response["ResponseMetadata"]["HTTPStatusCode"] != 412:
                raise StorageUnavailable("S3 conditional publish failed") from exc
            if self.read(self.uri(key), digest) != data:
                raise ValueError(
                    "Immutable object already exists with different contents"
                ) from exc
        except BotoCoreError as exc:
            raise StorageUnavailable("S3 publish outcome is unknown") from exc
        return self.uri(key), digest

    def read(self, uri: str, expected_sha256: str) -> bytes:
        try:
            response = self.client.get_object(
                Bucket=self.settings.bucket, Key=self._key(uri)
            )
            with response["Body"] as stream:
                data = stream.read(MAX_BYTES + 1)
        except (BotoCoreError, ClientError) as exc:
            raise StorageUnavailable("S3 object could not be read") from exc
        if (
            not 0 < len(data) <= MAX_BYTES
            or hashlib.sha256(data).hexdigest() != expected_sha256
        ):
            raise ValueError("S3 object integrity verification failed")
        return data

    def digest(self, key: str) -> str:
        try:
            response = self.client.head_object(Bucket=self.settings.bucket, Key=key)
        except (BotoCoreError, ClientError) as exc:
            raise StorageUnavailable("S3 object metadata unavailable") from exc
        sha = response.get("Metadata", {}).get("sha256", "")
        if (
            not re.fullmatch(r"[0-9a-f]{64}", sha)
            or not 0 < response["ContentLength"] <= MAX_BYTES
        ):
            raise StorageUnavailable("S3 object has invalid integrity metadata")
        return sha

    def latest_checkpoint(self, prefix: str, epochs: int) -> CheckpointRef | None:
        root = f"{prefix}/checkpoints/"
        try:
            response = self.client.list_objects_v2(
                Bucket=self.settings.bucket, Prefix=root
            )
        except (BotoCoreError, ClientError) as exc:
            raise StorageUnavailable("S3 checkpoint listing unavailable") from exc
        if response.get("IsTruncated"):
            raise StorageUnavailable(
                "Too many objects under this attempt's checkpoint prefix"
            )
        available = []
        for obj in response.get("Contents", []):
            match = re.fullmatch(r"(\d{4})\.pt", obj["Key"].removeprefix(root))
            if match and 1 <= int(match[1]) <= epochs:
                available.append(int(match[1]))
        if not available:
            return None
        epoch = max(available)
        key = checkpoint_key(prefix, epoch)
        return CheckpointRef(self.uri(key), self.digest(key), epoch)
