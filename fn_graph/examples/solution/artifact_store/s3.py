import logging
from typing import Any

import boto3
import cloudpickle
from botocore.exceptions import ClientError

from .base import BaseArtifactStore

log = logging.getLogger(__name__)


class S3ArtifactStore(BaseArtifactStore):
    def __init__(self, bucket: str, run_id: str, region: str):
        self.bucket = bucket
        self.run_id = run_id
        self.region = region
        self._client = boto3.client("s3", region_name=region)

    def _key(self, key: str) -> str:
        return f"{self.run_id}/{key}.pkl"

    def put(self, key: str, value: Any) -> None:
        s3_key = self._key(key)
        log.debug(f"[S3ArtifactStore] uploading {key} to s3://{self.bucket}/{s3_key}")
        data = cloudpickle.dumps(value, protocol=4)
        self._client.put_object(Bucket=self.bucket, Key=s3_key, Body=data)
        log.debug(f"[S3ArtifactStore] {key} uploaded, size: {len(data)} bytes")

    def get(self, key: str) -> Any:
        s3_key = self._key(key)
        log.debug(f"[S3ArtifactStore] downloading {key} from s3://{self.bucket}/{s3_key}")
        response = self._client.get_object(Bucket=self.bucket, Key=s3_key)
        result = cloudpickle.loads(response["Body"].read())
        log.debug(f"[S3ArtifactStore] {key} downloaded, type: {type(result).__name__}")
        return result

    def exists(self, key: str) -> bool:
        try:
            self._client.head_object(Bucket=self.bucket, Key=self._key(key))
            result = True
        except ClientError as e:
            if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
                result = False
            else:
                raise
        log.debug(f"[S3ArtifactStore] exists({key}): {result}")
        return result

    def delete(self, key: str) -> None:
        self._client.delete_object(Bucket=self.bucket, Key=self._key(key))

    def metadata(self, key: str) -> dict:
        response = self._client.head_object(Bucket=self.bucket, Key=self._key(key))
        return {
            "size": response["ContentLength"],
            "last_modified": response["LastModified"],
        }
