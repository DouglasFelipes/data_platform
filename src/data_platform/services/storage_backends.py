from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path

import pandas as pd


class Storage(ABC):
    """Abstract storage backend interface."""

    @abstractmethod
    def upload(
        self, df: pd.DataFrame, bucket: str, path: str, format: str = "parquet"
    ) -> str:
        """Upload DataFrame and return the remote path (or local path fallback)."""
        raise NotImplementedError()


class LocalStorage(Storage):
    """Save DataFrame locally under `<bucket>/<path>/data.<format>`."""

    def upload(
        self, df: pd.DataFrame, bucket: str, path: str, format: str = "parquet"
    ) -> str:
        out_dir = Path(bucket) / path
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / f"data.{format}"
        if format == "csv":
            df.to_csv(out_file, index=False)
        else:
            df.to_parquet(out_file)
        return str(out_file)


class GCSStorage(Storage):
    """GCS-backed storage implementation (requires google-cloud-storage).

    This class is intentionally small: for production you'd add retry/backoff,
    better temp-file handling and chunked uploads.
    """

    def upload(
        self, df: pd.DataFrame, bucket: str, path: str, format: str = "parquet"
    ) -> str:
        try:
            from google.cloud import storage
        except Exception as exc:  # pragma: no cover - requires external lib
            raise RuntimeError("google-cloud-storage not available") from exc

        client = storage.Client()
        bucket_obj = client.bucket(bucket)
        blob = bucket_obj.blob(f"{path}/data.{format}")

        # write to temp file then upload
        tmp = Path(".tmp_storage")
        tmp.mkdir(parents=True, exist_ok=True)
        tmp_file = tmp / f"data.{format}"
        if format == "csv":
            df.to_csv(tmp_file, index=False)
        else:
            df.to_parquet(tmp_file)

        blob.upload_from_filename(str(tmp_file))
        try:
            tmp_file.unlink()
        except Exception:
            pass
        return f"gs://{bucket}/{path}/data.{format}"
