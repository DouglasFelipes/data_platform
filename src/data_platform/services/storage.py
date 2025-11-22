import os
from pathlib import Path

import pandas as pd
from prefect import task


@task(name="save_to_storage", retries=3, retry_delay_seconds=5)
def save_dataframe(df: pd.DataFrame, bucket: str, path: str, format: str = "parquet"):
    """
    Task Genérica de Load.

    Behavior:
    - If google-cloud-storage is available, attempt to upload to GCS.
    - Otherwise, save to local path: <bucket>/<path>/data.<format>
    """
    if df.empty:
        print("⚠️ DataFrame vazio. Nada a salvar.")
        return None

    full_path = f"{path}/data.{format}"

    # Try to use GCS if available
    try:
        from google.cloud import storage

        client = storage.Client()
        bucket_obj = client.bucket(bucket)
        blob = bucket_obj.blob(full_path)

        # write to a temp local file
        tmp_dir = Path(".tmp_storage")
        tmp_dir.mkdir(parents=True, exist_ok=True)
        tmp_file = tmp_dir / f"data.{format}"
        if format == "parquet":
            df.to_parquet(tmp_file)
        elif format == "csv":
            df.to_csv(tmp_file, index=False)
        else:
            df.to_parquet(tmp_file)

        blob.upload_from_filename(str(tmp_file))
        os.remove(tmp_file)
        print(f"💾 [Storage Service] Uploaded to gs://{bucket}/{full_path}")
        return f"gs://{bucket}/{full_path}"
    except Exception:
        # Fallback: save locally under <bucket>/<path>/
        out_dir = Path(bucket) / path
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / f"data.{format}"
        if format == "parquet":
            df.to_parquet(out_file)
        elif format == "csv":
            df.to_csv(out_file, index=False)
        else:
            df.to_parquet(out_file)

        print(f"💾 [Storage Service] Saved locally to: {out_file}")
        return str(out_file)
