from data_platform.flows.universal_downloader import universal_download_flow


def test_universal_download_delegates_to_extractor(monkeypatch):
    # Create a dummy extractor class that returns a single file
    class DummyExtractor:
        def __init__(self, url: str, params: dict | None = None):
            self.url = url
            self.params = params or {}

        def find_files(self):
            return ["https://example.com/data/file1.pdf"]

    # Patch get_extractor used inside the flow to return our DummyExtractor
    monkeypatch.setattr(
        "data_platform.flows.universal_downloader.get_extractor",
        lambda et: DummyExtractor,
    )

    # Patch download_file to avoid network calls and return a fake saved path
    # Patch the task used by the flow to avoid network calls and return a fake
    # saved path. The flow calls `download_and_process_task`, so patch that
    # symbol in the module directly.
    monkeypatch.setattr(
        "data_platform.flows.universal_downloader.download_and_process_task",
        lambda url, bucket=None, prefix=None, dataset_name=None: ["/tmp/file1.pdf"],
    )

    # Patch save_metadata to be a no-op
    monkeypatch.setattr(
        "data_platform.flows.universal_downloader.save_metadata",
        lambda files, job_name, dest_dir="data": None,
    )

    cfg = {
        "job_name": "test_job",
        "environment": "dev",
        "source_type": "pdf",
        "source_url": "https://example.com/start",
        "source_params": {"max_files": 1, "dataset_name": "test_dataset"},
        "destination_path": "data_test",
        "destination_bucket": "local",
    }

    result = universal_download_flow(cfg)
    assert isinstance(result, list)
    assert result == ["/tmp/file1.pdf"]
