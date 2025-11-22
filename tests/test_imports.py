def test_imports():
    import importlib
    import os
    import sys

    # Ensure `src/` is on sys.path so `data_platform` imports during tests
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    src_path = os.path.join(project_root, "src")
    if src_path not in sys.path:
        sys.path.insert(0, src_path)

    # pandas
    import pandas as pd

    assert getattr(pd, "__version__", None)

    # data_platform package
    pkg = importlib.import_module("data_platform")
    assert pkg is not None
