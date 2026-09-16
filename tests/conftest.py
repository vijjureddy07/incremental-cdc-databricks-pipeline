import os
import shutil
import sys
import tempfile
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from src.utils.spark import get_spark_session, stop_spark_session

DEFAULT_JAVA_HOME = "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["JAVA_TOOL_OPTIONS"] = "-Djava.awt.headless=true"


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Session-scoped SparkSession for fast local tests."""
    if "JAVA_HOME" not in os.environ and os.path.exists(DEFAULT_JAVA_HOME):
        os.environ["JAVA_HOME"] = DEFAULT_JAVA_HOME

    spark_sess = get_spark_session(app_name="Test-Incremental-CDC")
    yield spark_sess
    stop_spark_session()


@pytest.fixture
def temp_test_dir() -> Path:
    """Create a clean isolated temporary directory for test artifacts."""
    tmp_path = Path(tempfile.mkdtemp(prefix="cdc_test_"))
    yield tmp_path
    shutil.rmtree(tmp_path, ignore_errors=True)
