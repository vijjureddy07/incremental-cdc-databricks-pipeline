import os
import sys

from pyspark.sql import SparkSession

DEFAULT_JAVA_HOME = "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"

# Ensure worker Python matches driver Python exactly and headless Java on macOS
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["JAVA_TOOL_OPTIONS"] = "-Djava.awt.headless=true"


def get_spark_session(app_name: str = "Incremental-CDC-Pipeline") -> SparkSession:
    """Build or retrieve an existing local SparkSession.

    Configured for laptop-friendly execution with low shuffle partitions.
    """
    if "JAVA_HOME" not in os.environ and os.path.exists(DEFAULT_JAVA_HOME):
        os.environ["JAVA_HOME"] = DEFAULT_JAVA_HOME

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config(
            "spark.driver.extraJavaOptions",
            "-Djava.awt.headless=true -Dderby.system.home=/tmp/derby",
        )
        .config("spark.executor.extraJavaOptions", "-Djava.awt.headless=true")
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def stop_spark_session() -> None:
    """Safely stop any active SparkSession."""
    active = SparkSession.getActiveSession()
    if active:
        active.stop()
