import code
import os
import platform
import readline
import sys
from rlcompleter import Completer

# pyspark-client needs env vars set before any import that touches pyspark.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from pyspark.sql import SparkSession

try:
    import pyspark

    _version = pyspark.__version__
except Exception:
    _version = "unknown"


def run_pyspark_shell(port: int):
    spark = SparkSession.builder.remote(f"sc://localhost:{port}").getOrCreate()
    try:
        _run(f"localhost:{port}", spark)
    finally:
        spark.stop()


def _run(endpoint: str, spark: SparkSession):
    namespace = {"spark": spark}
    readline.parse_and_bind("tab: complete")
    readline.set_completer(Completer(namespace).complete)

    python_version = platform.python_version()
    (build_number, build_date) = platform.python_build()
    banner = rf"""Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version {_version}
      /_/

Using Python version {python_version} ({build_number}, {build_date})
Client connected to the Sail Spark Connect server at {endpoint}
SparkSession available as 'spark'."""
    code.interact(local=namespace, banner=banner, exitmsg="")
