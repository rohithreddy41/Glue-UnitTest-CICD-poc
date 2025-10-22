# tests/test_etl_job.py
import pytest
from pyspark.sql import SparkSession
from src.etl_job import run_etl
import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

@pytest.fixture(scope="session")
def spark():
    """Provide a SparkSession for tests (inside Glue Docker image)."""
    return (
        SparkSession.builder
        .appName("pytest-glue-etl")
        .master("local[2]")
        .getOrCreate()
    )

def test_etl_workflow(spark, tmp_path):
    # Run ETL
    output_path = str(tmp_path / "output")
    df_out = run_etl(spark, output_path)

    # Collect results
    result = [(r["name"], r["age"], r["is_adult"]) for r in df_out.collect()]

    expected = [("ALICE", 17, False), ("BOB", 25, True)]

    assert sorted(result) == sorted(expected)


