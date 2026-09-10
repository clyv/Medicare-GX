"""
validate_service.py
Builds and runs the by-Provider-and-Service contract.

    python pipelines/validate_service.py --backend postgres
    python pipelines/validate_service.py --backend spark

Pandas is deliberately not offered. This extract is about 10M rows across 28
columns, one of which (HCPCS_Desc) is a long free-text description, so the
frame runs to several gigabytes before any validation starts. Postgres and
Spark are the engines built for it — which is the argument for a portable
contract in the first place: the same rules survive the move to an engine
that can actually hold the data.
"""

import argparse
import os
import sys
from pathlib import Path

import great_expectations as gx
from dotenv import load_dotenv
from great_expectations.core.expectation_suite import ExpectationSuite

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipelines.contract_service import (  # noqa: E402
    ADVISORY_SUITE_NAME,
    ALL_COLUMNS,
    BLOCKING_SUITE_NAME,
    FLOAT_COLUMNS,
    INTEGER_COLUMNS,
    STRING_COLUMNS,
    advisory_expectations,
    blocking_expectations,
)
from pipelines.download_data import dataset_path  # noqa: E402
from pipelines.validation import (  # noqa: E402
    ADVISORY,
    BLOCKING,
    read_csv_header,
    run_tiered_validation,
)

load_dotenv()

CONN = os.getenv(
    "POSTGRES_CONN",
    "postgresql+psycopg2://gx_user:gx_password@localhost:5432/medicare_db",
)
TABLE_NAME = "mup_provider_service"
DATA_FILE = dataset_path("service")

TIERS = ((BLOCKING, BLOCKING_SUITE_NAME), (ADVISORY, ADVISORY_SUITE_NAME))


def build_suites(context) -> None:
    for name, expectations in (
        (BLOCKING_SUITE_NAME, blocking_expectations()),
        (ADVISORY_SUITE_NAME, advisory_expectations()),
    ):
        try:
            context.suites.delete(name)
        except Exception:
            pass
        suite = context.suites.add(ExpectationSuite(name=name))
        for expectation in expectations:
            suite.add_expectation(expectation)
        print(f"  {name:<26} {len(suite.expectations):>3} expectations")


def postgres_batch(context):
    try:
        datasource = context.data_sources.get("cms_service_postgres")
    except Exception:
        datasource = context.data_sources.add_postgres(
            name="cms_service_postgres", connection_string=CONN
        )
    try:
        asset = datasource.get_asset("mup_service_table")
    except Exception:
        asset = datasource.add_table_asset(
            name="mup_service_table", table_name=TABLE_NAME
        )
    return asset.add_batch_definition_whole_table("whole_table"), None


def spark_batch(context):
    from pyspark.sql import SparkSession
    from pyspark.sql.types import (
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    spark = (
        SparkSession.builder.appName("medicare-gx-service-contract")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.driver.memory", "6g")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print(f"[INFO] Spark {spark.version} session started.")

    string_cols, float_cols = set(STRING_COLUMNS), set(FLOAT_COLUMNS)
    integer_cols = set(INTEGER_COLUMNS)

    # Ordered by the file's own header: Spark binds an explicit schema by
    # position, so following the contract's list instead would silently
    # relabel every column past any divergence. See validate_spark.py.
    fields = []
    for column in read_csv_header(DATA_FILE):
        if column in string_cols:
            spark_type = StringType()
        elif column in float_cols:
            spark_type = DoubleType()
        elif column in integer_cols:
            spark_type = LongType()
        else:
            spark_type = StringType()
        fields.append(StructField(column, spark_type, nullable=True))

    dataframe = spark.read.csv(
        str(DATA_FILE),
        header=True,
        schema=StructType(fields),
        mode="PERMISSIVE",
        # HCPCS_Desc and Rndrng_Prvdr_RUCA_Desc are long quoted free text that
        # can contain newlines; without multiLine the reader tears those
        # records apart and shifts the columns after them. See validate_spark.py.
        multiLine=True,
        quote='"',
        escape='"',
    )

    try:
        datasource = context.data_sources.get("cms_service_spark")
    except Exception:
        datasource = context.data_sources.add_spark(name="cms_service_spark")
    try:
        asset = datasource.get_asset("mup_service_spark")
    except Exception:
        asset = datasource.add_dataframe_asset(name="mup_service_spark")

    batch_definition = asset.add_batch_definition_whole_dataframe("full_spark_batch")
    return batch_definition, {"dataframe": dataframe}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", choices=["postgres", "spark"], required=True)
    args = parser.parse_args()

    context = gx.get_context(mode="file")

    print("Building the by-provider-and-service suites\n")
    build_suites(context)

    if args.backend == "postgres":
        batch_definition, batch_parameters = postgres_batch(context)
        label = f"PostgreSQL — {TABLE_NAME}"
    else:
        batch_definition, batch_parameters = spark_batch(context)
        label = f"Spark — {DATA_FILE.name}"

    passed = run_tiered_validation(
        context,
        batch_definition,
        backend_label=label,
        prefix=f"mup_service_{args.backend}",
        tiers=TIERS,
        batch_parameters=batch_parameters,
    )
    return 0 if passed else 1


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.exit(main())
