"""
validate_spark.py
Runs the SAME contract a third time, on Spark.

Pandas reads the CSV in one process, Postgres reads a loaded table, Spark
reads the CSV through a distributed engine. One contract, three execution
environments — the point being that a data contract is worth little if it
only holds on the backend it was written against.

The schema is declared rather than inferred. Spark would guess LongType for
Rndrng_NPI, which drops a leading zero and stops the value matching the TEXT
column Postgres holds, so the two backends would validate subtly different
data. The column types come from contract.py, the same place the
expectations do.

Run download_data.py first.
"""

import sys
from pathlib import Path

import great_expectations as gx
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipelines.contract import (  # noqa: E402
    ALL_COLUMNS,
    FLOAT_COLUMNS,
    INTEGER_COLUMNS,
    STRING_COLUMNS,
)
from pipelines.validation import run_tiered_validation  # noqa: E402

load_dotenv()

DATA_FILE = Path("data/raw/mup_phy_r25_p05_v20_d23_prov.csv")
DATASOURCE_NAME = "cms_spark"
ASSET_NAME = "mup_provider_spark"
PREFIX = "mup_spark"


def build_schema():
    """An explicit Spark schema, ordered as the CSV is, typed as the contract says."""
    from pyspark.sql.types import (
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    string_cols = set(STRING_COLUMNS)
    float_cols = set(FLOAT_COLUMNS)
    integer_cols = set(INTEGER_COLUMNS)

    fields = []
    for column in ALL_COLUMNS:
        if column in string_cols:
            spark_type = StringType()
        elif column in float_cols:
            spark_type = DoubleType()
        elif column in integer_cols:
            spark_type = LongType()
        else:  # unreachable while the type lists partition ALL_COLUMNS
            raise ValueError(f"no declared type for {column}")
        fields.append(StructField(column, spark_type, nullable=True))

    return StructType(fields)


def build_session():
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.appName("medicare-gx-contract")
        # A CI runner is a single machine; keep the shuffle small or Spark
        # spends the whole run planning partitions it does not have.
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "4g")
        .master("local[*]")
        .getOrCreate()
    )


def run_spark_validation() -> bool:
    spark = build_session()
    spark.sparkContext.setLogLevel("WARN")
    print(f"[INFO] Spark {spark.version} session started.")

    dataframe = spark.read.csv(
        str(DATA_FILE),
        header=True,
        schema=build_schema(),
        mode="PERMISSIVE",
    )
    print(f"[INFO] Read {DATA_FILE.name} with a declared {len(ALL_COLUMNS)}-column schema.")

    context = gx.get_context(mode="file")

    try:
        datasource = context.data_sources.get(DATASOURCE_NAME)
    except Exception:
        datasource = context.data_sources.add_spark(name=DATASOURCE_NAME)
        print("[INFO] Created Spark datasource.")

    try:
        asset = datasource.get_asset(ASSET_NAME)
    except Exception:
        asset = datasource.add_dataframe_asset(name=ASSET_NAME)

    batch_definition = asset.add_batch_definition_whole_dataframe("full_spark_batch")

    passed = run_tiered_validation(
        context,
        batch_definition,
        backend_label=f"Spark — {DATA_FILE.name}",
        prefix=PREFIX,
        batch_parameters={"dataframe": dataframe},
    )

    spark.stop()
    return passed


if __name__ == "__main__":
    # ✓/✗ in the output would blow up a cp1252 Windows console otherwise
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.exit(0 if run_spark_validation() else 1)
