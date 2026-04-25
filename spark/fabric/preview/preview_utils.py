import argparse
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


# Allow preview scripts in spark/fabric/preview to import spark/fabric/utils.
FABRIC_DIR = Path(__file__).resolve().parents[1]
if str(FABRIC_DIR) not in sys.path:
    sys.path.insert(0, str(FABRIC_DIR))

from utils.config import load_config  # noqa: E402
from utils.paths import join_path  # noqa: E402


def base_parser(description: str) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--config", required=True)
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--truncate", default="false", choices=["true", "false"])
    return parser


def get_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )


def output_path(config_path: str, table_name: str) -> str:
    config = load_config(config_path)
    return join_path(config["paths"]["out_dir"], table_name)


def show_table(
    spark: SparkSession,
    path: str,
    limit: int,
    truncate: str,
    sort_cols: list[tuple[str, bool]] | None = None,
) -> None:
    df = spark.read.parquet(path)

    if sort_cols:
        order_exprs = []
        for column_name, descending in sort_cols:
            if column_name in df.columns:
                order_exprs.append(
                    col(column_name).desc() if descending else col(column_name).asc()
                )

        if order_exprs:
            df = df.orderBy(*order_exprs)

    df.show(limit, truncate=(truncate == "true"))
