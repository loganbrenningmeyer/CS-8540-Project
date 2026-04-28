import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, lit

from utils.config import load_config
from utils.paths import join_path


TOKEN_KEY_COLS = ["post_id", "token", "token_start", "token_end"]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument(
        "--tokens-table",
        default="reddit_tokens_subset_10pct_parquet",
        help="Parquet table containing all token occurrences to label.",
    )
    parser.add_argument(
        "--occurrences-table",
        default="slang_occurrences_clean_ratio_spiking_combined_subreddit_parquet",
        help="Parquet table containing positive slang-candidate occurrences.",
    )
    args = parser.parse_args()

    config = load_config(args.config)

    spark = (
        SparkSession.builder
        .appName("CountSlangNonSlangCandidateOccurrencesFabric")
        .getOrCreate()
    )

    out_dir = config["paths"]["out_dir"]
    tokens_dir = join_path(out_dir, args.tokens_table)
    occurrences_dir = join_path(out_dir, args.occurrences_table)

    tokens = spark.read.parquet(tokens_dir).select(*TOKEN_KEY_COLS)
    positive_keys = (
        spark.read.parquet(occurrences_dir)
        .select(*TOKEN_KEY_COLS)
        .distinct()
        .withColumn("is_slang_candidate", lit(True))
    )

    labeled_counts = (
        tokens
        .join(positive_keys, on=TOKEN_KEY_COLS, how="left")
        .withColumn(
            "is_slang_candidate",
            coalesce(col("is_slang_candidate"), lit(False)),
        )
        .groupBy("is_slang_candidate")
        .count()
        .collect()
    )

    counts = {
        row["is_slang_candidate"]: row["count"]
        for row in labeled_counts
    }

    slang_candidate_occurrences = counts.get(True, 0)
    non_slang_candidate_occurrences = counts.get(False, 0)
    total_token_occurrences = (
        slang_candidate_occurrences + non_slang_candidate_occurrences
    )

    print("tokens_table={}".format(args.tokens_table))
    print("occurrences_table={}".format(args.occurrences_table))
    print("total_token_occurrences={}".format(total_token_occurrences))
    print("slang_candidate_occurrences={}".format(slang_candidate_occurrences))
    print(
        "non_slang_candidate_occurrences={}".format(
            non_slang_candidate_occurrences
        )
    )

    spark.stop()


if __name__ == "__main__":
    main()
