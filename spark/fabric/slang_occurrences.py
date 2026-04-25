import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

from utils.config import load_config
from utils.paths import join_path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--mode", choices=["global", "subreddit"], default="subreddit")
    parser.add_argument(
        "--candidate-type",
        choices=["clean_ratio", "spiking"],
        default="clean_ratio",
    )
    parser.add_argument(
        "--spike-time-grain",
        choices=["monthly", "yearly"],
        default="monthly",
    )
    parser.add_argument(
        "--include-full-text",
        action="store_true",
        help="Join reddit_clean_posts and include full_text in the output.",
    )
    args = parser.parse_args()

    config = load_config(args.config)

    spark = (
        SparkSession.builder
        .appName("BuildSlangOccurrencesFabric")
        .getOrCreate()
    )

    out_dir = config["paths"]["out_dir"]

    # -------------------------
    # Choose candidate input / occurrence output paths
    # -------------------------
    # -- clean_ratio:
    #       Uses the broad clean-ratio slang candidate table.
    #
    # -- spiking:
    #       Uses the stricter clean-ratio + monthly/yearly spiking candidate
    #       table.
    # -------------------------
    if args.candidate_type == "clean_ratio":
        candidate_name = f"slang_candidates_clean_ratio_{args.mode}_parquet"
        occurrence_name = f"slang_occurrences_clean_ratio_{args.mode}_parquet"
    else:
        candidate_name = (
            f"slang_candidates_clean_ratio_spiking_"
            f"{args.spike_time_grain}_{args.mode}_parquet"
        )
        occurrence_name = (
            f"slang_occurrences_clean_ratio_spiking_"
            f"{args.spike_time_grain}_{args.mode}_parquet"
        )

    candidates_dir = join_path(out_dir, candidate_name)
    occurrences_dir = join_path(out_dir, occurrence_name)

    reddit_tokens_dir = join_path(out_dir, "reddit_tokens_parquet")

    reddit_tokens = spark.read.parquet(reddit_tokens_dir)
    candidates = spark.read.parquet(candidates_dir)

    # -------------------------
    # Choose join keys
    # -------------------------
    # -- Subreddit candidates are subreddit-specific:
    #       join on [subreddit, token]
    #
    # -- Global candidates apply everywhere:
    #       join on [token]
    # -------------------------
    if args.mode == "subreddit":
        join_cols = ["subreddit", "token"]
    else:
        join_cols = ["token"]

    # -------------------------
    # Build sparse token occurrence annotations
    # -------------------------
    # -- This is the canonical space-efficient output:
    #       one row per slang-candidate token occurrence
    #
    # -- It intentionally does not include full text by default. Full text can
    #    be recovered by joining reddit_clean_posts_parquet on post_id.
    # -------------------------
    candidate_metadata_cols = [
        column_name
        for column_name in [
            "clean_ratio_score",
            "count_reddit_all_time",
            "total_tokens_all_time",
            "p_reddit_all_time",
            "p_clean",
            "max_spike_score",
            "num_spike_periods",
        ]
        if column_name in candidates.columns
    ]

    candidate_select_cols = [*join_cols, *candidate_metadata_cols]

    occurrences = (
        reddit_tokens
        .join(candidates.select(*candidate_select_cols), on=join_cols, how="inner")
        .select(
            "post_id",
            "subreddit",
            "timestamp",
            "year",
            "month",
            "week_start_date",
            "token",
            "surface",
            "token_start",
            "token_end",
            lit(True).alias("is_slang_candidate"),
            lit(args.candidate_type).alias("candidate_type"),
            lit(args.mode).alias("candidate_mode"),
            lit(args.spike_time_grain if args.candidate_type == "spiking" else None).alias(
                "spike_time_grain"
            ).cast("string"),
            *candidate_metadata_cols,
        )
    )

    # -------------------------
    # Optional full-text export
    # -------------------------
    # -- This duplicates post text for every candidate token occurrence, so it
    #    is disabled by default. Use only when you need an inspection/export
    #    table with full comment text.
    # -------------------------
    if args.include_full_text:
        clean_posts_dir = join_path(out_dir, "reddit_clean_posts_parquet")
        clean_posts = spark.read.parquet(clean_posts_dir)

        occurrences = occurrences.join(
            clean_posts.select("post_id", col("body").alias("full_text")),
            on="post_id",
            how="left",
        )

    occurrences.write.mode("overwrite").parquet(occurrences_dir)

    spark.stop()


if __name__ == "__main__":
    main()
