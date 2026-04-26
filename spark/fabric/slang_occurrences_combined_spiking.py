import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

from utils.config import load_config
from utils.paths import join_path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--mode", choices=["global", "subreddit"], default="subreddit")
    args = parser.parse_args()

    config = load_config(args.config)

    spark = (
        SparkSession.builder
        .appName("BuildSlangOccurrencesCombinedSpikingFabric")
        .getOrCreate()
    )

    out_dir = config["paths"]["out_dir"]

    if args.mode == "global":
        candidate_name = "slang_candidates_clean_ratio_spiking_combined_global_parquet"
        occurrence_name = "slang_occurrences_clean_ratio_spiking_combined_global_parquet"
        join_cols = ["token"]
    else:
        candidate_name = "slang_candidates_clean_ratio_spiking_combined_subreddit_parquet"
        occurrence_name = "slang_occurrences_clean_ratio_spiking_combined_subreddit_parquet"
        join_cols = ["subreddit", "token"]

    candidates_dir = join_path(out_dir, candidate_name)
    occurrences_dir = join_path(out_dir, occurrence_name)
    reddit_tokens_dir = join_path(out_dir, "reddit_tokens_subset_10pct_parquet")

    reddit_tokens = spark.read.parquet(reddit_tokens_dir)
    candidates = spark.read.parquet(candidates_dir)

    candidate_metadata_cols = [
        column_name
        for column_name in [
            "count_reddit_all_time",
            "total_tokens_all_time",
            "p_reddit_all_time",
            "count_clean",
            "total_clean_tokens",
            "p_clean",
            "clean_ratio_score",
            "has_monthly_spike",
            "max_spike_score_monthly",
            "num_spike_periods_monthly",
            "has_yearly_spike",
            "max_spike_score_yearly",
            "num_spike_periods_yearly",
            "spike_profile",
        ]
        if column_name in candidates.columns
    ]

    candidate_select_cols = [*join_cols, *candidate_metadata_cols]

    occurrences = (
        reddit_tokens
        .join(candidates.select(*candidate_select_cols).distinct(), on=join_cols, how="inner")
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
            lit("spiking_combined").alias("candidate_type"),
            lit(args.mode).alias("candidate_mode"),
            *candidate_metadata_cols,
        )
    )

    occurrences.write.mode("overwrite").parquet(occurrences_dir)

    print("wrote_occurrences={}".format(occurrences_dir))

    spark.stop()


if __name__ == "__main__":
    main()
