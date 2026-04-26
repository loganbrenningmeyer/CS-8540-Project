import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

from utils.config import load_config
from utils.paths import join_path


def select_spike_cols(df, prefix):
    cols = [
        "baseline_count_reddit",
        "baseline_total_tokens",
        "baseline_periods",
        "p_baseline",
        "p_current_smoothed",
        "p_baseline_smoothed",
        "spike_score",
    ]
    select_exprs = []
    for name in cols:
        if name in df.columns:
            select_exprs.append(col(name).alias(f"{prefix}_{name}"))
    return select_exprs


def ensure_columns(df, ordered_cols):
    for column_name in ordered_cols:
        if column_name not in df.columns:
            df = df.withColumn(column_name, lit(None))
    return df.select(*ordered_cols)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--mode", choices=["global", "subreddit"], default="subreddit")
    args = parser.parse_args()

    config = load_config(args.config)

    spark = (
        SparkSession.builder
        .appName("ExportCombinedSpikingPlottingCsv")
        .getOrCreate()
    )

    out_dir = config["paths"]["out_dir"]

    if args.mode == "global":
        key_cols = ["corpus", "token"]
        combined_name = "slang_candidates_clean_ratio_spiking_combined_global_parquet"
        monthly_stats_name = "reddit_stats_monthly_global_parquet"
        yearly_stats_name = "reddit_stats_yearly_global_parquet"
        monthly_spikes_name = "spike_scores_monthly_global_parquet"
        yearly_spikes_name = "spike_scores_yearly_global_parquet"
        csv_name = "combined_spiking_candidate_plotting_global_csv"
    else:
        key_cols = ["subreddit", "token"]
        combined_name = "slang_candidates_clean_ratio_spiking_combined_subreddit_parquet"
        monthly_stats_name = "reddit_stats_monthly_subreddit_parquet"
        yearly_stats_name = "reddit_stats_yearly_subreddit_parquet"
        monthly_spikes_name = "spike_scores_monthly_subreddit_parquet"
        yearly_spikes_name = "spike_scores_yearly_subreddit_parquet"
        csv_name = "combined_spiking_candidate_plotting_subreddit_csv"

    combined_dir = join_path(out_dir, combined_name)
    monthly_stats_dir = join_path(out_dir, monthly_stats_name)
    yearly_stats_dir = join_path(out_dir, yearly_stats_name)
    monthly_spikes_dir = join_path(out_dir, monthly_spikes_name)
    yearly_spikes_dir = join_path(out_dir, yearly_spikes_name)
    csv_dir = join_path(out_dir, csv_name)

    candidates = spark.read.parquet(combined_dir)
    monthly_stats = spark.read.parquet(monthly_stats_dir)
    yearly_stats = spark.read.parquet(yearly_stats_dir)
    monthly_spikes = spark.read.parquet(monthly_spikes_dir)
    yearly_spikes = spark.read.parquet(yearly_spikes_dir)

    monthly_spike_output_cols = [
        f"monthly_{name}"
        for name in [
            "baseline_count_reddit",
            "baseline_total_tokens",
            "baseline_periods",
            "p_baseline",
            "p_current_smoothed",
            "p_baseline_smoothed",
            "spike_score",
        ]
    ]

    yearly_spike_output_cols = [
        f"yearly_{name}"
        for name in [
            "baseline_count_reddit",
            "baseline_total_tokens",
            "baseline_periods",
            "p_baseline",
            "p_current_smoothed",
            "p_baseline_smoothed",
            "spike_score",
        ]
    ]

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

    candidates_small = candidates.select(*key_cols, *candidate_metadata_cols).distinct()

    monthly_plot = (
        monthly_stats
        .join(candidates_small, on=key_cols, how="inner")
        .join(
            monthly_spikes.select(
                *key_cols,
                "year",
                "month",
                "period_id",
                *select_spike_cols(monthly_spikes, "monthly"),
            ),
            on=[*key_cols, "year", "month"],
            how="left",
        )
        .withColumn("time_grain", lit("monthly"))
        .withColumn(
            "period_id",
            col("period_id"),
        )
        .withColumn("month", col("month"))
        .select(
            *key_cols,
            "time_grain",
            "year",
            "month",
            "period_id",
            "count_reddit",
            "total_tokens",
            "p_reddit",
            *candidate_metadata_cols,
            *[
                f"monthly_{name}"
                for name in [
                    "baseline_count_reddit",
                    "baseline_total_tokens",
                    "baseline_periods",
                    "p_baseline",
                    "p_current_smoothed",
                    "p_baseline_smoothed",
                    "spike_score",
                ]
                if f"monthly_{name}" in monthly_spikes.select(
                    *select_spike_cols(monthly_spikes, "monthly")
                ).columns
            ],
        )
    )

    yearly_plot = (
        yearly_stats
        .join(candidates_small, on=key_cols, how="inner")
        .join(
            yearly_spikes.select(
                *key_cols,
                "year",
                "period_id",
                *select_spike_cols(yearly_spikes, "yearly"),
            ),
            on=[*key_cols, "year"],
            how="left",
        )
        .withColumn("time_grain", lit("yearly"))
        .withColumn("month", lit(None).cast("int"))
        .select(
            *key_cols,
            "time_grain",
            "year",
            "month",
            "period_id",
            "count_reddit",
            "total_tokens",
            "p_reddit",
            *candidate_metadata_cols,
            *[
                f"yearly_{name}"
                for name in [
                    "baseline_count_reddit",
                    "baseline_total_tokens",
                    "baseline_periods",
                    "p_baseline",
                    "p_current_smoothed",
                    "p_baseline_smoothed",
                    "spike_score",
                ]
                if f"yearly_{name}" in yearly_spikes.select(
                    *select_spike_cols(yearly_spikes, "yearly")
                ).columns
            ],
        )
    )

    output_cols = [
        *key_cols,
        "time_grain",
        "year",
        "month",
        "period_id",
        "count_reddit",
        "total_tokens",
        "p_reddit",
        *candidate_metadata_cols,
        *monthly_spike_output_cols,
        *yearly_spike_output_cols,
    ]

    monthly_plot = ensure_columns(monthly_plot, output_cols)
    yearly_plot = ensure_columns(yearly_plot, output_cols)

    plotting_rows = monthly_plot.union(yearly_plot)

    (
        plotting_rows
        .orderBy(*key_cols, "time_grain", "year", "month")
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(csv_dir)
    )

    print("wrote_plotting_csv={}".format(csv_dir))

    spark.stop()


if __name__ == "__main__":
    main()
