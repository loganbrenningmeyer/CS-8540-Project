import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    concat_ws,
    lpad,
    lit,
    log,
    coalesce,
    sum as spark_sum,
    when,
)
from pyspark.sql.window import Window

from utils.config import load_config
from utils.paths import join_path


# -------------------------
# Spike configuration defaults
# -------------------------
# -- Monthly spikes are meant to catch broader emergence/adoption patterns
#    with less week-to-week noise.
#
# -- Yearly spikes are meant to catch larger era-level changes across the full
#    corpus history.
# -------------------------
N_WINDOW_MONTHS = 6
N_WINDOW_YEARS = 3

MIN_CANDIDATE_TOKEN_COUNT = 100
MIN_CANDIDATE_CLEAN_RATIO_SCORE = 0.0
MIN_BASELINE_PERIODS = 3
MIN_PERIOD_COUNT = 5
MIN_BASELINE_TOTAL_TOKENS = 10_000
SMOOTHING_ALPHA = 1.0


def add_period_id(df, time_grain):
    """
    Add a stable string period_id for counting and output.

    monthly:
        period_id = yyyy-MM

    yearly:
        period_id = yyyy
    """
    if time_grain == "monthly":
        return df.withColumn(
            "period_id",
            concat_ws(
                "-",
                col("year").cast("string"),
                lpad(col("month").cast("string"), 2, "0"),
            ),
        )

    return df.withColumn("period_id", col("year").cast("string"))


def build_spike_scores(
    stats,
    candidate_tokens,
    candidate_key_cols,
    entity_cols,
    time_cols,
    time_grain,
    n_window_periods,
    min_baseline_periods,
    min_period_count,
    min_baseline_total_tokens,
    smoothing_alpha,
):
    """
    Compute smoothed spike scores for one time grain.

    The input stats table must already be grouped by either:
        subreddit + time columns + token
    or:
        corpus + time columns + token

    The output keeps only rows where the token appears in the current period
    with enough support and the previous-period baseline has enough evidence.
    """
    stats = add_period_id(stats, time_grain)

    token_period_cols = [*candidate_key_cols, *time_cols]
    bucket_cols = [*entity_cols, *time_cols]

    # -------------------------
    # Restrict period stats to broad candidate tokens
    # -------------------------
    period_candidates = stats.join(
        candidate_tokens.select(*candidate_key_cols),
        on=candidate_key_cols,
        how="inner",
    )

    # -------------------------
    # Create dense candidate-token x period grid
    # -------------------------
    # -- This lets the rolling baseline include periods where a token did not
    #    appear. Those periods should contribute:
    #       count_reddit = 0
    #       total_tokens = real subreddit/corpus-period total
    # -------------------------
    periods = stats.select(*time_cols).distinct()
    grid_entities = candidate_tokens.select(*candidate_key_cols).distinct()
    grid = grid_entities.crossJoin(periods)

    # -------------------------
    # Bucket totals by subreddit/corpus period
    # -------------------------
    # -- Pull total_tokens from the full stats table, not from candidate-token
    #    rows. Otherwise absent-token periods would incorrectly get
    #    total_tokens = 0.
    # -------------------------
    bucket_totals = (
        stats
        .select(
            *bucket_cols,
            col("total_tokens").alias("bucket_total_tokens"),
        )
        .dropDuplicates(bucket_cols)
    )

    dense = (
        grid
        .join(period_candidates, on=token_period_cols, how="left")
        .join(candidate_tokens, on=candidate_key_cols, how="left")
        .join(bucket_totals, on=bucket_cols, how="left")
        .withColumn("count_reddit", coalesce(col("count_reddit"), lit(0)))
        .withColumn("total_tokens", coalesce(col("bucket_total_tokens"), lit(0)))
        .withColumn(
            "p_reddit",
            when(
                col("total_tokens") > 0,
                col("count_reddit") / col("total_tokens"),
            ).otherwise(lit(0.0)),
        )
        .drop("bucket_total_tokens")
    )

    dense = add_period_id(dense, time_grain)

    # -------------------------
    # Rolling baseline over previous periods
    # -------------------------
    # -- Monthly default: previous 6 months
    # -- Yearly default: previous 3 years
    #
    # -- p_baseline = sum(previous token count) / sum(previous total tokens)
    # -------------------------
    window = (
        Window
        .partitionBy(*candidate_key_cols)
        .orderBy(*time_cols)
        .rowsBetween(-n_window_periods, -1)
    )

    dense_with_baseline = (
        dense
        .withColumn("baseline_count_reddit", spark_sum("count_reddit").over(window))
        .withColumn("baseline_total_tokens", spark_sum("total_tokens").over(window))
        .withColumn(
            "baseline_periods",
            spark_sum(when(col("total_tokens") > 0, lit(1)).otherwise(lit(0))).over(window),
        )
        .withColumn(
            "p_baseline",
            when(
                col("baseline_total_tokens") > 0,
                col("baseline_count_reddit") / col("baseline_total_tokens"),
            ).otherwise(lit(0.0)),
        )
    )

    # -------------------------
    # Compute smoothed spike score
    # -------------------------
    # -- Additive smoothing prevents zero-baseline first mentions from
    #    producing infinite scores.
    #
    # => p_current_smoothed = (count_reddit + alpha) / (total_tokens + alpha)
    # => p_baseline_smoothed = (baseline_count_reddit + alpha) / (baseline_total_tokens + alpha)
    # => spike_score = log(p_current_smoothed / p_baseline_smoothed)
    # -------------------------
    alpha = lit(smoothing_alpha)

    return (
        dense_with_baseline
        .filter(col("baseline_periods") >= min_baseline_periods)
        .filter(col("count_reddit") >= min_period_count)
        .filter(col("baseline_total_tokens") >= min_baseline_total_tokens)
        .withColumn(
            "p_current_smoothed",
            (col("count_reddit") + alpha) / (col("total_tokens") + alpha),
        )
        .withColumn(
            "p_baseline_smoothed",
            (col("baseline_count_reddit") + alpha)
            / (col("baseline_total_tokens") + alpha),
        )
        .withColumn(
            "spike_score",
            log(col("p_current_smoothed") / col("p_baseline_smoothed")),
        )
        .withColumn("time_grain", lit(time_grain))
    )


def write_spike_scores(
    spike_scores,
    parquet_dir,
):
    spike_scores.write.mode("overwrite").parquet(parquet_dir)

    spike_scores.orderBy(col("spike_score").desc()).show(50, truncate=False)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--mode", choices=["global", "subreddit"], default="subreddit")
    parser.add_argument(
        "--time-grain",
        choices=["monthly", "yearly", "both"],
        default="both",
    )
    parser.add_argument("--min-token-count", type=int, default=None)
    parser.add_argument(
        "--min-clean-ratio-score",
        type=float,
        default=None,
    )
    parser.add_argument("--min-baseline-periods", type=int, default=None)
    parser.add_argument("--min-period-count", type=int, default=None)
    parser.add_argument(
        "--min-baseline-total-tokens",
        type=int,
        default=None,
    )
    parser.add_argument("--monthly-window-periods", type=int, default=None)
    parser.add_argument("--yearly-window-periods", type=int, default=None)
    parser.add_argument("--smoothing-alpha", type=float, default=None)
    args = parser.parse_args()

    config = load_config(args.config)
    params = config.get("params", {})

    min_token_count = args.min_token_count
    if min_token_count is None:
        min_token_count = params.get("min_token_count", MIN_CANDIDATE_TOKEN_COUNT)

    min_clean_ratio_score = args.min_clean_ratio_score
    if min_clean_ratio_score is None:
        min_clean_ratio_score = params.get(
            "spike_min_clean_ratio_score",
            MIN_CANDIDATE_CLEAN_RATIO_SCORE,
        )

    min_baseline_periods = args.min_baseline_periods
    if min_baseline_periods is None:
        min_baseline_periods = params.get("min_baseline_periods", MIN_BASELINE_PERIODS)

    min_period_count = args.min_period_count
    if min_period_count is None:
        min_period_count = params.get("min_period_count", MIN_PERIOD_COUNT)

    min_baseline_total_tokens = args.min_baseline_total_tokens
    if min_baseline_total_tokens is None:
        min_baseline_total_tokens = params.get(
            "min_baseline_total_tokens",
            MIN_BASELINE_TOTAL_TOKENS,
        )

    monthly_window_periods = args.monthly_window_periods
    if monthly_window_periods is None:
        monthly_window_periods = params.get("monthly_window_periods", N_WINDOW_MONTHS)

    yearly_window_periods = args.yearly_window_periods
    if yearly_window_periods is None:
        yearly_window_periods = params.get("yearly_window_periods", N_WINDOW_YEARS)

    smoothing_alpha = args.smoothing_alpha
    if smoothing_alpha is None:
        smoothing_alpha = params.get("smoothing_alpha", SMOOTHING_ALPHA)

    spark = (
        SparkSession.builder
        .appName("BuildSpikeScoresFabric")
        .getOrCreate()
    )

    out_dir = config["paths"]["out_dir"]

    # -------------------------
    # Choose global vs subreddit inputs / outputs
    # -------------------------
    if args.mode == "global":
        clean_ratio_scores_dir = join_path(out_dir, "clean_ratio_scores_global_parquet")
        monthly_stats_dir = join_path(out_dir, "reddit_stats_monthly_global_parquet")
        yearly_stats_dir = join_path(out_dir, "reddit_stats_yearly_global_parquet")
        entity_cols = ["corpus"]
        candidate_key_cols = ["corpus", "token"]
        output_suffix = "global"
    else:
        clean_ratio_scores_dir = join_path(out_dir, "clean_ratio_scores_subreddit_parquet")
        monthly_stats_dir = join_path(out_dir, "reddit_stats_monthly_subreddit_parquet")
        yearly_stats_dir = join_path(out_dir, "reddit_stats_yearly_subreddit_parquet")
        entity_cols = ["subreddit"]
        candidate_key_cols = ["subreddit", "token"]
        output_suffix = "subreddit"

    clean_ratio_scores = spark.read.parquet(clean_ratio_scores_dir)

    # -------------------------
    # Broad candidate token filter
    # -------------------------
    # -- Keep this broad: spike_scores.py computes spike evidence, while
    #    slang_candidates.py makes the final slang decision.
    #
    # -- clean_ratio_score >= 0 means the token is at least as common in
    #    Reddit as in the clean Wikipedia corpus.
    # -------------------------
    candidate_tokens = (
        clean_ratio_scores
        .filter(col("count_reddit") >= min_token_count)
        .filter(col("clean_ratio_score") >= min_clean_ratio_score)
        .select(
            *candidate_key_cols,
            col("count_reddit").alias("count_reddit_all_time"),
            col("p_reddit").alias("p_reddit_all_time"),
            "p_clean",
            "clean_ratio_score",
        )
        .distinct()
    )

    if args.time_grain in ("monthly", "both"):
        monthly_stats = spark.read.parquet(monthly_stats_dir)
        monthly_spike_scores = build_spike_scores(
            stats=monthly_stats,
            candidate_tokens=candidate_tokens,
            candidate_key_cols=candidate_key_cols,
            entity_cols=entity_cols,
            time_cols=["year", "month"],
            time_grain="monthly",
            n_window_periods=monthly_window_periods,
            min_baseline_periods=min_baseline_periods,
            min_period_count=min_period_count,
            min_baseline_total_tokens=min_baseline_total_tokens,
            smoothing_alpha=smoothing_alpha,
        )

        write_spike_scores(
            monthly_spike_scores,
            join_path(out_dir, f"spike_scores_monthly_{output_suffix}_parquet"),
        )

    if args.time_grain in ("yearly", "both"):
        yearly_stats = spark.read.parquet(yearly_stats_dir)
        yearly_spike_scores = build_spike_scores(
            stats=yearly_stats,
            candidate_tokens=candidate_tokens,
            candidate_key_cols=candidate_key_cols,
            entity_cols=entity_cols,
            time_cols=["year"],
            time_grain="yearly",
            n_window_periods=yearly_window_periods,
            min_baseline_periods=min_baseline_periods,
            min_period_count=min_period_count,
            min_baseline_total_tokens=min_baseline_total_tokens,
            smoothing_alpha=smoothing_alpha,
        )

        write_spike_scores(
            yearly_spike_scores,
            join_path(out_dir, f"spike_scores_yearly_{output_suffix}_parquet"),
        )

    spark.stop()


if __name__ == "__main__":
    main()
