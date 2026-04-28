import argparse
import math

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    coalesce,
    col,
    countDistinct,
    exp,
    greatest,
    least,
    lit,
    log1p,
    max as spark_max,
)

from utils.config import load_config
from utils.paths import join_path


MIN_CLEAN_RATIO_SCORE = 3.0
MIN_SPIKE_SCORE = 2.3
DEFAULT_COMPONENT_SCALE = 0.4
DEFAULT_RELIABILITY_SATURATION_COUNT = 100


def sigmoid(expr):
    return lit(1.0) / (lit(1.0) + exp(-expr))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--mode", choices=["global", "subreddit"], default="subreddit")
    parser.add_argument(
        "--tokens-table",
        default="reddit_tokens_subset_10pct_parquet",
        help="Parquet table containing all token occurrences to score.",
    )
    parser.add_argument(
        "--output-table",
        default=None,
        help="Optional output parquet table name. Defaults to a mode-specific slang-score table.",
    )
    parser.add_argument("--clean-center", type=float, default=None)
    parser.add_argument("--clean-scale", type=float, default=DEFAULT_COMPONENT_SCALE)
    parser.add_argument("--spike-center", type=float, default=None)
    parser.add_argument("--spike-scale", type=float, default=DEFAULT_COMPONENT_SCALE)
    parser.add_argument(
        "--reliability-saturation-count",
        type=int,
        default=DEFAULT_RELIABILITY_SATURATION_COUNT,
        help="Count where reliability saturates to 1.0 via log1p(count) / log1p(saturation_count).",
    )
    args = parser.parse_args()

    if args.clean_scale <= 0:
        raise ValueError("--clean-scale must be > 0")
    if args.spike_scale <= 0:
        raise ValueError("--spike-scale must be > 0")
    if args.reliability_saturation_count <= 0:
        raise ValueError("--reliability-saturation-count must be > 0")

    config = load_config(args.config)
    params = config.get("params", {})

    clean_center = args.clean_center
    if clean_center is None:
        clean_center = params.get("min_clean_ratio_score", MIN_CLEAN_RATIO_SCORE)

    spike_center = args.spike_center
    if spike_center is None:
        spike_center = params.get("min_spike_score", MIN_SPIKE_SCORE)

    spark = (
        SparkSession.builder
        .appName("BuildSlangScoresFabric")
        .getOrCreate()
    )

    out_dir = config["paths"]["out_dir"]

    if args.mode == "global":
        clean_ratio_name = "clean_ratio_scores_global_parquet"
        monthly_spikes_name = "spike_scores_monthly_global_parquet"
        yearly_spikes_name = "spike_scores_yearly_global_parquet"
        output_name = "reddit_tokens_subset_10pct_slang_scores_global_parquet"
        score_key_cols = ["corpus", "token"]
        token_join_cols = ["token"]
    else:
        clean_ratio_name = "clean_ratio_scores_subreddit_parquet"
        monthly_spikes_name = "spike_scores_monthly_subreddit_parquet"
        yearly_spikes_name = "spike_scores_yearly_subreddit_parquet"
        output_name = "reddit_tokens_subset_10pct_slang_scores_subreddit_parquet"
        score_key_cols = ["subreddit", "token"]
        token_join_cols = ["subreddit", "token"]

    if args.output_table is not None:
        output_name = args.output_table

    clean_ratio_dir = join_path(out_dir, clean_ratio_name)
    monthly_spikes_dir = join_path(out_dir, monthly_spikes_name)
    yearly_spikes_dir = join_path(out_dir, yearly_spikes_name)
    tokens_dir = join_path(out_dir, args.tokens_table)
    output_dir = join_path(out_dir, output_name)

    tokens = spark.read.parquet(tokens_dir)
    clean_ratio_scores = spark.read.parquet(clean_ratio_dir)
    monthly_spike_scores = spark.read.parquet(monthly_spikes_dir)
    yearly_spike_scores = spark.read.parquet(yearly_spikes_dir)

    clean_ratio_features = (
        clean_ratio_scores
        .select(
            *score_key_cols,
            col("count_reddit").alias("count_reddit_all_time"),
            col("total_tokens").alias("total_tokens_all_time"),
            col("p_reddit").alias("p_reddit_all_time"),
            "count_clean",
            "total_clean_tokens",
            "p_clean",
            "clean_ratio_score",
        )
        .distinct()
    )

    monthly_spike_features = (
        monthly_spike_scores
        .groupBy(*score_key_cols)
        .agg(
            spark_max("spike_score").alias("max_spike_score_monthly"),
            countDistinct("period_id").alias("num_spike_periods_monthly"),
        )
    )

    yearly_spike_features = (
        yearly_spike_scores
        .groupBy(*score_key_cols)
        .agg(
            spark_max("spike_score").alias("max_spike_score_yearly"),
            countDistinct("period_id").alias("num_spike_periods_yearly"),
        )
    )

    reliability_denominator = math.log1p(args.reliability_saturation_count)

    token_scores = (
        clean_ratio_features
        .join(monthly_spike_features, on=score_key_cols, how="left")
        .join(yearly_spike_features, on=score_key_cols, how="left")
        .withColumn(
            "has_monthly_spike",
            col("max_spike_score_monthly").isNotNull(),
        )
        .withColumn(
            "has_yearly_spike",
            col("max_spike_score_yearly").isNotNull(),
        )
        .withColumn(
            "clean_component",
            sigmoid(
                (col("clean_ratio_score") - lit(clean_center))
                / lit(args.clean_scale)
            ),
        )
        .withColumn(
            "monthly_component",
            sigmoid(
                (
                    coalesce(col("max_spike_score_monthly"), lit(0.0))
                    - lit(spike_center)
                )
                / lit(args.spike_scale)
            ),
        )
        .withColumn(
            "yearly_component",
            sigmoid(
                (
                    coalesce(col("max_spike_score_yearly"), lit(0.0))
                    - lit(spike_center)
                )
                / lit(args.spike_scale)
            ),
        )
        .withColumn(
            "spike_boost",
            greatest(col("monthly_component"), col("yearly_component")),
        )
        .withColumn(
            "reliability",
            least(
                lit(1.0),
                log1p(col("count_reddit_all_time")) / lit(reliability_denominator),
            ),
        )
        .withColumn(
            "slang_score",
            col("reliability")
            * col("clean_component")
            * (lit(0.4) + lit(0.6) * col("spike_boost")),
        )
    )

    score_feature_cols = [
        column_name
        for column_name in [
            *score_key_cols,
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
            "clean_component",
            "monthly_component",
            "yearly_component",
            "spike_boost",
            "reliability",
            "slang_score",
        ]
        if column_name in token_scores.columns
    ]

    scored_tokens = (
        tokens
        .join(
            token_scores.select(*score_feature_cols).distinct(),
            on=token_join_cols,
            how="left",
        )
        .withColumn("has_monthly_spike", coalesce(col("has_monthly_spike"), lit(False)))
        .withColumn("has_yearly_spike", coalesce(col("has_yearly_spike"), lit(False)))
        .withColumn("clean_component", coalesce(col("clean_component"), lit(0.0)))
        .withColumn("monthly_component", coalesce(col("monthly_component"), lit(0.0)))
        .withColumn("yearly_component", coalesce(col("yearly_component"), lit(0.0)))
        .withColumn("spike_boost", coalesce(col("spike_boost"), lit(0.0)))
        .withColumn("reliability", coalesce(col("reliability"), lit(0.0)))
        .withColumn("slang_score", coalesce(col("slang_score"), lit(0.0)))
    )

    scored_tokens.write.mode("overwrite").parquet(output_dir)

    print("tokens_table={}".format(args.tokens_table))
    print("output_table={}".format(output_name))
    print("clean_center={}".format(clean_center))
    print("clean_scale={}".format(args.clean_scale))
    print("spike_center={}".format(spike_center))
    print("spike_scale={}".format(args.spike_scale))
    print(
        "reliability_saturation_count={}".format(
            args.reliability_saturation_count
        )
    )
    print("wrote_slang_scores={}".format(output_dir))

    spark.stop()


if __name__ == "__main__":
    main()
