from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col, lit, sum as spark_sum

from utils.filter_tokens import filter_candidate_tokens
from utils.config import parse_args, load_config
from utils.paths import join_path

# -------------------------
# Parse config
# -------------------------
args = parse_args()
config = load_config(args.config)


def build_counts_and_totals(
    tokens_for_counts,
    tokens_for_totals,
    group_cols,
):
    """
    
    """
    # -------------------------
    # Count tokens by week / month / year / or all time
    # -------------------------
    token_counts = (
        tokens_for_counts
        .groupBy(*group_cols, "token")
        .agg(count("*").alias("count_reddit"))
    )

    # -------------------------
    # Count total tokens by timeframe (unfiltered)
    # -------------------------
    bucket_totals = (
        tokens_for_totals
        .groupBy(*group_cols)
        .agg(count("*").alias("total_tokens"))
    )

    return token_counts, bucket_totals


def join_counts_and_totals(
    token_counts,
    bucket_totals,
    group_cols,
):
    """
    
    """
    # -------------------------
    # Return token frequency / total tokens in timeframe
    # -- token count / total tokens
    # -------------------------
    return (
        token_counts
        .join(bucket_totals, group_cols)
        .withColumn("p_reddit", col("count_reddit") / col("total_tokens"))
    )


def rollup_to_global(
    token_counts,
    bucket_totals,
    time_cols,
):
    """
    
    """
    if time_cols:
        global_token_counts = (
            token_counts
            .groupBy(*(time_cols + ["token"]))
            .agg(spark_sum("count_reddit").alias("count_reddit"))
        )

        global_bucket_totals = (
            bucket_totals
            .groupBy(*time_cols)
            .agg(spark_sum("total_tokens").alias("total_tokens"))
        )

        global_stats = join_counts_and_totals(
            global_token_counts,
            global_bucket_totals,
            time_cols,
        )

        return global_stats.select(
            lit("reddit").alias("corpus"),
            *time_cols,
            "token",
            "count_reddit",
            "total_tokens",
            "p_reddit",
        )

    global_token_counts = (
        token_counts
        .groupBy("token")
        .agg(spark_sum("count_reddit").alias("count_reddit"))
    )

    global_total_tokens = bucket_totals.agg(
        spark_sum("total_tokens").alias("total_tokens")
    )

    global_stats = (
        global_token_counts
        .crossJoin(global_total_tokens)
        .withColumn("p_reddit", col("count_reddit") / col("total_tokens"))
    )

    return global_stats.select(
        lit("reddit").alias("corpus"),
        "token",
        "count_reddit",
        "total_tokens",
        "p_reddit",
    )


spark = (
    SparkSession.builder
    .appName("BuildRedditTokenCountsFabric")
    .getOrCreate()
)

# -------------------------
# Load reddit_tokens Parquet table
# -------------------------
out_dir = config["paths"]["out_dir"]

tokens_parquet_dir = join_path(out_dir, "reddit_tokens_parquet")

tokens = (
    spark.read.parquet(tokens_parquet_dir)
    .select("subreddit", "year", "month", "token")
    .persist(StorageLevel.DISK_ONLY)
)

# -------------------------
# Filter tokens for reasonable slang words
# -------------------------
filter_config = config.get("filter", {})

filtered = (
    filter_candidate_tokens(
        tokens,
        min_len=filter_config.get("min_len", 3),
        alphabetic_only=filter_config.get("alphabetic_only", False),
        allow_numbers=filter_config.get("allow_numbers", False),
        allow_hyphen=filter_config.get("allow_hyphen", True),
        allow_apostrophe=filter_config.get("allow_apostrophe", True),
        allow_underscore=filter_config.get("allow_underscore", False),
    )
    .persist(StorageLevel.DISK_ONLY)
)

# -------------------------
# Compute token probs by month / year / all time (by subreddit and globally)
# -------------------------
monthly_sub_counts, monthly_sub_totals = build_counts_and_totals(
    filtered,
    tokens,
    ["subreddit", "year", "month"]
)
monthly_sub_counts = monthly_sub_counts.persist(StorageLevel.DISK_ONLY)
monthly_sub_totals = monthly_sub_totals.persist(StorageLevel.DISK_ONLY)

yearly_sub_counts, yearly_sub_totals = build_counts_and_totals(
    filtered,
    tokens,
    ["subreddit", "year"]
)
yearly_sub_counts = yearly_sub_counts.persist(StorageLevel.DISK_ONLY)
yearly_sub_totals = yearly_sub_totals.persist(StorageLevel.DISK_ONLY)

all_time_sub_counts, all_time_sub_totals = build_counts_and_totals(
    filtered,
    tokens,
    ["subreddit"]
)
all_time_sub_counts = all_time_sub_counts.persist(StorageLevel.DISK_ONLY)
all_time_sub_totals = all_time_sub_totals.persist(StorageLevel.DISK_ONLY)

monthly_subreddit = join_counts_and_totals(
    monthly_sub_counts,
    monthly_sub_totals,
    ["subreddit", "year", "month"]
)

yearly_subreddit = join_counts_and_totals(
    yearly_sub_counts,
    yearly_sub_totals,
    ["subreddit", "year"]
)

all_time_subreddit = join_counts_and_totals(
    all_time_sub_counts,
    all_time_sub_totals,
    ["subreddit"]
)

monthly_global = rollup_to_global(
    monthly_sub_counts,
    monthly_sub_totals,
    ["year", "month"]
)

yearly_global = rollup_to_global(
    yearly_sub_counts,
    yearly_sub_totals,
    ["year"]
)

all_time_global = rollup_to_global(
    all_time_sub_counts,
    all_time_sub_totals,
    []
)

# -------------------------
# Write reddit_stats to Parquet tables
# -------------------------
monthly_subreddit.write.mode("overwrite").parquet(join_path(out_dir, "reddit_stats_monthly_subreddit_parquet"))
yearly_subreddit.write.mode("overwrite").parquet(join_path(out_dir, "reddit_stats_yearly_subreddit_parquet"))
all_time_subreddit.write.mode("overwrite").parquet(join_path(out_dir, "reddit_stats_all_time_subreddit_parquet"))
monthly_global.write.mode("overwrite").parquet(join_path(out_dir, "reddit_stats_monthly_global_parquet"))
yearly_global.write.mode("overwrite").parquet(join_path(out_dir, "reddit_stats_yearly_global_parquet"))
all_time_global.write.mode("overwrite").parquet(join_path(out_dir, "reddit_stats_all_time_global_parquet"))

spark.stop()
