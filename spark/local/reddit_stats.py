from pathlib import Path
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import count, col, lit

from filter_tokens import filter_candidate_tokens


def build_stats(
    tokens_for_counts: DataFrame,
    tokens_for_totals: DataFrame,
    group_cols: list[str],
) -> DataFrame:
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

    # -------------------------
    # Return token frequency / total tokens in timeframe
    # -- token count / total tokens
    # -------------------------
    return (
        token_counts
        .join(bucket_totals, group_cols)
        .withColumn("p_reddit", col("count_reddit") / col("total_tokens"))
    )


spark = (
    SparkSession.builder
    .appName("BuildRedditTokenCountsLocal")
    .master("local[*]")
    .getOrCreate()
)

# -------------------------
# Load reddit_tokens Parquet table
# -------------------------
out_dir = Path("/home/logan/projects/CS-8540-Project/spark/outputs")

tokens_parquet_dir = str(out_dir / "reddit_tokens_parquet")

tokens = spark.read.parquet(tokens_parquet_dir)
print(tokens.count())

# -------------------------
# Filter tokens for reasonable slang words
# -------------------------
filtered = filter_candidate_tokens(tokens, allow_numbers=False)

# -------------------------
# Compute token probs by month / year / all time (by subreddit and globally)
# -------------------------
monthly_subreddit = build_stats(
    filtered,
    tokens,
    ["subreddit", "year", "month"]
)

yearly_subreddit = build_stats(
    filtered,
    tokens,
    ["subreddit", "year"]
)

all_time_subreddit = build_stats(
    filtered,
    tokens,
    ["subreddit"]
)

monthly_global = build_stats(
    filtered.withColumn("corpus", lit("reddit")),
    tokens.withColumn("corpus", lit("reddit")),
    ["corpus", "year", "month"]
)

yearly_global = build_stats(
    filtered.withColumn("corpus", lit("reddit")),
    tokens.withColumn("corpus", lit("reddit")),
    ["corpus", "year"]
)

all_time_global = build_stats(
    filtered.withColumn("corpus", lit("reddit")),
    tokens.withColumn("corpus", lit("reddit")),
    ["corpus"]
)

# -------------------------
# Write reddit_stats to Parquet table / csv
# -------------------------
monthly_subreddit.write.mode("overwrite").parquet(str(out_dir / "reddit_stats_monthly_subreddit_parquet"))
yearly_subreddit.write.mode("overwrite").parquet(str(out_dir / "reddit_stats_yearly_subreddit_parquet"))
all_time_subreddit.write.mode("overwrite").parquet(str(out_dir / "reddit_stats_all_time_subreddit_parquet"))
monthly_global.write.mode("overwrite").parquet(str(out_dir / "reddit_stats_monthly_global_parquet"))
yearly_global.write.mode("overwrite").parquet(str(out_dir / "reddit_stats_yearly_global_parquet"))
all_time_global.write.mode("overwrite").parquet(str(out_dir / "reddit_stats_all_time_global_parquet"))

(
    monthly_subreddit
    .limit(100)
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", "true")
    .csv(str(out_dir / "reddit_stats_monthly_subreddit_csv"))
)
(
    yearly_subreddit
    .limit(100)
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", "true")
    .csv(str(out_dir / "reddit_stats_yearly_subreddit_csv"))
)
(
    all_time_subreddit
    .limit(100)
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", "true")
    .csv(str(out_dir / "reddit_stats_all_time_subreddit_csv"))
)
(
    monthly_global
    .limit(100)
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", "true")
    .csv(str(out_dir / "reddit_stats_monthly_global_csv"))
)
(
    yearly_global
    .limit(100)
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", "true")
    .csv(str(out_dir / "reddit_stats_yearly_global_csv"))
)
(
    all_time_global
    .limit(100)
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", "true")
    .csv(str(out_dir / "reddit_stats_all_time_global_csv"))
)

spark.stop()
