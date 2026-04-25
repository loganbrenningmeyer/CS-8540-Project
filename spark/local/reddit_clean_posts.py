import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col,
    from_unixtime,
    to_timestamp,
    to_date,
    year,
    month,
    date_trunc,
    row_number,
    rand,
    ceil,
    date_format,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
)

LOCAL_POST_LIMIT = 1_000_000

reddit_raw_schema = StructType([
    StructField("id", StringType(), True),
    StructField("subreddit", StringType(), True),
    StructField("created_utc", StringType(), True),
    StructField("body", StringType(), True),
    StructField("score", IntegerType(), True),
])

spark = (
    SparkSession.builder
    .appName("LocalTest")
    .master("local[*]")
    .getOrCreate()
)

# -------------------------
# Read gz chunks
# -------------------------
data_dir = Path("/home/logan/data/chunks")

df = spark.read.json(str(data_dir / "*.gz"))

print(f"Partitions: {df.rdd.getNumPartitions()}")

# -------------------------
# Build clean reddit posts
# -------------------------
out_dir = Path("/home/logan/projects/CS-8540-Project/spark/outputs")

clean_posts_parquet_dir = str(out_dir / "reddit_clean_posts_parquet")
clean_posts_csv_dir = str(out_dir / "reddit_clean_posts_csv")

clean_posts = (
    df
    .select(
        col("id").alias("post_id"),
        col("subreddit"),
        col("created_utc").cast("long"),
        col("body"),
        col("score").cast("int"),   
    )
    # -- Filtering
    .filter(col("post_id").isNotNull())
    .filter(col("subreddit").isNotNull())
    .filter(col("created_utc").isNotNull())
    .filter(col("body").isNotNull())
    .filter(~col("body").isin("[deleted]", "[removed]"))    # no [deleted]/[removed] text
    # -- Time columns
    .withColumn("timestamp", to_timestamp(from_unixtime(col("created_utc")))) 
    .withColumn("year", year(col("timestamp")))
    .withColumn("month", month(col("timestamp")))
    .withColumn("week_start_date", to_date(date_trunc("week", col("timestamp"))))
    # -- Final schema
    .select(
        "post_id",
        "subreddit",
        "timestamp",
        "year",
        "month",
        "week_start_date",
        "body",
        "score",
    )
)

print(f"Clean Posts Count: {clean_posts.count()}")

# -------------------------
# Sample posts by week randomly
# -------------------------
clean_posts_for_sample = clean_posts.withColumn(
    "week_start_key",
    date_format(col("week_start_date"), "yyyy-MM-dd")
)

week_counts = clean_posts_for_sample.groupBy("week_start_key").count().collect()
num_weeks = len(week_counts)
posts_per_week = max(1, -(-LOCAL_POST_LIMIT // num_weeks))

fractions = {
    row["week_start_key"]: min(1.0, posts_per_week / row["count"])
    for row in week_counts
}

clean_posts_sample = clean_posts_for_sample.sampleBy(
    "week_start_key",
    fractions=fractions,
    seed=8540,
).drop("week_start_key")

weeks = clean_posts_sample.select("week_start_date").distinct()

# Write weeks to CSV
(
weeks
    .orderBy("week_start_date")
    .coalesce(1)
    .write.mode("overwrite")
    .option("header", True)
    .csv(str(out_dir / "weeks"))
)

# Write small sample to CSV
(
    clean_posts_sample
    .limit(100)
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", "true")
    .csv(clean_posts_csv_dir)
)

# -------------------------
# Write Parquet sample (reddit_clean_posts)
# -------------------------
clean_posts_sample.limit(LOCAL_POST_LIMIT).write.mode("overwrite").parquet(clean_posts_parquet_dir)

spark.stop()
