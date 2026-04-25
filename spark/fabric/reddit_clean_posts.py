from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_unixtime,
    to_timestamp,
    to_date,
    year,
    month,
    expr,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
)

from utils.config import parse_args, load_config
from utils.paths import join_path

# -------------------------
# Parse config
# -------------------------
args = parse_args()
config = load_config(args.config)


reddit_raw_schema = StructType([
    StructField("id", StringType(), True),
    StructField("subreddit", StringType(), True),
    StructField("created_utc", StringType(), True),
    StructField("body", StringType(), True),
    StructField("score", IntegerType(), True),
])

spark = (
    SparkSession.builder
    .appName("BuildRedditCleanPostsFabric")
    .getOrCreate()
)

# -------------------------
# Read gz chunks
# -------------------------
chunks_dir = config["paths"]["reddit_chunks"]

df = spark.read.schema(reddit_raw_schema).json(join_path(chunks_dir, "*.gz"))

# -------------------------
# Build clean reddit posts
# -------------------------
out_dir = config["paths"]["out_dir"]

clean_posts_parquet_dir = join_path(out_dir, "reddit_clean_posts_parquet")

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
    # -- Spark 2.2 does not have pyspark.sql.functions.date_trunc.
    #    Compute Monday week start with SQL functions that are available on
    #    older Spark versions:
    #       dayofweek: Sunday=1, Monday=2, ..., Saturday=7
    #       pmod(dayofweek + 5, 7): days since Monday
    .withColumn(
        "week_start_date",
        expr("date_sub(to_date(timestamp), pmod(dayofweek(timestamp) + 5, 7))"),
    )
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

# -------------------------
# Write Parquet sample (reddit_clean_posts)
# -------------------------
clean_posts.write.mode("overwrite").parquet(clean_posts_parquet_dir)

spark.stop()
