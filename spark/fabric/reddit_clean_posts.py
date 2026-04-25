from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_unixtime,
    floor,
    lit,
    year,
    month,
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

# -------------------------
# Week start calculation constants
# -------------------------
# -- Spark 2.2 on Fabric is missing newer date helpers like date_trunc and
#    dayofweek. Compute Monday week start directly from Unix seconds instead.
#
# -- Unix epoch 0 is Thursday 1970-01-01.
# -- Monday 1970-01-05 is 345600 seconds after epoch.
# -- For Reddit timestamps, this gives the Monday 00:00:00 UTC bucket:
#       floor((created_utc - monday_offset) / seconds_per_week)
#       * seconds_per_week + monday_offset
# -------------------------
SECONDS_PER_WEEK = 7 * 24 * 60 * 60
MONDAY_EPOCH_OFFSET = 4 * 24 * 60 * 60

monday_start_epoch = (
    floor(
        (col("created_utc") - lit(MONDAY_EPOCH_OFFSET))
        / lit(SECONDS_PER_WEEK)
    )
    * lit(SECONDS_PER_WEEK)
    + lit(MONDAY_EPOCH_OFFSET)
)

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
    .withColumn("timestamp", from_unixtime(col("created_utc")).cast("timestamp"))
    .withColumn("year", year(col("timestamp")))
    .withColumn("month", month(col("timestamp")))
    .withColumn("week_start_date", from_unixtime(monday_start_epoch).cast("date"))
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
