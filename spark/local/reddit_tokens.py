import re
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, udf
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    StringType,
    StructType,
    StructField,
)

# -------------------------
# Word-level Regex:
# - \b --> Start word boundary
# - [\w]+ --> One or more word characters
# - (?:[-'][\w]+)* --> Optionally capture repeated group: hyphenated or apostrophes (e.g., rock-and-roll, y'all'd've)
# - \b --> End word boundary
#
# Allows capturing words like:
# - lmaooo!! -> lmaooo
# - foo_bar -> foo_bar
# - don't -> don't
# - y'all -> y'all 
# - hi!hello -> hi, hello
# - rock-and-roll -> rock-and-roll
# - low-key -> low-key
# -------------------------
TOKEN_RE = re.compile(r"\b[\w]+(?:[-'][\w]+)*\b", re.UNICODE)

token_schema = ArrayType(
    # -- StructField(name, datatype, nullable)
    StructType([
        StructField("token", StringType(), False),
        StructField("surface", StringType(), False),
        StructField("token_start", IntegerType(), False),
        StructField("token_end", IntegerType(), False),
    ])
)


def tokenize_with_offsets(text: str) -> list[dict]:
    """
    
    """
    if text is None:
        return []
    
    out = []
    for m in TOKEN_RE.finditer(text):
        surface = m.group(0)
        out.append({
            "token": surface.lower(),
            "surface": surface,
            "token_start": m.start(),
            "token_end": m.end(),
        })
    return out

# -------------------------
# Tokenize User-Defined Function
# -- Runs tokenize_with_offsets returning type token_schema
# -------------------------
tokenize_udf = udf(tokenize_with_offsets, token_schema)

spark = (
    SparkSession.builder
    .appName("BuildRedditTokensLocal")
    .master("local[*]")
    .getOrCreate()
)

# -------------------------
# Load reddit_clean_posts Parquet table
# -------------------------
out_dir = Path("/home/logan/projects/CS-8540-Project/spark/outputs")

clean_posts_parquet_dir = str(out_dir / "reddit_clean_posts_parquet")
tokens_parquet_dir = str(out_dir / "reddit_tokens_parquet")
tokens_csv_dir = str(out_dir / "reddit_tokens_csv")

clean_posts = spark.read.parquet(clean_posts_parquet_dir)

# -------------------------
# Create tokens Spark DataFrame from reddit_clean_posts Parquet table
# -------------------------
tokens = (
    clean_posts
    .withColumn("token_struct", explode(tokenize_udf(col("body"))))
    .select(
        "post_id",
        "subreddit",
        "timestamp",
        "year",
        "month",
        "week_start_date",
        col("token_struct.token").alias("token"),
        col("token_struct.surface").alias("surface"),
        col("token_struct.token_start").alias("token_start"),
        col("token_struct.token_end").alias("token_end"),
    )
)

tokens.printSchema()
tokens.show(20, truncate=False)

print(tokens.count())

# -------------------------
# Write samples to CSV
# -------------------------
(
    tokens
    .limit(20)
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", "true")
    .csv(tokens_csv_dir)
)

# -------------------------
# Write Parquet sample (reddit_tokens)
# -------------------------
tokens.write.mode("overwrite").parquet(tokens_parquet_dir)

spark.stop()