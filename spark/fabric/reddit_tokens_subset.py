import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, lpad

from utils.config import load_config
from utils.paths import join_path


def fraction_label(fraction):
    """
    Convert a sampling fraction like 0.10 to a stable path label like 10pct.
    """
    pct = fraction * 100.0
    if pct.is_integer():
        return "{}pct".format(int(pct))

    return "{}pct".format(str(pct).replace(".", "_"))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--fraction", type=float, required=True)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    if args.fraction <= 0.0 or args.fraction > 1.0:
        raise ValueError("--fraction must be in the interval (0, 1].")

    config = load_config(args.config)

    spark = (
        SparkSession.builder
        .appName("BuildRedditTokensSubsetFabric")
        .getOrCreate()
    )

    out_dir = config["paths"]["out_dir"]

    tokens_parquet_dir = join_path(out_dir, "reddit_tokens_parquet")
    subset_name = "reddit_tokens_subset_{}_parquet".format(
        fraction_label(args.fraction)
    )
    subset_parquet_dir = join_path(out_dir, subset_name)

    tokens = spark.read.parquet(tokens_parquet_dir)

    # -------------------------
    # Sample distinct posts within each year/month bucket.
    #
    # Because the sample is taken over distinct post ids rather than token
    # rows, selected posts keep all of their token occurrences in the subset.
    #
    # Use Spark's stratified sampling over a compact year-month key so each
    # month is sampled at the requested fraction without sorting entire buckets.
    # -------------------------
    distinct_posts = (
        tokens
        .select("post_id", "year", "month")
        .dropDuplicates()
        .withColumn(
            "year_month_key",
            concat_ws(
                "-",
                col("year").cast("string"),
                lpad(col("month").cast("string"), 2, "0"),
            ),
        )
    )

    strata = [
        row["year_month_key"]
        for row in distinct_posts.select("year_month_key").distinct().collect()
    ]
    fractions = dict((stratum, args.fraction) for stratum in strata)

    sampled_posts = (
        distinct_posts
        .sampleBy("year_month_key", fractions, args.seed)
        .select("post_id")
    )

    subset_tokens = tokens.join(sampled_posts, on="post_id", how="inner")

    subset_tokens.write.mode("overwrite").parquet(subset_parquet_dir)

    print("wrote_subset={}".format(subset_parquet_dir))

    spark.stop()


if __name__ == "__main__":
    main()
