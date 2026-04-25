import argparse
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, lit, log


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["global", "subreddit"], default="subreddit")
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("BuildCleanRatioScoresLocal")
        .master("local[*]")
        .getOrCreate()
    )

    # -------------------------
    # Load reddit / clean stats Parquet tables
    # -------------------------
    out_dir = Path("/home/logan/projects/CS-8540-Project/spark/outputs")

    clean_stats_parquet_dir = str(out_dir / "clean_stats_parquet")

    if args.mode == "global":
        reddit_stats_all_time_parquet_dir = str(out_dir / "reddit_stats_all_time_global_parquet")
        clean_ratio_scores_parquet_dir = str(out_dir / "clean_ratio_scores_global_parquet")
        clean_ratio_scores_csv_dir = str(out_dir / "clean_ratio_scores_global_csv")
    else:
        reddit_stats_all_time_parquet_dir = str(out_dir / "reddit_stats_all_time_subreddit_parquet")
        clean_ratio_scores_parquet_dir = str(out_dir / "clean_ratio_scores_subreddit_parquet")
        clean_ratio_scores_csv_dir = str(out_dir / "clean_ratio_scores_subreddit_csv")

    reddit_stats_all_time = spark.read.parquet(reddit_stats_all_time_parquet_dir)
    clean_stats = spark.read.parquet(clean_stats_parquet_dir)

    # -------------------------
    # Compute clean ratio score
    # -- Set p_clean for non-reddit tokens to very low value for stability
    # => s_\text{ratio} = \log \frac{p_\text{reddit} + \epsilon}{p_\text{clean} + \epsilon}
    # -------------------------
    eps = 1e-12

    candidates = (
        reddit_stats_all_time
        .join(clean_stats, on="token", how="left")
        .withColumn("p_clean", coalesce(col("p_clean"), lit(eps)))
        .withColumn("clean_ratio_score", log(col("p_reddit") / col("p_clean")))
        .orderBy(col("clean_ratio_score").desc())
    )

    candidates.select(
        "token",
        "clean_ratio_score",
        "count_reddit",
        "p_reddit",
        "p_clean"
    ).show(50, truncate=False)


    # -------------------------
    # Save clean_ratio_scores to Parquet / csv
    # -------------------------
    candidates.write.mode("overwrite").parquet(clean_ratio_scores_parquet_dir)

    (
        candidates
        .orderBy(col("clean_ratio_score").desc())
        .limit(100)
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(clean_ratio_scores_csv_dir)
    )

    spark.stop()


if __name__ == "__main__":
    main()
