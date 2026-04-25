import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

from utils.config import load_config
from utils.paths import join_path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--mode", choices=["global", "subreddit"], default="subreddit")
    parser.add_argument(
        "--candidate-type",
        choices=["clean_ratio", "spiking"],
        default="clean_ratio",
    )
    parser.add_argument(
        "--spike-time-grain",
        choices=["monthly", "yearly"],
        default="monthly",
    )
    args = parser.parse_args()

    config = load_config(args.config)

    spark = (
        SparkSession.builder
        .appName("BuildBertInputsFabric")
        .getOrCreate()
    )

    out_dir = config["paths"]["out_dir"]

    # -------------------------
    # Choose candidate input / BERT output paths
    # -------------------------
    # -- clean_ratio:
    #       Uses the broad clean-ratio slang candidate table.
    #
    # -- spiking:
    #       Uses the stricter clean-ratio + monthly/yearly spiking candidate
    #       table.
    # -------------------------
    if args.candidate_type == "clean_ratio":
        candidate_name = f"slang_candidates_clean_ratio_{args.mode}_parquet"
        bert_output_name = f"bert_inputs_clean_ratio_{args.mode}_parquet"
    else:
        candidate_name = (
            f"slang_candidates_clean_ratio_spiking_"
            f"{args.spike_time_grain}_{args.mode}_parquet"
        )
        bert_output_name = (
            f"bert_inputs_clean_ratio_spiking_"
            f"{args.spike_time_grain}_{args.mode}_parquet"
        )

    candidates_dir = join_path(out_dir, candidate_name)
    bert_output_dir = join_path(out_dir, bert_output_name)
    reddit_tokens_dir = join_path(out_dir, "reddit_tokens_parquet")
    clean_posts_dir = join_path(out_dir, "reddit_clean_posts_parquet")

    reddit_tokens = spark.read.parquet(reddit_tokens_dir)
    candidates = spark.read.parquet(candidates_dir)
    clean_posts = spark.read.parquet(clean_posts_dir)

    # -------------------------
    # Choose join keys
    # -------------------------
    # -- Subreddit candidates are subreddit-specific:
    #       join on [subreddit, token]
    #
    # -- Global candidates apply everywhere:
    #       join on [token]
    # -------------------------
    if args.mode == "subreddit":
        join_cols = ["subreddit", "token"]
    else:
        join_cols = ["token"]

    # -------------------------
    # Build BERT-ready token occurrence rows
    # -------------------------
    # -- One row per matched slang-candidate token occurrence.
    #
    # -- `full_text` is included by default because Ray/BERT will handle
    #    context windowing downstream.
    # -------------------------
    bert_inputs = (
        reddit_tokens
        .join(candidates.select(*join_cols).distinct(), on=join_cols, how="inner")
        .join(
            clean_posts.select("post_id", col("body").alias("full_text")),
            on="post_id",
            how="left",
        )
        .select(
            "post_id",
            "subreddit",
            "timestamp",
            "token",
            "token_start",
            "token_end",
            "full_text",
            lit(True).alias("is_slang_candidate"),
        )
    )

    bert_inputs.write.mode("overwrite").parquet(bert_output_dir)

    spark.stop()


if __name__ == "__main__":
    main()
