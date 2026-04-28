import argparse
import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from utils.config import load_config
from utils.paths import join_path


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument(
        "--table",
        default="slang_scores_bert_input_subreddit_parquet",
        help="Parquet table name under paths.out_dir.",
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--tokens-json",
        help='JSON list of tokens, for example: ["you", "your", "don\'t"]',
    )
    group.add_argument(
        "--tokens-file",
        help="Path to a UTF-8 text file with one token per line.",
    )
    parser.add_argument(
        "--subreddit",
        default=None,
        help="Optional subreddit filter.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=200,
        help="Maximum number of matching rows to print.",
    )
    return parser.parse_args()


def load_tokens(args):
    if args.tokens_json is not None:
        tokens = json.loads(args.tokens_json)
    else:
        with open(args.tokens_file, "r", encoding="utf-8") as f:
            tokens = [line.strip() for line in f]

    if not isinstance(tokens, list):
        raise ValueError("Token input must be a list of strings.")

    cleaned_tokens = []
    seen = set()
    for token in tokens:
        if not isinstance(token, str):
            raise ValueError("Each token must be a string.")
        if token == "":
            continue
        if token in seen:
            continue
        seen.add(token)
        cleaned_tokens.append(token)

    if not cleaned_tokens:
        raise ValueError("Provide at least one non-empty token.")

    return cleaned_tokens


def main():
    args = parse_args()
    config = load_config(args.config)
    tokens = load_tokens(args)

    spark = (
        SparkSession.builder
        .appName("LookupSlangScoresBertInput")
        .getOrCreate()
    )

    table_path = join_path(config["paths"]["out_dir"], args.table)
    scores = spark.read.parquet(table_path)

    filtered_scores = scores.filter(col("token").isin(tokens))
    if args.subreddit is not None:
        filtered_scores = filtered_scores.filter(col("subreddit") == args.subreddit)

    filtered_scores = filtered_scores.orderBy(
        col("token").asc(),
        col("slang_score").desc(),
        col("subreddit").asc(),
    )

    found_tokens = {
        row["token"]
        for row in filtered_scores.select("token").distinct().collect()
    }
    missing_tokens = [token for token in tokens if token not in found_tokens]

    print("table={}".format(args.table))
    if args.subreddit is not None:
        print("subreddit={}".format(args.subreddit))
    print("requested_tokens={}".format(len(tokens)))
    print("matched_tokens={}".format(len(found_tokens)))
    print("missing_tokens={}".format(json.dumps(missing_tokens)))

    filtered_scores.show(args.limit, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
