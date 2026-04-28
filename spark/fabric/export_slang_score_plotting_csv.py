import argparse

from pyspark.sql import SparkSession

from utils.config import load_config
from utils.paths import join_path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--mode", choices=["global", "subreddit"], default="subreddit")
    parser.add_argument(
        "--scores-table",
        default=None,
        help="Token-level slang-score parquet table. Defaults to the mode-specific slang score BERT-input table.",
    )
    parser.add_argument(
        "--csv-table",
        default=None,
        help="Optional output CSV directory name. Defaults to a mode-specific slang score plotting CSV.",
    )
    args = parser.parse_args()

    config = load_config(args.config)

    spark = (
        SparkSession.builder
        .appName("ExportSlangScorePlottingCsv")
        .getOrCreate()
    )

    out_dir = config["paths"]["out_dir"]

    if args.mode == "global":
        scores_name = "slang_scores_bert_input_global_parquet"
        csv_name = "slang_score_plotting_global_csv"
    else:
        scores_name = "slang_scores_bert_input_subreddit_parquet"
        csv_name = "slang_score_plotting_subreddit_csv"

    if args.scores_table is not None:
        scores_name = args.scores_table

    if args.csv_table is not None:
        csv_name = args.csv_table

    scores_dir = join_path(out_dir, scores_name)
    csv_dir = join_path(out_dir, csv_name)

    scores = spark.read.parquet(scores_dir)

    plotting_rows = scores.select("slang_score")

    (
        plotting_rows
        .orderBy("slang_score")
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(csv_dir)
    )

    print("scores_table={}".format(scores_name))
    print("csv_table={}".format(csv_name))
    print("wrote_plotting_csv={}".format(csv_dir))

    spark.stop()


if __name__ == "__main__":
    main()
