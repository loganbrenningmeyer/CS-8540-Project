import argparse

from pyspark.sql import SparkSession

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
        .appName("ExportSlangCandidateTokensCsv")
        .getOrCreate()
    )

    out_dir = config["paths"]["out_dir"]

    if args.candidate_type == "clean_ratio":
        candidate_name = f"slang_candidates_clean_ratio_{args.mode}_parquet"
        csv_name = f"slang_candidate_tokens_clean_ratio_{args.mode}_csv"
    else:
        candidate_name = (
            f"slang_candidates_clean_ratio_spiking_"
            f"{args.spike_time_grain}_{args.mode}_parquet"
        )
        csv_name = (
            f"slang_candidate_tokens_clean_ratio_spiking_"
            f"{args.spike_time_grain}_{args.mode}_csv"
        )

    candidates_dir = join_path(out_dir, candidate_name)
    csv_dir = join_path(out_dir, csv_name)

    (
        spark.read.parquet(candidates_dir)
        .select("token")
        .distinct()
        .orderBy("token")
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(csv_dir)
    )

    print("wrote_csv={}".format(csv_dir))

    spark.stop()


if __name__ == "__main__":
    main()
