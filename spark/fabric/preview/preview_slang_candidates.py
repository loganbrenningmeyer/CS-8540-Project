from preview_utils import base_parser, get_spark, output_path, show_table


def main():
    parser = base_parser("Preview slang candidate tables.")
    parser.add_argument("--mode", choices=["subreddit", "global"], default="subreddit")
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
    parser.add_argument(
        "--sort",
        choices=["ratio", "spike", "count"],
        default="ratio",
    )
    args = parser.parse_args()

    spark = get_spark("PreviewSlangCandidates")

    if args.candidate_type == "spiking":
        table_name = (
            "slang_candidates_clean_ratio_spiking_"
            f"{args.spike_time_grain}_{args.mode}_parquet"
        )
    else:
        table_name = f"slang_candidates_clean_ratio_{args.mode}_parquet"

    path = output_path(args.config, table_name)

    if args.sort == "spike":
        sort_cols = [("max_spike_score", True), ("clean_ratio_score", True)]
    elif args.sort == "count":
        sort_cols = [("count_reddit_all_time", True), ("clean_ratio_score", True)]
    else:
        sort_cols = [("clean_ratio_score", True), ("count_reddit_all_time", True)]

    show_table(spark, path, args.limit, args.truncate, sort_cols=sort_cols)

    spark.stop()


if __name__ == "__main__":
    main()
