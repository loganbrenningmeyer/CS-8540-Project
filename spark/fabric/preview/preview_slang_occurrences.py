from preview_utils import base_parser, get_spark, output_path, show_table


def main():
    parser = base_parser("Preview slang occurrence tables.")
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
        choices=["time", "ratio", "spike"],
        default="time",
    )
    args = parser.parse_args()

    spark = get_spark("PreviewSlangOccurrences")

    if args.candidate_type == "spiking":
        table_name = (
            "slang_occurrences_clean_ratio_spiking_"
            f"{args.spike_time_grain}_{args.mode}_parquet"
        )
    else:
        table_name = f"slang_occurrences_clean_ratio_{args.mode}_parquet"

    path = output_path(args.config, table_name)

    if args.sort == "ratio":
        sort_cols = [("clean_ratio_score", True), ("timestamp", False)]
    elif args.sort == "spike":
        sort_cols = [
            ("max_spike_score", True),
            ("clean_ratio_score", True),
            ("timestamp", False),
        ]
    else:
        sort_cols = [("timestamp", False), ("post_id", False), ("token_start", False)]

    show_table(spark, path, args.limit, args.truncate, sort_cols=sort_cols)

    spark.stop()


if __name__ == "__main__":
    main()
