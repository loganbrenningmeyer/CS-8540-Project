from preview_utils import base_parser, get_spark, output_path, show_table


def main():
    parser = base_parser("Preview spike_scores tables.")
    parser.add_argument("--mode", choices=["subreddit", "global"], default="subreddit")
    parser.add_argument("--time-grain", choices=["monthly", "yearly"], default="monthly")
    parser.add_argument(
        "--sort",
        choices=["spike", "time", "count"],
        default="spike",
    )
    args = parser.parse_args()

    spark = get_spark("PreviewSpikeScores")
    path = output_path(
        args.config,
        f"spike_scores_{args.time_grain}_{args.mode}_parquet",
    )

    if args.sort == "time":
        sort_cols = [("year", False), ("month", False), ("spike_score", True)]
    elif args.sort == "count":
        sort_cols = [("count_reddit", True), ("spike_score", True)]
    else:
        sort_cols = [("spike_score", True), ("count_reddit", True)]

    show_table(spark, path, args.limit, args.truncate, sort_cols=sort_cols)

    spark.stop()


if __name__ == "__main__":
    main()
