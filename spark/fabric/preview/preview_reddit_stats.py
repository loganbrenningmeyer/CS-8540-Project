from preview_utils import base_parser, get_spark, output_path, show_table


def table_name(mode, grain):
    return f"reddit_stats_{grain}_{mode}_parquet"


def main():
    parser = base_parser("Preview Reddit stats tables.")
    parser.add_argument("--mode", choices=["subreddit", "global"], default="subreddit")
    parser.add_argument(
        "--grain",
        choices=["monthly", "yearly", "all_time"],
        default="monthly",
    )
    parser.add_argument(
        "--sort",
        choices=["count", "probability", "time"],
        default="count",
    )
    args = parser.parse_args()

    spark = get_spark("PreviewRedditStats")
    path = output_path(args.config, table_name(args.mode, args.grain))

    if args.sort == "probability":
        sort_cols = [("p_reddit", True), ("count_reddit", True)]
    elif args.sort == "time":
        sort_cols = [("year", False), ("month", False), ("count_reddit", True)]
    else:
        sort_cols = [("count_reddit", True), ("p_reddit", True)]

    show_table(spark, path, args.limit, args.truncate, sort_cols=sort_cols)

    spark.stop()


if __name__ == "__main__":
    main()
