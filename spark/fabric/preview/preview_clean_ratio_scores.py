from preview_utils import base_parser, get_spark, output_path, show_table


def main():
    parser = base_parser("Preview clean_ratio_scores tables.")
    parser.add_argument("--mode", choices=["subreddit", "global"], default="subreddit")
    args = parser.parse_args()

    spark = get_spark("PreviewCleanRatioScores")
    path = output_path(args.config, f"clean_ratio_scores_{args.mode}_parquet")

    show_table(
        spark,
        path,
        args.limit,
        args.truncate,
        sort_cols=[("clean_ratio_score", True), ("count_reddit", True)],
    )

    spark.stop()


if __name__ == "__main__":
    main()
