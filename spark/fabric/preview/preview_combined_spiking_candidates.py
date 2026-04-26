from preview_utils import base_parser, get_spark, output_path, show_table


def main():
    parser = base_parser("Preview combined monthly/yearly spiking candidate tables.")
    parser.add_argument("--mode", choices=["subreddit", "global"], default="subreddit")
    parser.add_argument(
        "--sort",
        choices=["ratio", "monthly_spike", "yearly_spike", "count", "profile"],
        default="ratio",
    )
    args = parser.parse_args()

    spark = get_spark("PreviewCombinedSpikingCandidates")
    path = output_path(
        args.config,
        f"slang_candidates_clean_ratio_spiking_combined_{args.mode}_parquet",
    )

    if args.sort == "monthly_spike":
        sort_cols = [("max_spike_score_monthly", True), ("clean_ratio_score", True)]
    elif args.sort == "yearly_spike":
        sort_cols = [("max_spike_score_yearly", True), ("clean_ratio_score", True)]
    elif args.sort == "count":
        sort_cols = [("count_reddit_all_time", True), ("clean_ratio_score", True)]
    elif args.sort == "profile":
        sort_cols = [
            ("has_monthly_spike", True),
            ("has_yearly_spike", True),
            ("clean_ratio_score", True),
        ]
    else:
        sort_cols = [("clean_ratio_score", True), ("count_reddit_all_time", True)]

    show_table(spark, path, args.limit, args.truncate, sort_cols=sort_cols)

    spark.stop()


if __name__ == "__main__":
    main()
