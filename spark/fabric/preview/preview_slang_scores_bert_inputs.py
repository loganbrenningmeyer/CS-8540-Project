from preview_utils import base_parser, get_spark, output_path, show_table


def main():
    parser = base_parser("Preview token-level slang score tables.")
    parser.add_argument("--mode", choices=["subreddit", "global"], default="subreddit")
    parser.add_argument(
        "--sort",
        choices=["score", "token", "subreddit"],
        default="score",
    )
    args = parser.parse_args()

    spark = get_spark("PreviewSlangScoresBertInputs")

    path = output_path(
        args.config,
        f"slang_scores_bert_input_{args.mode}_parquet",
    )

    if args.sort == "token":
        sort_cols = [("token", False), ("slang_score", True)]
    elif args.sort == "subreddit":
        sort_cols = [("subreddit", False), ("slang_score", True), ("token", False)]
    else:
        sort_cols = [("slang_score", True), ("token", False)]

    show_table(spark, path, args.limit, args.truncate, sort_cols=sort_cols)

    spark.stop()


if __name__ == "__main__":
    main()
