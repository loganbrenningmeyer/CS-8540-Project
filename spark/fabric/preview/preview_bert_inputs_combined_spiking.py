from preview_utils import base_parser, get_spark, output_path, show_table


def main():
    parser = base_parser("Preview BERT input rows from combined spiking candidates.")
    parser.add_argument("--mode", choices=["subreddit", "global"], default="subreddit")
    parser.add_argument(
        "--sort",
        choices=["time", "token", "subreddit"],
        default="time",
    )
    args = parser.parse_args()

    spark = get_spark("PreviewBertInputsCombinedSpiking")
    path = output_path(
        args.config,
        f"bert_inputs_clean_ratio_spiking_combined_{args.mode}_parquet",
    )

    if args.sort == "token":
        sort_cols = [("token", False), ("timestamp", False)]
    elif args.sort == "subreddit":
        sort_cols = [("subreddit", False), ("timestamp", False), ("token", False)]
    else:
        sort_cols = [("timestamp", False), ("token", False)]

    show_table(spark, path, args.limit, args.truncate, sort_cols=sort_cols)

    spark.stop()


if __name__ == "__main__":
    main()
