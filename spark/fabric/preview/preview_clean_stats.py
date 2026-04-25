from preview_utils import base_parser, get_spark, output_path, show_table


def main():
    parser = base_parser("Preview clean_stats_parquet.")
    args = parser.parse_args()

    spark = get_spark("PreviewCleanStats")
    path = output_path(args.config, "clean_stats_parquet")

    show_table(
        spark,
        path,
        args.limit,
        args.truncate,
        sort_cols=[("count_clean", True)],
    )

    spark.stop()


if __name__ == "__main__":
    main()
