from preview_utils import base_parser, get_spark, output_path, show_table


def main():
    parser = base_parser("Preview reddit_clean_posts_parquet.")
    args = parser.parse_args()

    spark = get_spark("PreviewRedditCleanPosts")
    path = output_path(args.config, "reddit_clean_posts_parquet")

    show_table(
        spark,
        path,
        args.limit,
        args.truncate,
        sort_cols=[("timestamp", False), ("post_id", False)],
    )

    spark.stop()


if __name__ == "__main__":
    main()
