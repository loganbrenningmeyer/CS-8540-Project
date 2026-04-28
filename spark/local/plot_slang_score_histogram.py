import argparse
import os

import matplotlib.pyplot as plt
import pandas as pd


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--csv",
        default="slang_score_plotting_subreddit.csv",
        help="Path to the exported slang-score plotting CSV file.",
    )
    parser.add_argument(
        "--out-path",
        default="spark/plots/slang_score_histogram.png",
        help="Path to write the histogram image.",
    )
    parser.add_argument(
        "--bins",
        type=int,
        default=50,
        help="Number of histogram bins.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    scores_pdf = pd.read_csv(args.csv)

    if scores_pdf.empty:
        raise ValueError("No slang_score rows found to plot.")

    scores = pd.to_numeric(scores_pdf["slang_score"], errors="coerce").dropna()
    if scores.empty:
        raise ValueError("No valid numeric slang_score values found to plot.")

    out_dirname = os.path.dirname(args.out_path)
    if out_dirname:
        os.makedirs(out_dirname, exist_ok=True)

    plt.figure(figsize=(10, 6))
    plt.hist(scores, bins=args.bins, range=(0.0, 1.0), edgecolor="black")
    plt.title("Slang Score Histogram")
    plt.xlabel("slang_score")
    plt.ylabel("Number of tokens")
    plt.tight_layout()
    plt.savefig(args.out_path, dpi=150)
    plt.close()

    print(f"csv={args.csv}")
    print(f"rows_plotted={len(scores)}")
    print(f"bins={args.bins}")
    print(f"wrote_histogram={args.out_path}")


if __name__ == "__main__":
    main()
