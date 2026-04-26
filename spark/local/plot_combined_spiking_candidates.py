import argparse
import os
import re

import matplotlib.pyplot as plt
import pandas as pd


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--csv",
        default="combined_spiking_candidate_plotting_subreddit.csv",
        help="Path to the exported combined spiking plotting CSV.",
    )
    parser.add_argument(
        "--out-dir",
        default="spark/plots/combined_spiking_candidate_plots",
        help="Directory to write plots and summary CSVs.",
    )
    parser.add_argument(
        "--tokens",
        nargs="*",
        default=[],
        help="Optional specific tokens to plot in detail.",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=20,
        help="How many top candidates to show in summary bar charts.",
    )
    parser.add_argument(
        "--auto-token-count",
        type=int,
        default=8,
        help="If --tokens is not provided, how many strong tokens to auto-plot.",
    )
    return parser.parse_args()


def safe_name(value):
    return re.sub(r"[^a-zA-Z0-9._-]+", "_", value)


def token_dir(out_dir, token):
    path = os.path.join(out_dir, "tokens", safe_name(token))
    os.makedirs(path, exist_ok=True)
    return path


def read_data(csv_path):
    df = pd.read_csv(csv_path)

    numeric_cols = [
        "year",
        "month",
        "count_reddit",
        "total_tokens",
        "p_reddit",
        "count_reddit_all_time",
        "total_tokens_all_time",
        "p_reddit_all_time",
        "count_clean",
        "total_clean_tokens",
        "p_clean",
        "clean_ratio_score",
        "max_spike_score_monthly",
        "num_spike_periods_monthly",
        "max_spike_score_yearly",
        "num_spike_periods_yearly",
        "monthly_baseline_count_reddit",
        "monthly_baseline_total_tokens",
        "monthly_baseline_periods",
        "monthly_p_baseline",
        "monthly_p_current_smoothed",
        "monthly_p_baseline_smoothed",
        "monthly_spike_score",
        "yearly_baseline_count_reddit",
        "yearly_baseline_total_tokens",
        "yearly_baseline_periods",
        "yearly_p_baseline",
        "yearly_p_current_smoothed",
        "yearly_p_baseline_smoothed",
        "yearly_spike_score",
    ]

    bool_cols = [
        "has_monthly_spike",
        "has_yearly_spike",
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in bool_cols:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.lower()
                .map({"true": True, "false": False})
                .fillna(False)
            )

    monthly = df[df["time_grain"] == "monthly"].copy()
    yearly = df[df["time_grain"] == "yearly"].copy()

    if not monthly.empty:
        monthly["plot_date"] = pd.to_datetime(
            dict(year=monthly["year"], month=monthly["month"].fillna(1), day=1)
        )

    if not yearly.empty:
        yearly["plot_date"] = pd.to_datetime(
            dict(year=yearly["year"], month=1, day=1)
        )

    return df, monthly, yearly


def unique_candidates(df):
    cols = [
        "subreddit",
        "token",
        "count_reddit_all_time",
        "total_tokens_all_time",
        "p_reddit_all_time",
        "count_clean",
        "total_clean_tokens",
        "p_clean",
        "clean_ratio_score",
        "has_monthly_spike",
        "max_spike_score_monthly",
        "num_spike_periods_monthly",
        "has_yearly_spike",
        "max_spike_score_yearly",
        "num_spike_periods_yearly",
        "spike_profile",
    ]
    cols = [col for col in cols if col in df.columns]
    return df[cols].drop_duplicates(subset=["subreddit", "token"]).copy()


def save_candidate_summary(candidates, out_dir, top_n):
    summary = (
        candidates.sort_values(
            ["clean_ratio_score", "max_spike_score_monthly", "max_spike_score_yearly"],
            ascending=[False, False, False],
        )
        .head(top_n * 5)
        .copy()
    )
    summary.to_csv(os.path.join(out_dir, "candidate_summary_top.csv"), index=False)


def plot_bar(series, title, xlabel, out_path):
    plt.figure(figsize=(12, 8))
    series.sort_values().plot(kind="barh")
    plt.title(title)
    plt.xlabel(xlabel)
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()


def make_summary_plots(candidates, out_dir, top_n):
    spike_profile_counts = candidates["spike_profile"].value_counts(dropna=False)
    plot_bar(
        spike_profile_counts,
        "Combined Spiking Candidates By Spike Profile",
        "Number of tokens",
        os.path.join(out_dir, "spike_profile_counts.png"),
    )

    top_clean_ratio = (
        candidates[["token", "clean_ratio_score"]]
        .dropna()
        .sort_values("clean_ratio_score", ascending=False)
        .head(top_n)
        .set_index("token")["clean_ratio_score"]
    )
    plot_bar(
        top_clean_ratio,
        "Top Combined Candidates By Clean Ratio Score",
        "Clean ratio score",
        os.path.join(out_dir, "top_clean_ratio.png"),
    )

    top_monthly_spike = (
        candidates[["token", "max_spike_score_monthly"]]
        .dropna()
        .sort_values("max_spike_score_monthly", ascending=False)
        .head(top_n)
        .set_index("token")["max_spike_score_monthly"]
    )
    plot_bar(
        top_monthly_spike,
        "Top Combined Candidates By Max Monthly Spike Score",
        "Max monthly spike score",
        os.path.join(out_dir, "top_monthly_spike.png"),
    )

    top_yearly_spike = (
        candidates[["token", "max_spike_score_yearly"]]
        .dropna()
        .sort_values("max_spike_score_yearly", ascending=False)
        .head(top_n)
        .set_index("token")["max_spike_score_yearly"]
    )
    plot_bar(
        top_yearly_spike,
        "Top Combined Candidates By Max Yearly Spike Score",
        "Max yearly spike score",
        os.path.join(out_dir, "top_yearly_spike.png"),
    )


def auto_tokens(candidates, auto_token_count):
    ranked = candidates.sort_values(
        ["clean_ratio_score", "max_spike_score_monthly", "max_spike_score_yearly"],
        ascending=[False, False, False],
    )
    return ranked["token"].drop_duplicates().head(auto_token_count).tolist()


def plot_token_monthly(monthly, candidates, token, out_dir):
    token_rows = monthly[monthly["token"] == token].sort_values("plot_date")
    if token_rows.empty:
        return

    token_out_dir = token_dir(out_dir, token)
    meta = candidates[candidates["token"] == token].head(1)
    profile = meta["spike_profile"].iloc[0] if not meta.empty else "unknown"

    fig, axes = plt.subplots(3, 1, figsize=(12, 10), sharex=True)

    axes[0].plot(token_rows["plot_date"], token_rows["count_reddit"], marker="o")
    axes[0].set_ylabel("count_reddit")
    axes[0].set_title(f"{token} monthly usage ({profile})")

    axes[1].plot(token_rows["plot_date"], token_rows["p_reddit"], marker="o")
    axes[1].set_ylabel("p_reddit")

    if "monthly_spike_score" in token_rows.columns:
        axes[2].plot(
            token_rows["plot_date"],
            token_rows["monthly_spike_score"],
            marker="o",
            label="monthly_spike_score",
        )
    if "monthly_p_baseline" in token_rows.columns:
        axes[2].plot(
            token_rows["plot_date"],
            token_rows["monthly_p_baseline"],
            linestyle="--",
            label="monthly_p_baseline",
        )
    if "monthly_p_current_smoothed" in token_rows.columns:
        axes[2].plot(
            token_rows["plot_date"],
            token_rows["monthly_p_current_smoothed"],
            linestyle=":",
            label="monthly_p_current_smoothed",
        )
    axes[2].set_ylabel("spike/baseline")
    axes[2].legend(loc="best")

    plt.tight_layout()
    plt.savefig(
        os.path.join(token_out_dir, "monthly.png"),
        dpi=150,
    )
    plt.close(fig)


def plot_token_yearly(yearly, candidates, token, out_dir):
    token_rows = yearly[yearly["token"] == token].sort_values("plot_date")
    if token_rows.empty:
        return

    token_out_dir = token_dir(out_dir, token)
    meta = candidates[candidates["token"] == token].head(1)
    profile = meta["spike_profile"].iloc[0] if not meta.empty else "unknown"

    fig, axes = plt.subplots(3, 1, figsize=(12, 10), sharex=True)

    axes[0].plot(token_rows["plot_date"], token_rows["count_reddit"], marker="o")
    axes[0].set_ylabel("count_reddit")
    axes[0].set_title(f"{token} yearly usage ({profile})")

    axes[1].plot(token_rows["plot_date"], token_rows["p_reddit"], marker="o")
    axes[1].set_ylabel("p_reddit")

    if "yearly_spike_score" in token_rows.columns:
        axes[2].plot(
            token_rows["plot_date"],
            token_rows["yearly_spike_score"],
            marker="o",
            label="yearly_spike_score",
        )
    if "yearly_p_baseline" in token_rows.columns:
        axes[2].plot(
            token_rows["plot_date"],
            token_rows["yearly_p_baseline"],
            linestyle="--",
            label="yearly_p_baseline",
        )
    if "yearly_p_current_smoothed" in token_rows.columns:
        axes[2].plot(
            token_rows["plot_date"],
            token_rows["yearly_p_current_smoothed"],
            linestyle=":",
            label="yearly_p_current_smoothed",
        )
    axes[2].set_ylabel("spike/baseline")
    axes[2].legend(loc="best")

    plt.tight_layout()
    plt.savefig(
        os.path.join(token_out_dir, "yearly.png"),
        dpi=150,
    )
    plt.close(fig)


def main():
    args = parse_args()
    os.makedirs(args.out_dir, exist_ok=True)

    df, monthly, yearly = read_data(args.csv)
    candidates = unique_candidates(df)

    save_candidate_summary(candidates, args.out_dir, args.top_n)
    make_summary_plots(candidates, args.out_dir, args.top_n)

    tokens = args.tokens or auto_tokens(candidates, args.auto_token_count)

    for token in tokens:
        plot_token_monthly(monthly, candidates, token, args.out_dir)
        plot_token_yearly(yearly, candidates, token, args.out_dir)

    print(f"wrote_plots={args.out_dir}")
    print(f"plotted_tokens={','.join(tokens)}")


if __name__ == "__main__":
    main()
