import polars as pl
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path


def make_label(lo, hi):
    return f"[{lo*100:.2f}%, {hi*100:.2f}%)"


def cal_distribution_stats_auto_bins(
    df: pl.DataFrame,
    cal_col: str = "return",
    num_bins: int = 20,
    min_val: float = -0.10,
    max_val: float = 0.10,
) -> pl.DataFrame:
    """
    自动将 df中的某列数值按指定范围等距分箱，并统计每个区间的频数和占比（包括 count 为 0 的区间）
    """
    # 构建分箱区间和标签
    bins = np.linspace(min_val, max_val, num_bins + 1)
    labels = [make_label(bins[i], bins[i + 1]) for i in range(num_bins)]
    exprs = []
    for i in range(num_bins):
        lo = bins[i]
        hi = bins[i + 1]
        exprs.append(
            pl.when((pl.col(cal_col) >= lo) & (pl.col(cal_col) < hi)).then(
                pl.lit(labels[i])
            )
        )

    # 添加分箱列
    df = df.with_columns([pl.coalesce(exprs).alias("bucket")]).drop_nulls("bucket")

    # 实际统计到的区间分布
    dist = df.group_by("bucket").agg([pl.len().alias("count")])

    # 构造完整区间列表 DataFrame
    full_dist = pl.DataFrame({"bucket": labels}).with_columns(
        [
            pl.col("bucket")
            .str.extract(r"\[([-\d\.]+)", 1)
            .cast(pl.Float64)
            .alias("bucket_start")
        ]
    )

    # 补全 bucket_start 并 join
    dist = dist.with_columns(
        [
            pl.col("bucket")
            .str.extract(r"\[([-\d\.]+)", 1)
            .cast(pl.Float64)
            .alias("bucket_start")
        ]
    )

    dist = full_dist.join(dist, on="bucket", how="left").with_columns(
        [pl.col("count").fill_null(0)]
    )

    # 计算总数和百分比
    total = dist["count"].sum()
    dist = (
        dist.with_columns(
            [(pl.col("count") / total * 100).round(2).alias("percentage")]
        )
        .sort("bucket_start")
        .drop("bucket_start")
    )

    return dist.select(["bucket", "count", "percentage"])


def plot_distribution(
    dist_df: pl.DataFrame, save_path: str = "./distribution.png"
) -> None:
    """
    画出 istribution_stats 的结果柱状图，并保存为图片文件

    参数:
        dist_df: polars.DataFrame，包含 bucket, count, percentage 三列
        save_path: str，本地文件保存路径
    """
    # 提取数据
    buckets = dist_df["bucket"].to_list()
    percentages = dist_df["percentage"].to_list()

    plt.figure(figsize=(12, 7))
    bars = plt.bar(buckets, percentages, color="#69b3a2")

    # 显示百分比标签
    for bar, pct in zip(bars, percentages):
        height = bar.get_height()
        plt.text(
            bar.get_x() + bar.get_width() / 2,
            height + 0.5,
            f"{pct:.1f}%",
            ha="center",
            va="bottom",
            fontsize=9,
        )

    plt.title("Value Distribution", fontsize=14)
    plt.xlabel("Value Range")
    plt.ylabel("Percentage (%)")
    plt.xticks(rotation=30)
    plt.grid(axis="y", linestyle="--", alpha=0.5)
    plt.tight_layout()

    # 创建目录并保存图片
    Path(save_path).parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(save_path, dpi=300)
    plt.close()  # 不在交互环境中显示
    print(f"图已保存至: {save_path}")


def cal_stats_summary_df(
    df: pl.DataFrame, cal_col: str = "return", interval: tuple[float, float] = None
) -> pl.DataFrame:
    """
    计算 return 列的统计指标，并返回为 Polars DataFrame。

    包括：均值、标准差、中位数、mean±2σ、mean±3σ、指定区间内比例（可选）
    """
    df = df.drop_nulls(cal_col)
    mean = df[cal_col].mean()
    std = df[cal_col].std()
    median = df[cal_col].median()

    stats = [
        ("mean", mean),
        ("std", std),
        ("median", median),
        ("mean - 2σ", mean - 2 * std),
        ("mean + 2σ", mean + 2 * std),
        ("mean - 3σ", mean - 3 * std),
        ("mean + 3σ", mean + 3 * std),
    ]

    if interval:
        a, b = interval
        total_count = df.height
        inside_count = df.filter((pl.col(cal_col) >= a) & (pl.col(cal_col) <= b)).height
        percentage = round(inside_count / total_count * 100, 2)
        stats.append((f"prob in [{a:.4f}, {b:.4f}]", percentage))

    return pl.DataFrame({"stat": [s[0] for s in stats], "value": [s[1] for s in stats]})


# - 计算某列数值在过去n个交易日所处于的分位数
def cal_rolling_percentile_rank(series: pl.Series, window_size: int = 120) -> pl.Series:
    values = series.to_numpy()
    ranks = np.full(len(values), np.nan)

    for i in range(window_size - 1, len(values)):
        window = values[i - window_size + 1 : i + 1]
        current = values[i]
        # 计算当前值在过去窗口中的百分位（非严格定义）
        rank = (window < current).sum() / len(window)
        ranks[i] = rank
    return pl.Series(ranks)


def cal_rolling_percentile_rank_return(
    series: pl.Series, window_size: int = 120
) -> pl.Series:
    values = series.to_numpy()
    result = np.full(len(values), np.nan)

    for i in range(window_size - 1, len(values)):
        current = values[i]
        window = values[i - window_size + 1 : i + 1]

        if current > 0:
            pos_values = window[window > 0]
            if len(pos_values) > 0:
                result[i] = (pos_values < current).sum() / len(pos_values)
        elif current < 0:
            neg_values = window[window < 0]
            if len(neg_values) > 0:
                result[i] = (neg_values < current).sum() / len(neg_values)
        else:
            result[i] = np.nan

    return pl.Series(result)
