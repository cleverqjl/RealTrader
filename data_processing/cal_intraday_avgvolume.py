import polars as pl
import matplotlib.pyplot as plt
from pathlib import Path


def cal_intraday_volume_profile(
    df: pl.DataFrame,
    symbol: str,
) -> pl.DataFrame:
    """
    计算某品种的日内成交量分布

    参数:
        df: 包含 datetime, symbol,volume 的 polars.DataFrame
        symbol: 品种代码（如 "RB9999"）
    """
    # 过滤当前品种，提取时间
    df = df.filter(pl.col("symbol") == symbol).with_columns(
        pl.col("datetime").dt.time().alias("time_only")
    )
    # 计算每个 time_only 的平均成交量
    vol_profile = (
        df.group_by("time_only")
        .agg(pl.col("volume").mean().alias("avg_volume"))
        .sort("time_only")
    )
    return vol_profile


def plot_intraday_volume_profile(
    vol_profile: pl.DataFrame, save_path: str = "./intraday_volume_profile.png"
) -> None:
    # 提取数据用于绘图
    times = vol_profile["time_only"].cast(str).to_list()
    avg_vols = vol_profile["avg_volume"].to_list()

    # 绘图
    plt.figure(figsize=(14, 6))
    plt.plot(times, avg_vols, color="#4682B4", linewidth=2)
    plt.fill_between(times, avg_vols, color="#B0C4DE", alpha=0.5)

    plt.title(f"Intraday Average Volume Profile", fontsize=14)
    plt.xlabel("Time")
    plt.ylabel("Average Volume")
    # 控制横坐标稀疏显示
    xtick_locs = list(range(0, len(times), 14))  # 每隔多少分钟显示一个label
    xtick_labels = [times[i] for i in xtick_locs]
    plt.xticks(ticks=xtick_locs, labels=xtick_labels, rotation=45, fontsize=9)
    plt.grid(True, linestyle="--", alpha=0.6)
    plt.tight_layout()

    # 保存图片
    Path(save_path).parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(save_path, dpi=300)
    plt.close()
    print(f"图已保存至: {save_path}")
