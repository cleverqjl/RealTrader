from datetime import datetime
import polars as pl


def cal_vwap(
    df: pl.DataFrame,
    date_str: str,
    start_time: str,
    end_time: str,
    symbol: str = None,
) -> float:
    """
    计算某一天、某个时间段内的 VWAP（Volume Weighted Average Price）

    参数:
        df: 包含 datetime, close, volume 的 polars.DataFrame
        date_str: 日期字符串（如 "2022/01/04"）
        start_time: 起始时间字符串（如 "14:45"）
        end_time: 结束时间字符串（如 "15:10"）
        symbol: 可选，若传入则只计算该品种的 VWAP

    返回:
        对应时间段的 VWAP 值
    """

    # 构造 datetime 起止区间
    date_fmt = "%Y/%m/%d"
    time_fmt = "%H:%M"

    start_dt = datetime.strptime(f"{date_str} {start_time}", f"{date_fmt} {time_fmt}")
    end_dt = datetime.strptime(f"{date_str} {end_time}", f"{date_fmt} {time_fmt}")

    # 过滤条件
    df_filtered = df.filter(
        (pl.col("datetime") > start_dt) & (pl.col("datetime") <= end_dt)
    )

    if symbol is not None:
        df_filtered = df_filtered.filter(pl.col("symbol") == symbol)

    # VWAP = sum(price * volume) / sum(volume)
    vwap_df = df_filtered.select(
        [
            (pl.col("close") * pl.col("volume")).sum().alias("pv_sum"),
            pl.col("volume").sum().alias("v_sum"),
        ]
    )

    pv_sum = vwap_df[0, "pv_sum"]
    v_sum = vwap_df[0, "v_sum"]

    return float(pv_sum / v_sum) if v_sum != 0 else float("nan")


def add_vwap_columns(
    data_1d: pl.DataFrame,
    data_1m: pl.DataFrame,
    time_periods: list[tuple[str, str]] = [("13:00", "14:45"), ("14:45", "15:10")],
    column_names: list[str] = ["vwap1", "vwap2"],
) -> pl.DataFrame:
    """
    为 data_1d 添加指定时间段的 VWAP 列。

    参数:
        data_1d: 日线数据（含 datetime, symbol）
        data_1m: 分钟数据（含 datetime, close, volume, symbol）
        time_periods: 时间段列表，每个元素是 (start_time, end_time)
        column_names: 每个时间段对应的新列名

    返回:
        添加了 VWAP 列的 data_1d
    """
    assert len(time_periods) == len(column_names), "时间段与列名数量必须一致"

    # 初始化每列的 VWAP 结果列表
    vwap_results = [[] for _ in time_periods]

    for row in data_1d.iter_rows(named=True):
        date_str = row["datetime"].strftime("%Y/%m/%d")
        symbol = row["symbol"]

        for i, (start_time, end_time) in enumerate(time_periods):
            vwap_val = cal_vwap(
                df=data_1m,
                date_str=date_str,
                start_time=start_time,
                end_time=end_time,
                symbol=symbol,
            )
            vwap_results[i].append(vwap_val)

    # 拼接进 df
    for name, values in zip(column_names, vwap_results):
        data_1d = data_1d.with_columns(pl.Series(name=name, values=values))
    return data_1d
