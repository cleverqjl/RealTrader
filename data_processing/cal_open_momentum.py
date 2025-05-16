import polars as pl


def add_open_momentum_column(
    data_1d: pl.DataFrame,
    data_1m: pl.DataFrame,
    open_time: str = "09:31",
    check_time: str = "09:45",
    column_name: str = "open_momentum",
) -> pl.DataFrame:
    """
    计算开盘动量 (check_price - open_price) / open_price，并加入 data_1d。

    参数:
        data_1d: 日线数据，需包含 datetime 和 symbol
        data_1m: 分钟数据，需包含 datetime, close, symbol
        open_time: 开盘价格时间点（默认 "09:30"）
        check_time: 检查价格时间点（默认 "09:45"）
        column_name: 生成列的名称（默认 "open_return"）

    返回:
        含新列的 data_1d
    """
    # 提取时间列对应的日期
    data_1m = data_1m.with_columns(
        [
            pl.col("datetime").dt.date().alias("date"),
            pl.col("datetime").dt.strftime("%H:%M").alias("time_str"),
        ]
    )

    # 分别提取两个时间点的价格
    df_open = data_1m.filter(pl.col("time_str") == open_time).select(
        [pl.col("date"), pl.col("symbol"), pl.col("open").alias("open_price")]
    )

    df_check = data_1m.filter(pl.col("time_str") == check_time).select(
        [pl.col("date"), pl.col("symbol"), pl.col("close").alias("check_price")]
    )
    # print(df_open)
    # print(df_check)
    # 合并两个价格数据
    df_momentum = df_open.join(df_check, on=["date", "symbol"], how="inner")

    # 计算收益率
    df_momentum = df_momentum.with_columns(
        [
            (
                (pl.col("check_price") - pl.col("open_price")) / pl.col("open_price")
            ).alias(column_name)
        ]
    ).select(["date", "symbol", column_name])

    # # 把日期对齐为 datetime64[ns] 类型（匹配 data_1d 的 datetime 列）
    df_momentum = df_momentum.with_columns(pl.col("date").cast(pl.Date))

    # 合并到 data_1d
    if "date" not in data_1d.columns:
        data_1d = data_1d.with_columns(
            [pl.col("datetime").dt.date().cast(pl.Date).alias("date")]
        )
    else:
        data_1d = data_1d.with_columns(pl.col("date").cast(pl.Date))
    data_1d = data_1d.join(df_momentum, on=["date", "symbol"], how="left")
    return data_1d
