import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


# 固定金额定投
def fixinvest(
    data_df,
    startdate,
    enddate,
    fix_mode="week",
    fix_money=1000,
    c_rate=0.000,
    price_col="close",
):
    data_df.bfill(inplace=True)
    data_df.index = pd.to_datetime(data_df.index)
    df = pd.DataFrame(data_df[startdate:enddate])
    df["month_num"] = [int(i.strftime("%d")) for i in df.index]
    df["week_num"] = [int(i.strftime("%w")) for i in df.index]
    df.index = pd.to_datetime(df.index)
    if fix_mode == "day":
        df["investment_per_time"] = fix_money
    elif fix_mode == "week":
        df.loc[df["week_num"] < df["week_num"].shift(1), "investment_per_time"] = (
            fix_money
        )
        df.iloc[0, 3] = fix_money
    elif fix_mode == "month":
        df.loc[df["month_num"] < df["month_num"].shift(1), "investment_per_time"] = (
            fix_money
        )
        df.iloc[0, 3] = fix_money
    df["investment_per_time"] = df["investment_per_time"].fillna(0)
    df["investment"] = df["investment_per_time"].cumsum()  # cumulative investment
    # calculate buy amount
    c_rate = 0.000  # commission rate
    df["buy_amount_per_time"] = (
        df["investment_per_time"] / df[price_col] * (1 - c_rate)
    )  # amount bought each time, considering commission
    df["buy_amount"] = df["buy_amount_per_time"].cumsum()  # total amount bought
    df["average_cost"] = df["investment"] / df["buy_amount"]
    df["marketvalue"] = df["buy_amount"] * df[price_col]
    df["profit"] = df["marketvalue"] - df["investment"]
    return df


def plot_investment_returns(
    df,
    column_names,
    title=None,
    figsize=(12, 6),
    save_path=None,
    price_col="close",
    show_price=True,
):
    """
    绘制投资回报图表

    参数:
    df: DataFrame, 包含投资数据的DataFrame
    column_names: list, 要绘制的列名列表
    title: str, 图表标题，如果为None则使用"fixed invest return"
    figsize: tuple, 图表大小
    save_path: str, 图片保存路径，如果为None则尝试显示图片
    price_col: str, 价格列名
    show_price: bool, 是否显示价格走势
    """
    fig, ax1 = plt.subplots(figsize=figsize)

    # 为每个指标设置不同的颜色和样式
    styles = ["-", "--", "-."]
    colors = ["#1f77b4", "#ff7f0e", "#2ca02c"]

    for i, col in enumerate(column_names):
        ax1.plot(
            df.index,
            df[col],
            label=col,
            linestyle=styles[i % len(styles)],
            color=colors[i % len(colors)],
            linewidth=2,
        )

    # 设置第一个y轴
    ax1.set_xlabel("date")
    ax1.set_ylabel("money")
    ax1.grid(True)

    # 创建第二个y轴并绘制价格
    if show_price:
        ax2 = ax1.twinx()
        ax2.plot(
            df.index, df[price_col], label="price", color="red", alpha=0.5, linewidth=1
        )
        ax2.set_ylabel("price")
        # 合并两个y轴的图例
        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left")
    else:
        ax1.legend(loc="upper left")

    plt.title(title if title else "fixed invest return")

    if save_path:
        plt.savefig(save_path, dpi=300, bbox_inches="tight")
        plt.close()
    else:
        try:
            plt.show()
        except Exception as e:
            print(f"无法显示图片: {e}")
            print("请使用save_path参数保存图片到文件")
    plt.close()


data = pd.read_csv("demo/fixed_invest/H20269.csv", index_col=0)
# 删除close为空值的行
data = data[data["close"].notna()]
return_df = fixinvest(
    data, "2015-01-01", "2025-12-31", fix_mode="month", fix_money=5000
)
print(return_df.head(1000))

# 使用新的绘图函数绘制多个指标，不显示价格走势
plot_investment_returns(
    return_df,
    ["marketvalue", "profit", "investment"],
    "fixed invest return",
    save_path="demo/fixed_invest/returns_plot.png",
    price_col="close",
    show_price=False,
)
print(return_df.iloc[-1])
