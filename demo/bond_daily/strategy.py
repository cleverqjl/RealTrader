
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from factorlib.PriceVolume.trend.bollinger_bands import calculate_bollinger_bands
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.font_manager as fm
from utils.backtest import cal_atr

# 设置中文字体
plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号
 
# Load and prepare data
df = pd.read_parquet(r'Data/T9999.parquet')
# print(df.columns)
# print(df)
data_1d = df[df['freq']=='1d'].copy()  # Create an explicit copy
data_1m = df[df['freq']=='1m'].copy()

# 确保数据按时间排序
data_1d = data_1d.sort_values('datetime')
data_1d = data_1d[data_1d['datetime']<'2026-01-01']

# 计算收益率
data_1d['return'] = data_1d['close'].pct_change() * 100  # 转换为百分比
data_1d['return_settle'] = data_1d['settle_price']/data_1d['settle_price'].shift(1)-1

# Calculate Bollinger Bands for daily data
middle, upper, lower = calculate_bollinger_bands(data_1d,10,0.5)

# Add Bollinger Bands to the DataFrame
data_1d['bb_middle'] = middle
data_1d['bb_upper'] = upper
data_1d['bb_lower'] = lower

# 计算atr
# data_1d['atr'] = cal_atr(data_1d,10)
# 计算ema
data_1d['ema5'] = data_1d['close'].ewm(span=5,adjust=False).mean()
data_1d['ema10'] = data_1d['close'].ewm(span=10,adjust=False).mean()
# print(data_1d[['datetime','close','atr','ema']].head(50))
# exit()

# 实现量化交易策略
# 创建T-2日收益率的移位列
data_1d['return_lag2'] = data_1d['return_settle'].shift(1)

# 创建信号列
data_1d['signal'] = pd.NA  # 0表示不操作

# 做空信号：T-2日收益率大于0.3%，且close<bb_lower
short_condition1 = (data_1d['return_lag2'] > 0.0003) &  (data_1d['return_lag2'] < 0.005)
short_condition2 = data_1d['close']<data_1d['ema5']
short_condition = short_condition1 & short_condition2
data_1d.loc[short_condition, 'signal'] = -1  # -1表示做空

# 将datetime转换为日期类型（如果尚未转换）
data_1d['date'] = pd.to_datetime(data_1d['datetime'])

# 判断是否为周三（0-6 表示周一到周日）
is_wednesday = data_1d['date'].dt.weekday == 2  # 2表示周三

# 做多信号1：T-2日收益率小于-0.3%
long_condition1 = (data_1d['return_lag2'] < -0.0003) &  (data_1d['return_lag2'] > -0.005)
# 做多信号2：当天是周三，下一日开盘做多
long_condition2 = is_wednesday.shift(-1).fillna(False)  # 下一日是周三（即当天是周二）
long_condition3 = data_1d['close']>data_1d['ema5']

# 合并做多条件
long_condition = (long_condition1 & long_condition3) #| long_condition2
data_1d.loc[long_condition1, 'signal'] = 1  # 1表示做多
data_1d.loc[~(long_condition1|short_condition), 'signal'] = 0



# -对齐
df2 = pd.read_parquet(r'Data\backtest.parquet')

df1 = data_1d.set_index('datetime')
df2 = df2.set_index('time')
# print(df1.head(50))
# print(df2)
df_merge = pd.concat([df1['signal'],df2['size']],axis=1,join='outer')
# print(df_merge.head(50))
# exit()

# 创建持仓列（使用cumsum和ffill实现持仓保持）
data_1d['pos'] = data_1d['signal'].shift(1)  # T+1日开盘价执行
data_1d['pos'] = data_1d['pos'].replace(0, pd.NA)  # 将0替换为NA以便后续填充
data_1d['pos'] = data_1d['pos'].ffill().fillna(0)
# data_1d['pos'] = (df_merge['size']/10000).to_list()
# data_1d['pos'] = data_1d['pos'].replace(0, pd.NA)  # 将0替换为NA以便后续填充
# data_1d['pos'] = data_1d['pos'].ffill().fillna(0)  # 向前填充，保持上一日仓位
# print(data_1d[['datetime','return_lag2','signal','pos']].head(50))
# exit()

# 仓位--->盈亏
from utils.backtest import contract_num_to_profit,pos_to_contract_num
data_1d['trading_date'] = data_1d['datetime'].dt.strftime('%Y-%m-%d')
data_1d.rename(columns={'datetime':'candle_begin_time'},inplace=True)
data_1d = pos_to_contract_num(data_1d,minpoint=0.005,slippageMulti=1,capitalmode='atr_day',capital_unit=1000000,contractMulti=1000000,limitValue=None)
print(data_1d[['candle_begin_time','pos','close','open','trade_price','contract_num']])
data_1d = contract_num_to_profit(data_1d,contractMulti=1000000,cost=3,cost_type='amount',minpoint=0.005,slippageMulti=0)

print(data_1d[['candle_begin_time','pos','close','open','trade_price','contract_num','contract_num_hold','contract_num_trade','fee','profit','profit_cum']].tail(50))
# exit()
print(data_1d['profit_cum'])


# exit()

# 盈亏--->净值

# # 净值--->收益回报分析
# print("\n策略执行详情：")
# print(data_1d[['datetime', 'open', 'close', 'next_open', 'next2_open', 'return_settle', 'return_lag2', 'signal', 'position', 'position_return']].tail(20))
# # -输出策略统计
# print("\n策略统计:")
# print(f"总交易次数: {(data_1d['position'] != 0).sum()}")
# print(f"做多次数: {(data_1d['position'] == 1).sum()}")
# print(f"做空次数: {(data_1d['position'] == -1).sum()}")
# print(f"策略累计收益: {data_1d['cumulative_strategy_return'].iloc[-1]:.2%}")
# print(f"基准累计收益: {data_1d['cumulative_return'].iloc[-1]:.2%}")

# 收益回报分析--->绘图
# -绘制净值曲线
plt.figure(figsize=(12, 6))
# -将datetime转换为日期格式
data_1d['date'] = pd.to_datetime(data_1d['candle_begin_time'])
# -计算净值曲线（初始资金为1）
# data_1d['profit_cum'] = (1 + data_1d['cumulative_strategy_return'])
# data_1d['benchmark_equity'] = (1 + data_1d['cumulative_return'])
# -绘制策略净值曲线和基准净值曲线
plt.plot(data_1d['date'], data_1d['profit_cum'], label='策略净值', linewidth=2)
# plt.plot(data_1d['date'], data_1d['benchmark_equity'], label='基准净值', linewidth=2, alpha=0.7)
# -设置图表格式
plt.title('策略净值曲线对比图', fontsize=15)
plt.xlabel('日期', fontsize=12)
plt.ylabel('净值', fontsize=12)
plt.grid(True, linestyle='--', alpha=0.5)
plt.legend(fontsize=12)
# -设置x轴日期格式
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
plt.gca().xaxis.set_major_locator(mdates.MonthLocator(interval=3))
plt.gcf().autofmt_xdate()  # 自动旋转日期标签
# -保存图表
plt.tight_layout()
plt.savefig('strategy_equity_curve.png', dpi=300)
plt.show()