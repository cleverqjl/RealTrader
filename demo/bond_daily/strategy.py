
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from factorlib.PriceVolume.trend.bollinger_bands import calculate_bollinger_bands
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.font_manager as fm
# import talib as tb

# 设置中文字体
try:
    plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
    plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号
except:
    print("警告: 无法设置中文字体，图表中的中文可能无法正确显示")

# Load and prepare data
df = pd.read_parquet(r'Data/T9999.parquet')
print(df.columns)
print(df)
data_1d = df[df['freq']=='1d'].copy()  # Create an explicit copy
data_1m = df[df['freq']=='1m'].copy()

# 确保数据按时间排序
data_1d = data_1d.sort_values('datetime')
data_1d = data_1d[data_1d['datetime']<'2026-01-01']

# 计算收益率
data_1d['return'] = data_1d['close'].pct_change() * 100  # 转换为百分比
data_1d['return_settle'] = data_1d['settle_price']/data_1d['settle_price'].shift(1)-1

# Calculate Bollinger Bands for daily data
middle, upper, lower = calculate_bollinger_bands(data_1d,20,0.5)

# Add Bollinger Bands to the DataFrame
data_1d['bb_middle'] = middle
data_1d['bb_upper'] = upper
data_1d['bb_lower'] = lower

# 实现量化交易策略
# 创建T-2日收益率的移位列
data_1d['return_lag2'] = data_1d['return_settle'].shift(2)

# 创建信号列
data_1d['signal'] = pd.NA  # 0表示不操作

# 做空信号：T-2日收益率大于0.3%，且close<bb_lower
short_condition1 = (data_1d['return_lag2'] > 0.0003) &  (data_1d['return_lag2'] < 0.005)
short_condition2 = data_1d['close'].shift(1)<data_1d['bb_lower'].shift(1)
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
long_condition3 = data_1d['close'].shift(1)>data_1d['bb_upper'].shift(1)

# 合并做多条件
long_condition = (long_condition1 & long_condition3) | long_condition2
data_1d.loc[long_condition, 'signal'] = 1  # 1表示做多
data_1d.loc[~(long_condition|short_condition), 'signal'] = 0



# -对齐
df2 = pd.read_parquet(r'Data\backtest.parquet')

df1 = data_1d.set_index('datetime')
df2 = df2.set_index('time')
print(df1.head(50))
print(df2)
df_merge = pd.concat([df1['signal'],df2['size']],axis=1,join='outer')
print(df_merge.head(50))
# exit()

# 创建持仓列（使用cumsum和ffill实现持仓保持）
data_1d['pos'] = data_1d['signal'].shift(0)  # T+1日开盘价执行
# data_1d['pos'] = (df_merge['size']/10000).to_list()
# data_1d['pos'] = data_1d['pos'].replace(0, pd.NA)  # 将0替换为NA以便后续填充
# data_1d['pos'] = data_1d['pos'].ffill().fillna(0)  # 向前填充，保持上一日仓位
print(data_1d.head(50))
# exit()

# 仓位--->盈亏
# -计算下一根k线开盘价（计算平仓价格用）
data_1d['next_open'] = data_1d['open'].shift(-1)  # 下根K线的开盘价
data_1d['next_open'].fillna(value=data_1d['close'], inplace=True)
# -找出开仓的k线
condition1 = data_1d['pos'] != 0  # 当前周期不为空仓
condition2 = data_1d['pos'] != data_1d['pos'].shift(1)  # 当前周期和上个周期持仓方向不一样。
open_pos_condition = condition1 & condition2
# -找出平仓的k线
condition1 = data_1d['pos'] != 0  # 当前周期不为空仓
condition2 = data_1d['pos'] != data_1d['pos'].shift(-1).fillna(method='ffill')  # 当前周期和下个周期持仓方向不一样。
close_pos_condition = condition1 & condition2    
# -滑点
minpoint = 0
slippageMulti = 0
slippage = minpoint * slippageMulti
# -开仓价格：理论开盘价加上相应滑点
data_1d.loc[open_pos_condition, 'open_pos_price'] = data_1d['open'] + slippage*data_1d['pos']
# -平仓价格
data_1d.loc[close_pos_condition, 'close_pos_price'] = data_1d['next_open'] - slippage*data_1d['pos']
# -固定手数计算
data_1d.loc[open_pos_condition, 'contract_num'] = 1
data_1d['contract_num'].fillna(method='ffill',inplace=True)
data_1d.loc[data_1d['pos'] == 0, ['contract_num']] = 0
data_1d['contract_num'].fillna(0,inplace=True)
# -盈利计算(加上滑点耗损)
contractMulti = 1000000
data_1d['profit']=data_1d['contract_num']*(data_1d['close']-data_1d['close'].shift(1))*data_1d['pos']*contractMulti
data_1d.loc[open_pos_condition, 'profit']=data_1d['contract_num']*(data_1d['close']-data_1d['open_pos_price'])*data_1d['pos']*contractMulti
data_1d.loc[close_pos_condition, 'profit']=data_1d['contract_num']*(data_1d['close_pos_price']-data_1d['close'].shift(1))*data_1d['pos']*contractMulti
# -手续费计算
c_rate = 0
data_1d.loc[open_pos_condition, 'fee'] = data_1d[open_pos_condition]['contract_num']*data_1d[open_pos_condition]['open_pos_price']*c_rate*contractMulti
data_1d.loc[close_pos_condition, 'fee'] = data_1d[close_pos_condition]['contract_num']*data_1d[close_pos_condition]['close_pos_price']*c_rate*contractMulti
data_1d['fee'].fillna(0,inplace=True)
# -净盈利计算
data_1d['net_profit']=data_1d['profit']-data_1d['fee']
# -累计盈亏计算
data_1d['profit_cum']=data_1d['net_profit'].cumsum()
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
data_1d['date'] = pd.to_datetime(data_1d['datetime'])
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