# whisky策略
# data_df = pd.DataFrame(portfolio_df_dict[i]).copy()
# strategyname = 'wh1'
import pandas as pd
import numpy as np
import talib as tb
import time 

def brandySignal(data_df,strategyname,contractMulti,para=[400,1.5,8],capital_unit=100,long=1,short=-1):  #long,short指做多做空，如果不做则改为0,如果做空short改为-1
    # ===策略参数
    n = int(para[0])
    m = para[1]
    atrn = para[2]
    # ===计算指标
    # 计算均线
    data_df['median'] = data_df['close'].rolling(n).mean()  # 此处只计算最后几行的均线值，因为没有加min_period参数
    median = data_df.iloc[-1]['median']
    median2 = data_df.iloc[-2]['median']
    # 计算标准差
    data_df['std'] = data_df['close'].rolling(n).std(ddof=0)  # ddof代表标准差自由度，只计算最后几行的均线值，因为没有加min_period参数
    std = data_df.iloc[-1]['std']
    std2 = data_df.iloc[-2]['std']
    # 计算上轨、下轨道
    upper = median + m * std
    lower = median - m * std
    upper2 = median2 + m * std2
    lower2 = median2 - m * std2
    
    # ===寻找交易信号
    signal = None
    close = data_df.iloc[-1]['close']
    close2 = data_df.iloc[-2]['close']
    # 查看当前仓位
    last_signal = int(list(data_df[strategyname+'_signal'].dropna())[-1])
    #首先继承之前的仓位
    data_df.loc[data_df.index[-1],strategyname+'_signal'] = data_df.loc[data_df.index[-2],strategyname+'_signal'] 
    data_df.loc[data_df.index[-1],strategyname+'_contract_num'] = data_df.loc[data_df.index[-2],strategyname+'_contract_num'] 
    # 计算止盈指标
    data_df.loc[data_df.index[-1],strategyname+'_long_entry_price'] = data_df.loc[data_df.index[-2],strategyname+'_long_entry_price'] 
    data_df.loc[data_df.index[-1],strategyname+'_short_entry_price'] = data_df.loc[data_df.index[-2],strategyname+'_short_entry_price'] 
    data_df[strategyname+'_long_profitstop_price'] = np.nan
    data_df[strategyname+'_short_profitstop_price'] = np.nan
    
    if last_signal == 1:
        data_df.loc[data_df.index[-1],strategyname+'_long_profitstop_price'] = data_df.loc[data_df.index[-1],strategyname+'_long_entry_price'] + atrn*data_df.loc[data_df.index[-1],'atr_day']
    elif last_signal == -1:
        data_df.loc[data_df.index[-1],strategyname+'_short_profitstop_price'] = data_df.loc[data_df.index[-1],strategyname+'_short_entry_price'] - atrn*data_df.loc[data_df.index[-1],'atr_day'] 
    # 找出做多信号
    if (close > upper) and (close2 <= upper2) and (last_signal <= 0) :
        data_df.loc[data_df.index[-1],strategyname+'_signal']  = long
        data_df.loc[data_df.index[-1],strategyname+'_long_entry_price'] = data_df.loc[data_df.index[-1],'close']
        data_df.loc[data_df.index[-1],strategyname+'_contract_num'] =  max(1,round(capital_unit / data_df.loc[data_df.index[-1],'atr_day'] / contractMulti))
        data_df.loc[data_df.index[-1],strategyname+'_long_profitstop_price'] = data_df.loc[data_df.index[-1],strategyname+'_long_entry_price'] + atrn*data_df.loc[data_df.index[-1],'atr_day']
    # 找出做空信号
    elif (close < lower) and (close2 >= lower2) and (last_signal >= 0) :
        data_df.loc[data_df.index[-1],strategyname+'_signal']  = short
        data_df.loc[data_df.index[-1],strategyname+'_short_entry_price'] = data_df.loc[data_df.index[-1],'close']
        data_df.loc[data_df.index[-1],strategyname+'_contract_num'] =  max(1,round(capital_unit / data_df.loc[data_df.index[-1],'atr_day'] / contractMulti))
        data_df.loc[data_df.index[-1],strategyname+'_short_profitstop_price'] = data_df.loc[data_df.index[-1],strategyname+'_short_entry_price'] - atrn*data_df.loc[data_df.index[-1],'atr_day']
    # 找出多头平仓信号
    elif (close < median) and (close2 >= median2) and (last_signal == 1):
        data_df.loc[data_df.index[-1],strategyname+'_signal']  = 0
        data_df.loc[data_df.index[-1],strategyname+'_contract_num'] = 0
    # 找出空头平仓信号
    elif (close > median) and (close2 <= median2) and (last_signal == -1):
        data_df.loc[data_df.index[-1],strategyname+'_signal']  = 0
        data_df.loc[data_df.index[-1],strategyname+'_contract_num'] = 0
    # 找出多头止盈信号
    elif (data_df.loc[data_df.index[-1],'high'] > data_df.loc[data_df.index[-1],strategyname+'_long_profitstop_price']) and (last_signal == 1):
        data_df.loc[data_df.index[-1],strategyname+'_signal']  = 0
        data_df.loc[data_df.index[-1],strategyname+'_contract_num'] = 0
    # 找出空头止盈信号 
    elif (data_df.loc[data_df.index[-1],'low'] < data_df.loc[data_df.index[-1],strategyname+'_short_profitstop_price']) and (last_signal == -1):
        data_df.loc[data_df.index[-1],strategyname+'_signal']  = 0
        data_df.loc[data_df.index[-1],strategyname+'_contract_num'] = 0
    data_df.loc[data_df.index[-1],strategyname+'_pos'] = data_df.loc[data_df.index[-1],strategyname+'_signal'] 
    data_df = data_df.drop(['median','std'],axis=1)
    if data_df.loc[data_df.index[-1],strategyname+'_signal'] != data_df.loc[data_df.index[-2],strategyname+'_signal']:
        thisBarSignal = strategyname + '策略产生信号：%s'%(data_df.loc[data_df.index[-1],strategyname+'_signal'])
    else:
        thisBarSignal = ''
    return data_df,thisBarSignal

# data_df = portfolio_df_dict['cu99.SHFE']
# strategyname = 'wh2'
# contractMulti = 10
# para=[200,400,1.2,8]
# capital_unit=100   
def whiskySignal(data_df,strategyname,contractMulti,para=[100,200, 1, 6],capital_unit=100,long=1,short=-1):  #long,short指做多做空，如果不做则改为0,如果做空short改为-1
    # ===策略参数
    n = int(para[0])
    fltn = int(para[1])
    m = para[2]
    atrn = para[3]
    # ===计算指标
    # 计算均线
    data_df['median'] = data_df['close'].rolling(n).mean()  # 此处只计算最后几行的均线值，因为没有加min_period参数
    median = data_df.iloc[-1]['median']
    median2 = data_df.iloc[-2]['median']
    # 计算标准差
    data_df['std'] = data_df['close'].rolling(n).std(ddof=0)  # ddof代表标准差自由度，只计算最后几行的均线值，因为没有加min_period参数
    std = data_df.iloc[-1]['std']
    std2 = data_df.iloc[-2]['std']
    # 计算上轨、下轨道
    upper = median + m * std
    lower = median - m * std
    upper2 = median2 + m * std2
    lower2 = median2 - m * std2
    # 计算过滤器指标
    stdvalue = data_df['close'].rolling(fltn, min_periods=1).std(ddof=0)
    stdMA = tb.EMA(stdvalue,4*fltn)
    data_df['stdcdt'] = 0
    data_df.loc[stdvalue <= stdMA*0.75,'stdcdt']=1 
    data_df['filtercdt'] = data_df['stdcdt'].rolling(fltn, min_periods=fltn).sum() >= fltn*0.5
    # ===寻找交易信号
    signal = None
    close = data_df.iloc[-1]['close']
    close2 = data_df.iloc[-2]['close']
    # 查看当前仓位
    last_signal = int(list(data_df[strategyname+'_signal'].dropna())[-1])
    #首先继承之前的仓位
    data_df.loc[data_df.index[-1],strategyname+'_signal'] = data_df.loc[data_df.index[-2],strategyname+'_signal'] 
    data_df.loc[data_df.index[-1],strategyname+'_contract_num'] = data_df.loc[data_df.index[-2],strategyname+'_contract_num'] 
    # 计算止盈指标
    data_df.loc[data_df.index[-1],strategyname+'_long_entry_price'] = data_df.loc[data_df.index[-2],strategyname+'_long_entry_price'] 
    data_df.loc[data_df.index[-1],strategyname+'_short_entry_price'] = data_df.loc[data_df.index[-2],strategyname+'_short_entry_price'] 
    data_df[strategyname+'_long_profitstop_price'] = np.nan
    data_df[strategyname+'_short_profitstop_price'] = np.nan
    if last_signal == 1:
        data_df.loc[data_df.index[-1],strategyname+'_long_profitstop_price'] = data_df.loc[data_df.index[-1],strategyname+'_long_entry_price'] + atrn*data_df.loc[data_df.index[-1],'atr_day']
    elif last_signal == -1:
        data_df.loc[data_df.index[-1],strategyname+'_short_profitstop_price'] = data_df.loc[data_df.index[-1],strategyname+'_short_entry_price'] - atrn*data_df.loc[data_df.index[-1],'atr_day'] 
    # 找出做多信号
    if (close > upper) and (close2 <= upper2) and (last_signal <= 0) and (data_df.iloc[-1]['filtercdt'] == True):
        data_df.loc[data_df.index[-1],strategyname+'_signal']  = long
        data_df.loc[data_df.index[-1],strategyname+'_long_entry_price'] = data_df.loc[data_df.index[-1],'close']
        data_df.loc[data_df.index[-1],strategyname+'_contract_num'] =  max(1,round(capital_unit / data_df.loc[data_df.index[-1],'atr_day'] / contractMulti))
        data_df.loc[data_df.index[-1],strategyname+'_long_profitstop_price'] = data_df.loc[data_df.index[-1],strategyname+'_long_entry_price'] + atrn*data_df.loc[data_df.index[-1],'atr_day']
    # 找出做空信号
    elif (close < lower) and (close2 >= lower2) and (last_signal >= 0) and (data_df.iloc[-1]['filtercdt'] == True):
        data_df.loc[data_df.index[-1],strategyname+'_signal']  = short
        data_df.loc[data_df.index[-1],strategyname+'_short_entry_price'] = data_df.loc[data_df.index[-1],'close']
        data_df.loc[data_df.index[-1],strategyname+'_contract_num'] =  max(1,round(capital_unit / data_df.loc[data_df.index[-1],'atr_day'] / contractMulti))
        data_df.loc[data_df.index[-1],strategyname+'_short_profitstop_price'] = data_df.loc[data_df.index[-1],strategyname+'_short_entry_price'] - atrn*data_df.loc[data_df.index[-1],'atr_day']
    # 找出多头平仓信号
    elif (close < median) and (close2 >= median2) and (last_signal == 1):
        data_df.loc[data_df.index[-1],strategyname+'_signal']  = 0
        data_df.loc[data_df.index[-1],strategyname+'_contract_num'] = 0
    # 找出空头平仓信号
    elif (close > median) and (close2 <= median2) and (last_signal == -1):
        data_df.loc[data_df.index[-1],strategyname+'_signal']  = 0
        data_df.loc[data_df.index[-1],strategyname+'_contract_num'] = 0
    # 找出多头止盈信号
    elif (data_df.loc[data_df.index[-1],'high'] > data_df.loc[data_df.index[-1],strategyname+'_long_profitstop_price']) and (last_signal == 1):
        data_df.loc[data_df.index[-1],strategyname+'_signal']  = 0
        data_df.loc[data_df.index[-1],strategyname+'_contract_num'] = 0
    # 找出空头止盈信号 
    elif (data_df.loc[data_df.index[-1],'low'] < data_df.loc[data_df.index[-1],strategyname+'_short_profitstop_price']) and (last_signal == -1):
        data_df.loc[data_df.index[-1],strategyname+'_signal']  = 0
        data_df.loc[data_df.index[-1],strategyname+'_contract_num'] = 0
    data_df.loc[data_df.index[-1],strategyname+'_pos'] = data_df.loc[data_df.index[-1],strategyname+'_signal'] 
    data_df = data_df.drop(['stdcdt','filtercdt','median','std'],axis=1)
    if data_df.loc[data_df.index[-1],strategyname+'_signal'] != data_df.loc[data_df.index[-2],strategyname+'_signal']:
        thisBarSignal = strategyname + '策略产生信号：%s'%(data_df.loc[data_df.index[-1],strategyname+'_signal'])
    else:
        thisBarSignal = ''
    return data_df,thisBarSignal

 
def tequilaSignal(data_df,strategyname,contractMulti,para=[100,200,50,200,6],capital_unit=100,long=1,short=-1): 
    n = para[0]
    m = para[1]
    z = para[2]
    eman = para[3]
    atrn = para[4]
    # ===计算指标
    #EMA
    data_df['EMA'] = tb.EMA(data_df['close'],eman)
    #diff
    data_df['zb'] = (data_df['close'] - data_df['close'].rolling(eman, min_periods=1).mean())/data_df['close'].rolling(eman, min_periods=1).std(ddof=0)
    #对zb指标进行容错处理（std为0时会出现zb无限大导致指标计算错误）
    data_df.loc[data_df['zb'] == np.inf,'zb'] = np.nan
    data_df.loc[data_df['zb'] == -np.inf,'zb'] = np.nan
    data_df['zb'] = data_df['zb'].fillna(method='ffill')
    data_df['diff'] = tb.EMA(tb.EMA(data_df['zb'],n) - tb.EMA(data_df['zb'],m),z)
    diff = data_df.iloc[-1]['diff']
    diff1 = data_df.iloc[-2]['diff']
    diff2 = data_df.iloc[-3]['diff']
    ema = data_df.iloc[-1]['EMA']
    # ===寻找交易信号
    signal = None
    close = data_df.iloc[-1]['close']
    # 查看当前仓位
    last_signal = int(list(data_df[strategyname+'_signal'].dropna())[-1])
    #首先继承之前的仓位
    data_df.loc[data_df.index[-1],strategyname+'_signal'] = data_df.loc[data_df.index[-2],strategyname+'_signal'] 
    data_df.loc[data_df.index[-1],strategyname+'_contract_num'] = data_df.loc[data_df.index[-2],strategyname+'_contract_num'] 
    # 计算止盈指标
    data_df.loc[data_df.index[-1],strategyname+'_long_entry_price'] = data_df.loc[data_df.index[-2],strategyname+'_long_entry_price']
    data_df.loc[data_df.index[-1],strategyname+'_short_entry_price'] = data_df.loc[data_df.index[-2],strategyname+'_short_entry_price'] 
    data_df[strategyname+'_long_profitstop_price'] = np.nan
    data_df[strategyname+'_short_profitstop_price'] = np.nan
    if last_signal == 1:
        data_df.loc[data_df.index[-1],strategyname+'_long_profitstop_price'] = data_df.loc[data_df.index[-1],strategyname+'_long_entry_price'] + atrn*data_df.loc[data_df.index[-1],'atr_day']
    elif last_signal == -1:
        data_df.loc[data_df.index[-1],strategyname+'_short_profitstop_price'] = data_df.loc[data_df.index[-1],strategyname+'_short_entry_price'] - atrn*data_df.loc[data_df.index[-1],'atr_day']
    #----先计算平仓信号 （因为如果先计算开仓信号，如空翻多时，会造成信号触发1后再次归0，因此先计算平仓信号）
    # 找出多头平仓信号
    if (diff <= diff1) and (diff1 > diff2) and (last_signal == 1):
        data_df.loc[data_df.index[-1],strategyname+'_signal']  = 0
        data_df.loc[data_df.index[-1],strategyname+'_contract_num'] = 0
    # 找出多头止盈信号
    elif (data_df.loc[data_df.index[-1],'high'] > data_df.loc[data_df.index[-1],strategyname+'_long_profitstop_price']) and (last_signal == 1):
        data_df.loc[data_df.index[-1],strategyname+'_signal']  = 0
        data_df.loc[data_df.index[-1],strategyname+'_contract_num'] = 0
    # 找出空头平仓信号
    elif (diff >= diff1) and (diff1 < diff2) and (last_signal == -1):
        data_df.loc[data_df.index[-1],strategyname+'_signal']  = 0
        data_df.loc[data_df.index[-1],strategyname+'_contract_num'] = 0
    # 找出空头止盈信号
    elif (data_df.loc[data_df.index[-1],'low'] < data_df.loc[data_df.index[-1],strategyname+'_short_profitstop_price']) and (last_signal == -1):
        data_df.loc[data_df.index[-1],strategyname+'_signal']  = 0
        data_df.loc[data_df.index[-1],strategyname+'_contract_num'] = 0
    #----在计算开仓信号
    # 找出做多信号
    if (close >= ema) and (diff >= diff1) and (diff1 < diff2) and (last_signal <= 0):
        data_df.loc[data_df.index[-1],strategyname+'_signal']  = long
        data_df.loc[data_df.index[-1],strategyname+'_long_entry_price'] = data_df.loc[data_df.index[-1],'close']
        data_df.loc[data_df.index[-1],strategyname+'_contract_num'] = max(1,round(capital_unit / data_df.loc[data_df.index[-1],'atr_day'] / contractMulti))    
        data_df.loc[data_df.index[-1],strategyname+'_long_profitstop_price'] = data_df.loc[data_df.index[-1],strategyname+'_long_entry_price'] + atrn*data_df.loc[data_df.index[-1],'atr_day']
    # 找出做空信号
    elif (close <= ema) and (diff <= diff1) and (diff1 > diff2)and (last_signal >= 0):
        data_df.loc[data_df.index[-1],strategyname+'_signal']  = short
        data_df.loc[data_df.index[-1],strategyname+'_short_entry_price'] = data_df.loc[data_df.index[-1],'close']
        data_df.loc[data_df.index[-1],strategyname+'_contract_num'] = max(1,round(capital_unit / data_df.loc[data_df.index[-1],'atr_day'] / contractMulti))    
        data_df.loc[data_df.index[-1],strategyname+'_short_profitstop_price'] = data_df.loc[data_df.index[-1],strategyname+'_short_entry_price'] - atrn*data_df.loc[data_df.index[-1],'atr_day']
    data_df.loc[data_df.index[-1],strategyname+'_pos'] = data_df.loc[data_df.index[-1],strategyname+'_signal']    
    data_df = data_df.drop(['EMA','zb','diff'],axis=1)
    if data_df.loc[data_df.index[-1],strategyname+'_signal'] != data_df.loc[data_df.index[-2],strategyname+'_signal']:
        thisBarSignal = strategyname + '策略产生信号：%s'%(data_df.loc[data_df.index[-1],strategyname+'_signal'])
    else:
        thisBarSignal = ''
    return data_df,thisBarSignal

