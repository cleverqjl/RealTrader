path = r'D:\CTATrade'
data_path = r'D:\futureData'
import sys
sys.path.append(path)
sys.path.append(data_path)
import pandas as pd
from datetime import timedelta
import talib as tb
from strategy import brandy,tequila,whisky,whiskyMix
def backtestStrategy(df,
                     strategy,
                     minpoint,
                     para,
                     c_rate ,
                     drop_days,
                     slippageMulti,
                     contractMulti,
                     capitalmode,
                     capital_unit=100,
                     multiCompute=False,
                     limitValue=None,
                     margin_percent=0.12,
                     long=1,
                     short=-1
                      ):
    # #设定策略参数test
    # symbol = 'j99.DCE'
    # time_interval = '10m'
    # symbol_h5 = '_'.join([symbol.split('.')[0],time_interval])   #对应的h5文件名
    # # para = [400, 1.5, 8]    #策略参数
    # para = [100, 200, 50, 200, 8] 
    # # para = [100,200,1,60] 
    # # para = [300,500,1.5,8]
    # c_rate = 1 / 10000  #手续费
    # drop_days = 20     # 刚刚上线10天内不交易
    # slippageMulti = 1  #滑点
    # contractMulti = 10 #合约乘数
    # minpoint = 0.5
    # strategy = tequila
    # multiCompute=False
    # limitValue=None
    # capitalmode = 'atr'
    # capital_unit = 3200
    # margin_percent=0.12 
    # # 读入h5数据
    # df = pd.read_hdf(data_path+'/h5_Data_trade/%s.h5' % symbol_h5, key='df')
    """
    #对比数据差异用
    df2 = pd.read_hdf(data_path+'/h5_Data_trade/%s.h5' % symbol_h5, key='df')
    df = df.set_index('candle_begin_time')
    df2 = df2.set_index('candle_begin_time')
    pd.concat([df['20220509':'20220509']['close'],df2['20220509':'20220509']['close']],axis=1)
    """
    
    '''合成分钟及日线数据'''
    # 任何原始数据读入都进行一下排序、去重，以防万一
    df.sort_values(by=['candle_begin_time'], inplace=True)
    df.drop_duplicates(subset=['candle_begin_time'], inplace=True)
    df.reset_index(inplace=True, drop=True)
    # 对行情数据进行格式化处理(暂不进行格式化处理，实际影响不大，保留小数位反而还原精细化程度)
    # for i in ['high','open','low','close']:
    #     df[i] = (df[i]/minpoint).round()*minpoint
    # 转换为日线数据
    period_df_day = df.groupby('trading_date').agg(
                                                    {'open': 'first',
                                                      'high': 'max',
                                                      'low': 'min',
                                                      'close': 'last',
                                                      'volume': 'sum',
                                                      })
    period_df_day.dropna(subset=['open'], inplace=True)  # 去除一天都没有交易的周期
    # period_df_day = period_df_day[period_df_day['volume'] > 0]  # 去除成交量为0的交易周期(暂不去除，因为有可能一字板)
    period_df_day.reset_index(inplace=True)
    period_df_day['candle_begin_time'] = period_df_day['trading_date']
    day_df = period_df_day[['candle_begin_time', 'open', 'high', 'low', 'close', 'volume']]
    #day_df = day_df[day_df['candle_begin_time'] >= pd.to_datetime('2017-01-01')]
    day_df.reset_index(inplace=True, drop=True)

    '''计算ATR'''
    # 分钟线
    df['atr'] = tb.ATR(df['high'],df['low'],df['close'],1000)
    # 日线atr
    day_df['atr'] = tb.ATR(day_df['high'],day_df['low'],day_df['close'],20)
    day_df = day_df.set_index('candle_begin_time')
    day_df.index = pd.to_datetime(day_df.index)
    
    '''计算交易信号'''
    df = strategy(df,day_df,para=para,long=long,short=short)
    df['signal'].fillna(method='ffill', inplace=True)
    df['signal'].fillna(value=0, inplace=True)  # 将初始行数的signal补全为0
    df['pos'] = df['signal'].shift(1)
    df['pos'].fillna(value=0, inplace=True)  # 将初始行数的pos补全为0
    df['pos'].fillna(method='ffill', inplace=True)
    # df[df['signal']!=df['signal'].shift(1)][['candle_begin_time','signal']]
    # aaa=df.tail(500)
    '''计算资金曲线'''
    # 选取相关时间,drop_days天之后的日期
    t = df.iloc[0]['candle_begin_time'] + timedelta(days=drop_days)
    df = pd.DataFrame(df[df['candle_begin_time'] > t])
    # 计算下一根k线开盘价（计算平仓价格用）
    df['next_open'] = df['open'].shift(-1)  # 下根K线的开盘价
    df['next_open'].fillna(value=df['close'], inplace=True)
    # 找出开仓的k线
    condition1 = df['pos'] != 0  # 当前周期不为空仓
    condition2 = df['pos'] != df['pos'].shift(1)  # 当前周期和上个周期持仓方向不一样。
    open_pos_condition = condition1 & condition2
    # 找出平仓的k线
    condition1 = df['pos'] != 0  # 当前周期不为空仓
    condition2 = df['pos'] != df['pos'].shift(-1).fillna(method='ffill')  # 当前周期和下个周期持仓方向不一样。
    close_pos_condition = condition1 & condition2    
    # 滑点
    slippage = minpoint * slippageMulti
    # 开仓价格：理论开盘价加上相应滑点
    df.loc[open_pos_condition, 'open_pos_price'] = df['open'] + slippage*df['pos']
    # 平仓价格
    df.loc[close_pos_condition, 'close_pos_price'] = df['next_open'] - slippage*df['pos']
    # 计算仓位 
        #复利计算
    if multiCompute == True:
        df_mtp = df[open_pos_condition | close_pos_condition].copy()
        df_mtp['open_pos_price'].fillna(method='ffill',inplace=True)
        df_mtp['close_pos_price'].fillna(method='bfill',inplace=True)
        df_mtp.loc[df['signal']==0,'multiply']=df_mtp.loc[df['signal']==0]['close_pos_price']/df_mtp.loc[df['signal']==0]['open_pos_price']
        df.loc[df_mtp['multiply'].dropna().index,'multiply_cpd'] = df_mtp['multiply'].dropna().cumprod()
        df['multiply_cpd'].fillna(method='ffill',inplace=True)
    elif multiCompute == False:
        df['multiply_cpd']=1
        #atr计算
    if capitalmode == 'atr':
        if limitValue == None:
            df.loc[open_pos_condition, 'contract_num'] = capital_unit * df['multiply_cpd'] / df['atr_day'] / contractMulti
        else:
            df.loc[open_pos_condition, 'atr_day'] = df[open_pos_condition]['candle_begin_time'].apply(lambda x:day_df['atr'].shift(1)[x.strftime('%Y/%m/%d')])
            df.loc[open_pos_condition, 'contract_num1'] = capital_unit * df['multiply_cpd']  /  df['atr_day'] / contractMulti
            df.loc[open_pos_condition, 'contract_num2'] = limitValue * df['multiply_cpd'] /  df['open_pos_price'] / contractMulti
            df.loc[open_pos_condition, 'contract_num'] = [min(list(df.loc[open_pos_condition, 'contract_num1'])[i],list(df.loc[open_pos_condition, 'contract_num2'])[i]) for i in range(len(df[open_pos_condition]))]
            df.drop(['contract_num1','contract_num2'],axis=1,inplace=True)
        #固定手数计算
    elif capitalmode == 'contractnum':
        df.loc[open_pos_condition, 'contract_num'] = capital_unit
        #市值计算
    elif capitalmode == 'marketValue':
        df.loc[open_pos_condition, 'contract_num'] = capital_unit /  df['open_pos_price'] / contractMulti
    # 头寸取整
    df.loc[open_pos_condition, 'contract_num'] = [max(i,1) for i in round(df.loc[open_pos_condition, 'contract_num'])]
    #计算仓位占用
    df.loc[open_pos_condition, 'capital_use'] = df['open_pos_price'] * df['contract_num'] * contractMulti * margin_percent
    df['capital_use'].fillna(method='ffill',inplace=True)
    df['contract_num'].fillna(method='ffill',inplace=True)
    df.loc[df['pos'] == 0, ['contract_num']] = 0
    df.loc[df['pos'] == 0, ['capital_use']] = 0
    df['contract_num'].fillna(0,inplace=True)
    df['capital_use'].fillna(0,inplace=True)
    # 盈利计算(加上滑点耗损)
    df['profit']=df['contract_num']*(df['close']-df['close'].shift(1))*df['pos']*contractMulti
    df.loc[open_pos_condition, 'profit']=df['contract_num']*(df['close']-df['open_pos_price'])*df['pos']*contractMulti
    df.loc[close_pos_condition, 'profit']=df['contract_num']*(df['close_pos_price']-df['close'].shift(1))*df['pos']*contractMulti
    # 手续费计算
    df.loc[open_pos_condition, 'fee'] = df[open_pos_condition]['contract_num']*df[open_pos_condition]['open_pos_price']*c_rate*contractMulti
    df.loc[close_pos_condition, 'fee'] = df[close_pos_condition]['contract_num']*df[close_pos_condition]['close_pos_price']*c_rate*contractMulti
    df['fee'].fillna(0,inplace=True)
    # 净盈利计算
    df['net_profit']=df['profit']-df['fee'] #-df['slip']
    # 累计盈亏计算
    df['profit_cum']=df['net_profit'].cumsum()
    df = df.set_index('candle_begin_time')
#    df['20140821':]['profit_cum'].plot(figsize=(12,7))
    # a=df.tail(100)
    return df

# strategy_dict ={'wh1':{'strategy':whisky,
#                         'para':[100,200,1,6],
#                         'capital_unit':10000},
#                 'wh2':{'strategy':whisky,
#                         'para':[200,400,1.2,8],
#                         'capital_unit':10000},
#                 'wh3':{'strategy':whisky,
#                         'para':[300,500,1.5,8],
#                         'capital_unit':10000},
#                 'tequila':{'strategy':tequila,
#                         'para':[100,200,50,200,4],
#                         'capital_unit':10000},
#                 }

def backtestStrategies(  df,
                         strategy_dict,
                         minpoint,
                         c_rate ,
                         drop_days,
                         slippageMulti,
                         contractMulti,
                         capitalmode,
                         multiCompute=False,
                         limitValue=None,
                         method='contract_sum',
                         margin_percent=0.15,):
    backtest_df_dict = {}
    '''处理数据'''
     # 任何原始数据读入都进行一下排序、去重，以防万一
    df.sort_values(by=['candle_begin_time'], inplace=True)
    df.drop_duplicates(subset=['candle_begin_time'], inplace=True)
    df.reset_index(inplace=True, drop=True)
    # 转换为日线数据
    period_df_day = df.groupby('trading_date').agg(
                                                    {'open': 'first',
                                                      'high': 'max',
                                                      'low': 'min',
                                                      'close': 'last',
                                                      'volume': 'sum',
                                                      })
    period_df_day.dropna(subset=['open'], inplace=True)  # 去除一天都没有交易的周期
    # period_df_day = period_df_day[period_df_day['volume'] > 0]  # 去除成交量为0的交易周期(暂不去除，因为有可能一字板)
    period_df_day.reset_index(inplace=True)
    period_df_day['candle_begin_time'] = period_df_day['trading_date']
    day_df = period_df_day[['candle_begin_time', 'open', 'high', 'low', 'close', 'volume']]
    #day_df = day_df[day_df['candle_begin_time'] >= pd.to_datetime('2017-01-01')]
    day_df.reset_index(inplace=True, drop=True)
    '''计算ATR'''
    # 分钟线
    df['atr'] = tb.ATR(df['high'],df['low'],df['close'],1000)
    # 日线atr
    day_df['atr'] = tb.ATR(day_df['high'],day_df['low'],day_df['close'],20)
    day_df = day_df.set_index('candle_begin_time')
    day_df.index = pd.to_datetime(day_df.index)
    # 回测各策略
    for i in list(strategy_dict.keys()):
        strategy = strategy_dict[i]['strategy'][0]
        para = strategy_dict[i]['para']
        capital_unit = strategy_dict[i]['capital_unit']
        data_df = pd.DataFrame(df.copy())
        long = strategy_dict[i]['long'] 
        short = strategy_dict[i]['short'] 
        backtest_df_dict[i] = backtestStrategy(  data_df,
                                                 strategy,
                                                 minpoint,
                                                 para,
                                                 c_rate ,
                                                 drop_days,
                                                 slippageMulti,
                                                 contractMulti,
                                                 capitalmode,
                                                 capital_unit,
                                                 multiCompute,
                                                 limitValue,
                                                 margin_percent,
                                                 long,
                                                 short
                                                  )
        print('回测%s策略完成'%i)
    '''分别计算信号'''
    # 统计个策略信号/仓位/持仓数量
    backtest_df = pd.DataFrame()
    for i in list(backtest_df_dict.keys()):
        backtest_df[['open','high','low','close']] = backtest_df_dict[i][['open','high','low','close']]
        backtest_df[i+'_signal'] = backtest_df_dict[i]['signal']
        backtest_df[i+'_pos'] = backtest_df_dict[i]['pos']
        backtest_df[i+'_contract_num'] = backtest_df_dict[i]['contract_num'] 
    backtest_df['contract_num_drt'] = 0
    backtest_df['pos'] = 0 
    # 按照不同方法计算合约数量contract_num_drt
    if method == 'contract_sum':
        for i in list(backtest_df_dict.keys()):
            backtest_df['pos'] += backtest_df[i+'_pos']
            backtest_df['contract_num_drt'] += backtest_df[i+'_contract_num'] * backtest_df[i+'_pos'] 
    elif method == 'pos_max':    
        backtest_df['capital_pos_max'] = 0
        for i in list(backtest_df_dict.keys()):
           backtest_df['pos'] += backtest_df[i+'_pos']
           backtest_df['capital_pos'] = backtest_df[i+'_pos'] * strategy_dict[i]['capital_unit']
           backtest_df['capital_pos_max'] = backtest_df[['capital_pos_max','capital_pos']].apply(lambda x:x.max(),axis=1)
        backtest_df.loc[backtest_df['pos']>0,'pos'] = 1
        backtest_df.loc[backtest_df['pos']<0,'pos'] = -1
        # 找出开仓的k线
        condition1 = backtest_df['pos'] != 0  # 当前周期不为空仓
        condition2 = backtest_df['pos'] != backtest_df['pos'].shift(1)  # 当前周期和上个周期持仓方向不一样。
        open_pos_condition = condition1 & condition2
        backtest_df['candle_begin_time'] = backtest_df.index
        backtest_df.loc[open_pos_condition, 'atr_day'] = backtest_df[open_pos_condition]['candle_begin_time'].apply(lambda x:day_df['atr'].shift(1)[x.strftime('%Y/%m/%d')])
        # backtest_df.loc[open_pos_condition, 'contract_num'] = capital_unit / backtest_df.loc[open_pos_condition, 'atr_day'] / contractMulti
        backtest_df.loc[open_pos_condition, 'contract_num'] = backtest_df.loc[open_pos_condition, 'capital_pos_max'] / backtest_df.loc[open_pos_condition, 'atr_day'] / contractMulti
        # 头寸取整
        backtest_df.loc[open_pos_condition, 'contract_num'] = [max(i,1) for i in round(backtest_df.loc[open_pos_condition, 'contract_num'])]
        #计算仓位占用
        backtest_df['contract_num'].fillna(method='ffill',inplace=True)
        backtest_df.loc[backtest_df['pos'] == 0, ['contract_num']] = 0
        backtest_df['contract_num'].fillna(0,inplace=True)
        backtest_df['contract_num_drt'] = backtest_df['contract_num'] * backtest_df['pos']
    elif method == 'capital_unit_sum':
        backtest_df['capital_unit_sum'] = 0
        for i in list(backtest_df_dict.keys()):
            backtest_df['pos'] += backtest_df[i+'_pos']
            backtest_df['capital_unit_sum'] += backtest_df[i+'_pos']*strategy_dict[i]['capital_unit']
        # 找出开仓的k线
        condition1 = backtest_df['pos'] != 0  # 当前周期不为空仓
        condition2 = backtest_df['pos'] != backtest_df['pos'].shift(1)  # 当前周期和上个周期持仓方向不一样。
        open_pos_condition = condition1 & condition2
        backtest_df['candle_begin_time'] = backtest_df.index
        backtest_df.loc[open_pos_condition, 'atr_day'] = backtest_df[open_pos_condition]['candle_begin_time'].apply(lambda x:day_df['atr'].shift(1)[x.strftime('%Y/%m/%d')])
        # backtest_df.loc[open_pos_condition, 'contract_num'] = capital_unit * abs(backtest_df.loc[open_pos_condition, 'pos']) / backtest_df.loc[open_pos_condition, 'atr_day'] / contractMulti
        backtest_df.loc[open_pos_condition, 'contract_num'] = abs(backtest_df.loc[open_pos_condition, 'capital_unit_sum']) / backtest_df.loc[open_pos_condition, 'atr_day'] / contractMulti
        # 头寸取整
        backtest_df.loc[open_pos_condition, 'contract_num'] = [max(i,1) for i in round(backtest_df.loc[open_pos_condition, 'contract_num'])]
        #计算仓位占用
        backtest_df['contract_num'].fillna(method='ffill',inplace=True)
        backtest_df.loc[backtest_df['pos'] == 0, ['contract_num']] = 0
        backtest_df['contract_num'].fillna(0,inplace=True)
        backtest_df.loc[backtest_df['pos']>0,'direction'] = 1
        backtest_df.loc[backtest_df['pos']<0,'direction'] = -1
        backtest_df.loc[backtest_df['pos']==0,'direction'] = 0
        backtest_df['contract_num_drt'] = backtest_df['contract_num'] * backtest_df['direction']
    #计算仓位占用
    backtest_df['capital_use'] = abs(backtest_df['high'] * backtest_df['contract_num_drt'] * contractMulti * margin_percent)
    backtest_df['capital_use'].fillna(0,inplace=True)
    # 盈利计算(加上滑点耗损)
    backtest_df['profit']=backtest_df['contract_num_drt']*(backtest_df['close']-backtest_df['close'].shift(1))*contractMulti
    # 滑点计算
    backtest_df.loc[(backtest_df['contract_num_drt']-backtest_df['contract_num_drt'].shift(1)).fillna(0)!=0,'slippage'] = abs(minpoint*contractMulti*slippageMulti*(backtest_df['contract_num_drt']-backtest_df['contract_num_drt'].shift(1)))
    backtest_df['slippage'].fillna(0,inplace=True)
    # 手续费计算
    backtest_df.loc[(backtest_df['contract_num_drt']-backtest_df['contract_num_drt'].shift(1)).fillna(0)!=0, 'fee'] = abs((backtest_df['contract_num_drt']-backtest_df['contract_num_drt'].shift(1))*backtest_df['open']*c_rate*contractMulti)
    backtest_df['fee'].fillna(0,inplace=True)
    # 净盈利计算
    backtest_df['net_profit']=backtest_df['profit'] - backtest_df['fee'] - backtest_df['slippage']
    # 累计盈亏计算
    backtest_df['profit_cum']=backtest_df['net_profit'].cumsum()
    # backtest_df['net_profit'].cumsum().plot(figsize=(12,7))
    return backtest_df

    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
                      
    

    