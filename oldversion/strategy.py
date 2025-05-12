path = r'D:\CTATrade'
import sys
sys.path.append(path)
# sys.path.append(r'C:\vnstudio\Lib\site-packages')
import pandas as pd
import numpy as np
import talib as tb

# brandy策略
def brandy(df,day_df,para=[400, 1.5, 8],long=1,short=-1):  #long,short指做多做空，如果不做则改为0,如果做空short改为-1
    # ===策略参数
    n = int(para[0])
    m = para[1]
    atrn = para[2]

    # ===计算指标
    # 计算均线
    df['median'] = df['close'].rolling(n, min_periods=1).mean()
    # 计算上轨、下轨道
    df['std'] = df['close'].rolling(n, min_periods=1).std(ddof=0)  # ddof代表标准差自由度
    df['upper'] = df['median'] + m * df['std']
    df['lower'] = df['median'] - m * df['std']
    # ===计算信号
    # 找出做多信号
    condition1 = df['close'] > df['upper']  # 当前K线的收盘价 > 上轨
    condition2 = df['close'].shift(1) <= df['upper'].shift(1)  # 之前K线的收盘价 <= 上轨
    df.loc[condition1 & condition2, 'signal_long'] = long  # 将产生做多信号的那根K线的signal设置为1，1代表做多
    # 找出做多平仓信号
    condition1 = df['close'] < df['median']  # 当前K线的收盘价 < 中轨
    condition2 = df['close'].shift(1) >= df['median'].shift(1)  # 之前K线的收盘价 >= 中轨
    df.loc[condition1 & condition2, 'signal_long'] = 0  # 将产生平仓信号当天的signal设置为0，0代表平仓
    df.loc[df.index[0],'signal_long'] = 0
    
    # =====多头止盈信号
    '''
    # 找出多头开仓信号位置
    df_long = df[df['signal_long'].notnull()]
    df_long = df_long[(df_long['signal_long'] == 1) & (df_long['signal_long'].shift(1) == 0)]
    df.loc[df_long.index,'long_entry_price'] = df['close']
    df['long_entry_price'].fillna(method='ffill',inplace=True)
    # 该位置收盘价加上8倍日线atr
    long_position = df['signal_long'].fillna(method='ffill') == long
    if df.loc[long_position].empty == False:
        df.loc[long_position, 'atr_day'] = df[long_position]['candle_begin_time'].apply(lambda x:day_df['atr'].shift(1)[x.strftime('%Y/%m/%d')])
    else:
        df.loc[long_position, 'atr_day'] = np.nan
    df['long_profitstop_price'] = df['long_entry_price'] + atrn * df['atr_day']
    # 如果最高价大于止盈价就平仓
    df.loc[df['high'] > df['long_profitstop_price'],'signal_long'] = 0
    '''
    # 找出多头开仓信号位置
    entry_num = 0
    profitstop_num = 0
    atr_compute = False
    while True:
        df_long = df[df['signal_long'].notnull()]
        df_long = df_long[(df_long['signal_long'] == 1) & (df_long['signal_long'].shift(1) != 1)]
        df['long_entry_price'] = np.nan
        df.loc[df_long.index,'long_entry_price'] = df['close']
        df['long_entry_price'].fillna(method='ffill',inplace=True)
        # 该位置收盘价加上8倍日线atr
        if atr_compute == False:
            long_position = df['signal_long'].fillna(method='ffill') == 1
            if df.loc[long_position].empty == False:
                df.loc[long_position, 'atr_day'] = df[long_position]['candle_begin_time'].apply(lambda x:day_df['atr'].shift(1)[x.strftime('%Y/%m/%d')])
            else:
                df.loc[long_position, 'atr_day'] = np.nan
            atr_compute = True
        df['long_profitstop_price'] = df['long_entry_price'] + atrn * df['atr_day']
        # 如果最高价大于止盈价就平仓
        df.loc[(df['high'] >= df['long_profitstop_price']) & (df['signal_long']!=1),'signal_long'] = 2
        # 需要二次处理
        df.loc[(df['high'] < df['long_profitstop_price']) & (df['signal_long']==2),'signal_long'] = np.nan
        
        # 验证是否处理完成
        if (len(df_long) == entry_num) and (len(df[df['signal_long']==2]) == profitstop_num):
            break
        entry_num = len(df_long)
        profitstop_num = len(df[df['signal_long']==2])
        # print(entry_num,profitstop_num)
        # a=df.tail(3000)
    df.loc[df['signal_long'] == 2,'signal_long'] = 0
    
    # 找出做空信号
    condition1 = df['close'] < df['lower']  # 当前K线的收盘价 < 下轨
    condition2 = df['close'].shift(1) >= df['lower'].shift(1)  # 之前K线的收盘价 >= 下轨
    df.loc[condition1 & condition2 , 'signal_short'] = short  # 将产生做空信号的那根K线的signal设置为-1，-1代表做空
    # 找出做空平仓信号
    condition1 = df['close'] > df['median']  # 当前K线的收盘价 > 中轨
    condition2 = df['close'].shift(1) <= df['median'].shift(1)  # 之前K线的收盘价 <= 中轨
    df.loc[condition1 & condition2, 'signal_short'] = 0  # 将产生平仓信号当天的signal设置为0，0代表平仓
    # =====空头止盈信号
    '''
    # 找出空头开仓信号位置
    df_short = df[df['signal_short'].notnull()]
    df_short = df_short[(df_short['signal_short'] == -1) & (df_short['signal_short'].shift(1) == 0)]
    df.loc[df_short.index,'short_entry_price'] = df['close']
    df['short_entry_price'].fillna(method='ffill',inplace=True)
    # 该位置收盘价减去8倍日线atr
    short_position = df['signal_short'].fillna(method='ffill') == short
    if df.loc[short_position].empty == False:
        df.loc[short_position, 'atr_day'] = df[short_position]['candle_begin_time'].apply(lambda x:day_df['atr'].shift(1)[x.strftime('%Y/%m/%d')])
    else:
        df.loc[short_position, 'atr_day'] = np.nan
    df['short_profitstop_price'] = df['short_entry_price'] - atrn * df['atr_day']
    # 如果最高价大于止盈价就平仓
    df.loc[df['low'] < df['short_profitstop_price'],'signal_short'] = 0
    '''
    # 找出空头开仓信号位置
    entry_num = 0
    profitstop_num = 0
    atr_compute = False
    while True:
        df_short = df[df['signal_short'].notnull()]
        df_short = df_short[(df_short['signal_short'] == -1) & (df_short['signal_short'].shift(1) != -1)]
        df['short_entry_price'] = np.nan
        df.loc[df_short.index,'short_entry_price'] = df['close']
        df['short_entry_price'].fillna(method='ffill',inplace=True)
        # 该位置收盘价减去8倍日线atr
        if atr_compute == False:
            short_position = df['signal_short'].fillna(method='ffill') == -1
            if df.loc[short_position].empty == False: 
                df.loc[short_position, 'atr_day'] = df[short_position]['candle_begin_time'].apply(lambda x:day_df['atr'].shift(1)[x.strftime('%Y/%m/%d')])
            else: 
                df.loc[short_position, 'atr_day'] = np.nan
            atr_compute = True
        df['short_profitstop_price'] = df['short_entry_price'] - atrn * df['atr_day']
        # 如果最高价大于止盈价就平仓
        df.loc[(df['low'] <= df['short_profitstop_price']) & (df['signal_short']!=-1),'signal_short'] = -2
        # 需要二次处理
        df.loc[(df['low'] > df['short_profitstop_price']) & (df['signal_short']==-2),'signal_short'] = np.nan
        
        # 验证是否处理完成
        if (len(df_short) == entry_num) and (len(df[df['signal_short']==-2]) == profitstop_num):
            break
        entry_num = len(df_short)
        profitstop_num = len(df[df['signal_short']==-2])
#        print(entry_num,profitstop_num)
    df.loc[df['signal_short'] == -2,'signal_short'] = 0
    
    # 合并做多做空信号，去除重复信号
    df['signal'] = df[['signal_long','signal_short']].sum(axis=1, min_count=1, skipna=True) 
#    df['signal'] = df[['signal_long', 'signal_short']].sum(axis=1, min_count=1, skipna=True)  # 若你的pandas版本是最新的，请使用本行代码代替上面一行
    temp = df[df['signal'].notnull()][['signal']]
    temp = temp[temp['signal'] != temp['signal'].shift(1)]
    df['signal'] = temp['signal']
    a=df.tail(100)
    df.drop(['median', 'std', 'upper', 'lower', 'signal_long','signal_short'], axis=1, inplace=True)
    return df

# buyandhold策略
def buyandhold(df,day_df,para=['2017-01-01']):
    startday = para[0]
    openposbar = df[df['candle_begin_time']>startday].index[0]
    df.loc[openposbar,'signal'] = 1
    df['signal'].fillna(method='ffill',inplace=True)    
    return df

#tequila策略
def tequila(df,day_df,para=[100, 200, 50, 200, 4],long=1,short=-1):  #long,short指做多做空，如果不做则改为0,如果做空short改为-1
    # ===策略参数
    n = para[0]
    m = para[1]
    z = para[2]
    eman = para[3]
    atrn = para[4]

    '''计算指标'''
    #EMA
    df['EMA'] = tb.EMA(df['close'],eman)
    #diff
    df['zb'] = (df['close'] - df['close'].rolling(eman, min_periods=1).mean())/df['close'].rolling(eman, min_periods=1).std(ddof=0)
    #----对zb指标进行容错处理（std为0时会出现zb无限大导致指标计算错误）
    df.loc[df['zb'] == np.inf,'zb'] = np.nan
    df.loc[df['zb'] == -np.inf,'zb'] = np.nan
    df['zb'] = df['zb'].fillna(method='ffill')
    df['diff'] = tb.EMA(tb.EMA(df['zb'],n) - tb.EMA(df['zb'],m),z)
    
    '''计算信号'''
    # 找出做多信号
    condition1 = df['close'] >= df['EMA']  
    condition2 = df['diff'] >= df['diff'].shift(1)
    condition3 = df['diff'].shift(1) < df['diff'].shift(2)
    df.loc[condition1 & condition2 & condition3, 'signal_long'] = long  # 将产生做多信号的那根K线的signal设置为1，1代表做多
    # 找出做多平仓信号
    condition1 = df['diff'] <= df['diff'].shift(1)  
    condition2 = df['diff'].shift(1) > df['diff'].shift(2)
    df.loc[condition1 & condition2, 'signal_long'] = 0  # 将产生平仓信号当天的signal设置为0，0代表平仓
    df.loc[df.index[0],'signal_long'] = 0
    # =====多头止盈信号
    # 找出多头开仓信号位置
    entry_num = 0
    profitstop_num = 0
    atr_compute = False
    while True:
        df_long = df[df['signal_long'].notnull()]
        df_long = df_long[(df_long['signal_long'] == 1) & (df_long['signal_long'].shift(1) != 1)]
        df['long_entry_price'] = np.nan
        df.loc[df_long.index,'long_entry_price'] = df['close']
        df['long_entry_price'].fillna(method='ffill',inplace=True)
        # 该位置收盘价加上8倍日线atr
        if atr_compute == False:
            long_position = df['signal_long'].fillna(method='ffill') == 1
            if df.loc[long_position].empty == False:
                df.loc[long_position, 'atr_day'] = df[long_position]['candle_begin_time'].apply(lambda x:day_df['atr'].shift(1)[x.strftime('%Y/%m/%d')])
            else:
                df.loc[long_position, 'atr_day'] = np.nan
            atr_compute = True
        df['long_profitstop_price'] = df['long_entry_price'] + atrn * df['atr_day']
        # 如果最高价大于止盈价就平仓
        df.loc[(df['high'] >= df['long_profitstop_price']) & (df['signal_long']!=1),'signal_long'] = 2
        # 需要二次处理
        df.loc[(df['high'] < df['long_profitstop_price']) & (df['signal_long']==2),'signal_long'] = np.nan
        
        # 验证是否处理完成
        if (len(df_long) == entry_num) and (len(df[df['signal_long']==2]) == profitstop_num):
            break
        entry_num = len(df_long)
        profitstop_num = len(df[df['signal_long']==2])
        # print(entry_num,profitstop_num)
        # a=df.tail(200)
    df.loc[df['signal_long'] == 2,'signal_long'] = 0
    
    # 找出做空信号
    condition1 = df['close'] <= df['EMA']  
    condition2 = df['diff'] <= df['diff'].shift(1)
    condition3 = df['diff'].shift(1) > df['diff'].shift(2)
    df.loc[condition1 & condition2 & condition3, 'signal_short'] = short  # 将产生做多信号的那根K线的signal设置为1，1代表做多
    # 找出做空平仓信号
    condition1 = df['diff'] >= df['diff'].shift(1)  
    condition2 = df['diff'].shift(1) < df['diff'].shift(2)
    df.loc[condition1 & condition2, 'signal_short'] = 0  # 将产生平仓信号当天的signal设置为0，0代表平仓
    df.loc[df.index[0],'signal_short'] = 0
    # =====空头止盈信号
    # 找出空头开仓信号位置
    entry_num = 0
    profitstop_num = 0
    atr_compute = False
    while True:
        df_short = df[df['signal_short'].notnull()]
        df_short = df_short[(df_short['signal_short'] == -1) & (df_short['signal_short'].shift(1) != -1)]
        df['short_entry_price'] = np.nan
        df.loc[df_short.index,'short_entry_price'] = df['close']
        df['short_entry_price'].fillna(method='ffill',inplace=True)
        # 该位置收盘价减去8倍日线atr
        if atr_compute == False:
            short_position = df['signal_short'].fillna(method='ffill') == -1
            if df.loc[short_position].empty == False: 
                df.loc[short_position, 'atr_day'] = df[short_position]['candle_begin_time'].apply(lambda x:day_df['atr'].shift(1)[x.strftime('%Y/%m/%d')])
            else: 
                df.loc[short_position, 'atr_day'] = np.nan
            atr_compute = True
        df['short_profitstop_price'] = df['short_entry_price'] - atrn * df['atr_day']
        # 如果最高价大于止盈价就平仓
        df.loc[(df['low'] <= df['short_profitstop_price']) & (df['signal_short']!=-1),'signal_short'] = -2
        # 需要二次处理
        df.loc[(df['low'] > df['short_profitstop_price']) & (df['signal_short']==-2),'signal_short'] = np.nan
        
        # 验证是否处理完成
        if (len(df_short) == entry_num) and (len(df[df['signal_short']==-2]) == profitstop_num):
            break
        entry_num = len(df_short)
        profitstop_num = len(df[df['signal_short']==-2])
#        print(entry_num,profitstop_num)
    df.loc[df['signal_short'] == -2,'signal_short'] = 0
    
    # 合并做多做空信号，去除重复信号
    df['signal'] = df[['signal_long','signal_short']].sum(axis=1, min_count=1, skipna=True) 
    temp = df[df['signal'].notnull()][['signal']]
    temp = temp[temp['signal'] != temp['signal'].shift(1)]
    df['signal'] = temp['signal']
    df.drop(['EMA', 'diff'], axis=1, inplace=True)
    return df

# whisky策略
def whisky(df,day_df,para=[100,200, 1, 6],long=1,short=-1):  #long,short指做多做空，如果不做则改为0,如果做空short改为-1
    # ===策略参数
    n = int(para[0])
    fltn = int(para[1])
    m = para[2]
    atrn = para[3]

    # ===计算指标
    # 计算均线
    df['median'] = df['close'].rolling(n, min_periods=1).mean()
    # 计算上轨、下轨道
    df['std'] = df['close'].rolling(n, min_periods=1).std(ddof=0)  # ddof代表标准差自由度
    df['upper'] = df['median'] + m * df['std']
    df['lower'] = df['median'] - m * df['std']
    # 计算过滤器指标
    stdvalue = df['close'].rolling(fltn, min_periods=1).std(ddof=0)
    stdMA = tb.EMA(stdvalue,4*fltn)
    df['stdcdt'] = 0
    df.loc[stdvalue <= stdMA*0.75,'stdcdt']=1 
    df['filtercdt'] = df['stdcdt'].rolling(fltn, min_periods=fltn).sum() >= fltn*0.5
    
    # ===计算信号
    # 找出做多信号
    condition1 = df['close'] > df['upper']  # 当前K线的收盘价 > 上轨
    condition2 = df['close'].shift(1) <= df['upper'].shift(1)  # 之前K线的收盘价 <= 上轨
    condition3 = df['filtercdt'] == True
    df.loc[condition1 & condition2 & condition3, 'signal_long'] = long  # 将产生做多信号的那根K线的signal设置为1，1代表做多
    # 找出做多平仓信号
    condition1 = df['close'] < df['median']  # 当前K线的收盘价 < 中轨
    condition2 = df['close'].shift(1) >= df['median'].shift(1)  # 之前K线的收盘价 >= 中轨
    df.loc[condition1 & condition2, 'signal_long'] = 0  # 将产生平仓信号当天的signal设置为0，0代表平仓
    df.loc[df.index[0],'signal_long'] = 0
    
    # =====多头止盈信号
    # 找出多头开仓信号位置
    entry_num = 0
    profitstop_num = 0
    atr_compute = False
    while True:
        df_long = df[df['signal_long'].notnull()]
        df_long = df_long[(df_long['signal_long'] == 1) & (df_long['signal_long'].shift(1) != 1)]
        df['long_entry_price'] = np.nan
        df.loc[df_long.index,'long_entry_price'] = df['close']
        df['long_entry_price'].fillna(method='ffill',inplace=True)
        # 该位置收盘价加上8倍日线atr
        if atr_compute == False:
            long_position = df['signal_long'].fillna(method='ffill') == 1
            if df.loc[long_position].empty == False:
                df.loc[long_position, 'atr_day'] = df[long_position]['candle_begin_time'].apply(lambda x:day_df['atr'].shift(1)[x.strftime('%Y/%m/%d')])
            else:
                df.loc[long_position, 'atr_day'] = np.nan
            atr_compute = True
        df['long_profitstop_price'] = df['long_entry_price'] + atrn * df['atr_day']
        # 如果最高价大于止盈价就平仓
        df.loc[(df['high'] >= df['long_profitstop_price']) & (df['signal_long']!=1),'signal_long'] = 2
        # 需要二次处理
        df.loc[(df['high'] < df['long_profitstop_price']) & (df['signal_long']==2),'signal_long'] = np.nan
        # 验证是否处理完成
        if (len(df_long) == entry_num) and (len(df[df['signal_long']==2]) == profitstop_num):
            break
        entry_num = len(df_long)
        profitstop_num = len(df[df['signal_long']==2])
        # print(entry_num,profitstop_num)
    df.loc[df['signal_long'] == 2,'signal_long'] = 0
    
 
    # 找出做空信号
    condition1 = df['close'] < df['lower']  # 当前K线的收盘价 < 下轨
    condition2 = df['close'].shift(1) >= df['lower'].shift(1)
    condition3 = df['filtercdt'] == True
    # 之前K线的收盘价 >= 下轨
    df.loc[condition1 & condition2 & condition3 , 'signal_short'] = short  # 将产生做空信号的那根K线的signal设置为-1，-1代表做空
    # 找出做空平仓信号
    condition1 = df['close'] > df['median']  # 当前K线的收盘价 > 中轨
    condition2 = df['close'].shift(1) <= df['median'].shift(1)  # 之前K线的收盘价 <= 中轨
    df.loc[condition1 & condition2, 'signal_short'] = 0  # 将产生平仓信号当天的signal设置为0，0代表平仓
    # =====空头止盈信号
    # 找出空头开仓信号位置
    entry_num = 0
    profitstop_num = 0
    atr_compute = False
    while True:
        df_short = df[df['signal_short'].notnull()]
        df_short = df_short[(df_short['signal_short'] == -1) & (df_short['signal_short'].shift(1) != -1)]
        df['short_entry_price'] = np.nan
        df.loc[df_short.index,'short_entry_price'] = df['close']
        df['short_entry_price'].fillna(method='ffill',inplace=True)
        # 该位置收盘价减去8倍日线atr
        if atr_compute == False:
            short_position = df['signal_short'].fillna(method='ffill') == -1
            if df.loc[short_position].empty == False: 
                df.loc[short_position, 'atr_day'] = df[short_position]['candle_begin_time'].apply(lambda x:day_df['atr'].shift(1)[x.strftime('%Y/%m/%d')])
            else: 
                df.loc[short_position, 'atr_day'] = np.nan
            atr_compute = True
        df['short_profitstop_price'] = df['short_entry_price'] - atrn * df['atr_day']
        # 如果最高价大于止盈价就平仓
        df.loc[(df['low'] <= df['short_profitstop_price']) & (df['signal_short']!=-1),'signal_short'] = -2
        # 需要二次处理
        df.loc[(df['low'] > df['short_profitstop_price']) & (df['signal_short']==-2),'signal_short'] = np.nan
        # 验证是否处理完成
        if (len(df_short) == entry_num) and (len(df[df['signal_short']==-2]) == profitstop_num):
            break
        entry_num = len(df_short)
        profitstop_num = len(df[df['signal_short']==-2])
        # print(entry_num,profitstop_num) 
    df.loc[df['signal_short'] == -2,'signal_short'] = 0
    
    # 合并做多做空信号，去除重复信号
    df['signal'] = df[['signal_long','signal_short']].sum(axis=1, min_count=1, skipna=True) 
#    df['signal'] = df[['signal_long', 'signal_short']].sum(axis=1, min_count=1, skipna=True)  # 若你的pandas版本是最新的，请使用本行代码代替上面一行
    temp = df[df['signal'].notnull()][['signal']]
    temp = temp[temp['signal'] != temp['signal'].shift(1)]
    df['signal'] = temp['signal']
    df.drop(['median', 'std', 'upper', 'lower', 'signal_long','signal_short'], axis=1, inplace=True)
    return df

# whiskyMix策略
def whiskyMix(df,day_df,para=[[100,200,1,6],[200,400,1.2,8],[300,500,1.5,8]],long=1,short=-1):  #long,short指做多做空，如果不做则改为0,如果做空short改为-1
    # ===策略参数
    n1 = int(para[0][0])
    fltn1 = int(para[0][1])
    m1 = para[0][2]
    atrn1 = para[0][3]
    
    n2 = int(para[1][0])
    fltn2 = int(para[1][1])
    m2 = para[1][2]
    atrn2 = para[1][3]
    
    n3 = int(para[2][0])
    fltn3 = int(para[2][1])
    m3 = para[2][2]
    atrn3 = para[2][3]

    # ===计算指标
    # 计算均线
    df['median1'] = df['close'].rolling(n1, min_periods=1).mean()
    df['median2'] = df['close'].rolling(n2, min_periods=1).mean()
    df['median3'] = df['close'].rolling(n3, min_periods=1).mean()
    # 计算上轨、下轨道
    df['std1'] = df['close'].rolling(n1, min_periods=1).std(ddof=0)  # ddof代表标准差自由度
    df['std2'] = df['close'].rolling(n2, min_periods=1).std(ddof=0)
    df['std3'] = df['close'].rolling(n3, min_periods=1).std(ddof=0)
    df['upper1'] = df['median1'] + m1 * df['std1']
    df['upper2'] = df['median2'] + m2 * df['std2']
    df['upper3'] = df['median3'] + m3 * df['std3']
    df['lower1'] = df['median1'] - m1 * df['std1']
    df['lower2'] = df['median2'] - m2 * df['std2']
    df['lower3'] = df['median3'] - m3 * df['std3']
    # 计算过滤器指标
    stdvalue1 = df['close'].rolling(fltn1, min_periods=1).std(ddof=0)
    stdvalue2 = df['close'].rolling(fltn2, min_periods=1).std(ddof=0)
    stdvalue3 = df['close'].rolling(fltn3, min_periods=1).std(ddof=0)
    stdMA1 = tb.EMA(stdvalue1,4*fltn1)
    stdMA2 = tb.EMA(stdvalue2,4*fltn2)
    stdMA3 = tb.EMA(stdvalue3,4*fltn3)
    df['stdcdt1'] = 0
    df['stdcdt2'] = 0
    df['stdcdt3'] = 0
    df.loc[stdvalue1 <= stdMA1*0.75,'stdcdt1']=1 
    df.loc[stdvalue2 <= stdMA2*0.75,'stdcdt2']=1 
    df.loc[stdvalue3 <= stdMA3*0.75,'stdcdt3']=1 
    df['filtercdt1'] = df['stdcdt1'].rolling(fltn1, min_periods=1).sum() >= fltn1*0.5
    df['filtercdt2'] = df['stdcdt2'].rolling(fltn2, min_periods=1).sum() >= fltn2*0.5
    df['filtercdt3'] = df['stdcdt3'].rolling(fltn3, min_periods=1).sum() >= fltn3*0.5
    
    # ===计算信号
    df['trade'] = ''
    # 找出做多信号
    # ---1
    condition11 = df['close'] > df['upper1']  # 当前K线的收盘价 > 上轨
    condition12 = df['close'].shift(1) <= df['upper1'].shift(1) # 之前K线的收盘价 <= 上轨
    condition13 = df['filtercdt1'] == True
    # ---2
    condition21 = df['close'] > df['upper2']  # 当前K线的收盘价 > 上轨
    condition22 = df['close'].shift(1) <= df['upper2'].shift(1)  # 之前K线的收盘价 <= 上轨
    condition23 = df['filtercdt2'] == True
    # ---3
    condition31 = df['close'] > df['upper3']  # 当前K线的收盘价 > 上轨
    condition32 = df['close'].shift(1) <= df['upper3'].shift(1) # 之前K线的收盘价 <= 上轨
    condition33 = df['filtercdt3'] == True
    # 
    df.loc[condition11 & condition12 & condition13,'trade'] += 'BK1,'
    df.loc[condition21 & condition22 & condition23,'trade'] += 'BK2,'
    df.loc[condition31 & condition32 & condition33,'trade'] += 'BK3,'

    # 找出做多平仓信号
    # ---1
    condition11 = df['close'] < df['median1']  # 当前K线的收盘价 < 中轨
    condition12 = df['close'].shift(1) >= df['median1'].shift(1)   # 之前K线的收盘价 >= 中轨
    # ---2
    condition21 = df['close'] < df['median2']  # 当前K线的收盘价 < 中轨
    condition22 = df['close'].shift(1)  >= df['median2'].shift(1)   # 之前K线的收盘价 >= 中轨
    # ---3
    condition31 = df['close'] < df['median3']  # 当前K线的收盘价 < 中轨
    condition32 = df['close'].shift(1)  >= df['median3'].shift(1)   # 之前K线的收盘价 >= 中轨
    #
    df.loc[condition11 & condition12,'trade'] += 'SP1,'
    df.loc[condition21 & condition22,'trade'] += 'SP2,'
    df.loc[condition31 & condition32,'trade'] += 'SP3,'
    
        
    # 找出做空信号
    # ---1
    condition11 = df['close'] < df['lower1']  # 当前K线的收盘价 > 上轨
    condition12 = df['close'].shift(1) >= df['lower1'].shift(1) # 之前K线的收盘价 <= 上轨
    condition13 = df['filtercdt1'] == True
    
    # ---2
    condition21 = df['close'] < df['lower2']  # 当前K线的收盘价 > 上轨
    condition22 = df['close'].shift(1) >= df['lower2'].shift(1) # 之前K线的收盘价 <= 上轨
    condition23 = df['filtercdt2'] == True
    
    # ---3
    condition31 = df['close'] < df['lower3']  # 当前K线的收盘价 > 上轨
    condition32 = df['close'].shift(1) >= df['lower3'].shift(1)   # 之前K线的收盘价 <= 上轨
    condition33 = df['filtercdt3'] == True
    # 
    df.loc[condition11 & condition12 & condition13,'trade'] += 'SK1,'
    df.loc[condition21 & condition22 & condition23,'trade'] += 'SK2,'
    df.loc[condition31 & condition32 & condition33,'trade'] += 'SK3,'
   
    # 找出做空平仓信号
    # ---1
    condition11 = df['close'] > df['median1']  # 当前K线的收盘价 < 中轨
    condition12 = df['close'].shift(1) <= df['median1'].shift(1)  # 之前K线的收盘价 >= 中轨
    # ---2
    condition21 = df['close'] > df['median2']  # 当前K线的收盘价 < 中轨
    condition22 = df['close'].shift(1) <= df['median2'].shift(1)  # 之前K线的收盘价 >= 中轨
    # ---3
    condition31 = df['close'] > df['median3']  # 当前K线的收盘价 < 中轨
    condition32 = df['close'].shift(1) <= df['median3'].shift(1)  # 之前K线的收盘价 >= 中轨
    #
    df.loc[condition11 & condition12,'trade'] += 'BP1,'
    df.loc[condition21 & condition22,'trade'] += 'BP2,'
    df.loc[condition31 & condition32,'trade'] += 'BP3,'
    
    df_check = pd.DataFrame(df[df['trade']!='']['trade'])
    # df_check = df_check[df_check['trade']!=df_check['trade'].shift(1)]
    df_check['signal_long'] = None
    df_check['signal_short'] = None
    df_check['atrn'] = None
    # df_check['atrn'] = 8  #默认按照8倍止盈
    df_check.loc[df_check.index[0],'signal_long'] = 0
    df_check.loc[df_check.index[0],'signal_short'] = 0
    next_signal_list = ['BK1','BK2','BK3','SK1','SK2','SK3']
    for i in range(len(df_check)):
        if list(set(df_check.iloc[i]['trade'].strip(',').split(','))&set(next_signal_list)) !=[]:
            #平仓    
            if list(set(df_check.iloc[i]['trade'].strip(',').split(','))&set(['SP1','SP2','SP3'])) !=[]:
                df_check.loc[df_check.index[i],['signal_long']] = 0
                next_signal_list = ['BK1','BK2','BK3','SK1','SK2','SK3']
            if list(set(df_check.iloc[i]['trade'].strip(',').split(','))&set(['BP1','BP2','BP3'])) !=[]:
                df_check.loc[df_check.index[i],['signal_short']] = 0
                next_signal_list = ['BK1','BK2','BK3','SK1','SK2','SK3']
        if list(set(df_check.iloc[i]['trade'].strip(',').split(','))&set(next_signal_list)) !=[]:
            #开仓
            if 'BK1' in df_check.iloc[i]['trade']:
                df_check.loc[df_check.index[i],['signal_long']] = 1
                df_check.loc[df_check.index[i],['signal_short']] = 0
                df_check.loc[df_check.index[i],['atrn']] = atrn1 #只有model1的时候按6倍止盈
                next_signal_list = ['SP1','SK1']
            if 'BK2' in df_check.iloc[i]['trade']:
                df_check.loc[df_check.index[i],['signal_long']] = 1
                df_check.loc[df_check.index[i],['signal_short']] = 0
                df_check.loc[df_check.index[i],['atrn']] = atrn2
                next_signal_list = ['SP2','SK2']
            if 'BK3' in df_check.iloc[i]['trade']:
                df_check.loc[df_check.index[i],['signal_long']] = 1
                df_check.loc[df_check.index[i],['signal_short']] = 0
                df_check.loc[df_check.index[i],['atrn']] = atrn3
                next_signal_list = ['SP3','SK3']  
            if 'SK1' in df_check.iloc[i]['trade']:
                df_check.loc[df_check.index[i],['atrn']] = atrn1  #只有model1的时候按6倍止盈
                df_check.loc[df_check.index[i],['signal_short']] = -1
                df_check.loc[df_check.index[i],['signal_long']] = 0
                next_signal_list = ['BP1','BK1']
            if 'SK2' in df_check.iloc[i]['trade']:
                df_check.loc[df_check.index[i],['signal_short']] = -1
                df_check.loc[df_check.index[i],['signal_long']] = 0
                df_check.loc[df_check.index[i],['atrn']] = atrn2
                next_signal_list = ['BP2','BK2']
            if 'SK3' in df_check.iloc[i]['trade']:
                df_check.loc[df_check.index[i],['signal_short']] = -1
                df_check.loc[df_check.index[i],['signal_long']] = 0
                df_check.loc[df_check.index[i],['atrn']] = atrn3
                next_signal_list = ['BP3','BK3']  
            

    df.loc[df_check.index,'signal_long'] = df_check['signal_long']
    df.loc[df_check.index,'signal_short'] = df_check['signal_short']
    df.loc[df_check.index,'atrn'] = df_check['atrn']
    df.loc[df.index[0],'atrn'] = 8
    df['atrn'].fillna(method='ffill',inplace=True)
    # df.loc[df_check.index,'atr_day'] = df.loc[df_check.index,'candle_begin_time'].apply(lambda x:day_df['atr'].shift(1)[x.strftime('%Y/%m/%d')])
            
     
    # =====多头止盈信号
    # 找出多头开仓信号位置
    entry_num = 0
    profitstop_num = 0
    atr_compute = False
    while True:
        df_long = df[df['signal_long'].notnull()]
        df_long = df_long[(df_long['signal_long'] == 1) & (df_long['signal_long'].shift(1) != 1)]
        df['long_entry_price'] = np.nan
        df.loc[df_long.index,'long_entry_price'] = df['close']
        df['long_entry_price'].fillna(method='ffill',inplace=True)
        # 该位置收盘价加上8倍日线atr
        if atr_compute == False:
            long_position = df['signal_long'].fillna(method='ffill') == 1
            if df.loc[long_position].empty == False:
                df.loc[long_position, 'atr_day'] = df[long_position]['candle_begin_time'].apply(lambda x:day_df['atr'].shift(1)[x.strftime('%Y/%m/%d')])
            else:
                df.loc[long_position, 'atr_day'] = np.nan
            atr_compute = True
        df['long_profitstop_price'] = df['long_entry_price'] + df['atrn'] * df['atr_day']
        # 如果最高价大于止盈价就平仓
        df.loc[(df['high'] >= df['long_profitstop_price']) & (df['signal_long']!=1),'signal_long'] = 2
        # 需要二次处理
        df.loc[(df['high'] < df['long_profitstop_price']) & (df['signal_long']==2),'signal_long'] = np.nan
        # 验证是否处理完成
        if (len(df_long) == entry_num) and (len(df[df['signal_long']==2]) == profitstop_num):
            break
        entry_num = len(df_long)
        profitstop_num = len(df[df['signal_long']==2])
        # print(entry_num,profitstop_num)
    df.loc[df['signal_long'] == 2,'signal_long'] = 0
    
    # =====空头止盈信号
    # 找出空头开仓信号位置
    entry_num = 0
    profitstop_num = 0
    atr_compute = False
    while True:
        df_short = df[df['signal_short'].notnull()]
        df_short = df_short[(df_short['signal_short'] == -1) & (df_short['signal_short'].shift(1) != -1)]
        df['short_entry_price'] = np.nan
        df.loc[df_short.index,'short_entry_price'] = df['close']
        df['short_entry_price'].fillna(method='ffill',inplace=True)
        # 该位置收盘价减去8倍日线atr
        if atr_compute == False:
            short_position = df['signal_short'].fillna(method='ffill') == -1
            if df.loc[short_position].empty == False: 
                df.loc[short_position, 'atr_day'] = df[short_position]['candle_begin_time'].apply(lambda x:day_df['atr'].shift(1)[x.strftime('%Y/%m/%d')])
            else: 
                df.loc[short_position, 'atr_day'] = np.nan
            atr_compute = True
        df['short_profitstop_price'] = df['short_entry_price'] - df['atrn'] * df['atr_day']
        # 如果最高价大于止盈价就平仓
        df.loc[(df['low'] <= df['short_profitstop_price']) & (df['signal_short']!=-1),'signal_short'] = -2
        # 需要二次处理
        df.loc[(df['low'] > df['short_profitstop_price']) & (df['signal_short']==-2),'signal_short'] = np.nan
        
        # 验证是否处理完成
        if (len(df_short) == entry_num) and (len(df[df['signal_short']==-2]) == profitstop_num):
            break
        entry_num = len(df_short)
        profitstop_num = len(df[df['signal_short']==-2])
#        print(entry_num,profitstop_num)
    df.loc[df['signal_short'] == -2,'signal_short'] = 0
     
    # 合并做多做空信号，去除重复信号
    df['signal'] = df[['signal_long','signal_short']].sum(axis=1, min_count=1, skipna=True) 
    temp = df[df['signal'].notnull()][['signal']]
    temp = temp[temp['signal'] != temp['signal'].shift(1)]
    df['signal'] = temp['signal']
    df.drop(['median1', 'median2', 'median3', 'std1','std2', 'std3', 
             'upper1', 'upper2', 'upper3', 'lower1', 'lower2','lower3', 
             'stdcdt1', 'stdcdt2', 'stdcdt3', 
             'filtercdt1', 'filtercdt2','filtercdt3'], axis=1, inplace=True)
    return df

#AbsolutVodka策略
def AbsolutVodka(df,day_df,para=[100, 200, 50, 200, 4, 200],long=1,short=-1):  #long,short指做多做空，如果不做则改为0,如果做空short改为-1
    # ===策略参数
    n = para[0]
    m = para[1]
    z = para[2]
    eman = para[3]
    atrn = para[4]
    fltn = para[5]

    '''计算指标'''
    #EMA
    df['EMA'] = tb.EMA(df['close'],eman)
    #diff
    df['zb'] = (df['close'] - df['close'].rolling(eman, min_periods=1).mean())/df['close'].rolling(eman, min_periods=1).std(ddof=0)
    #----对zb指标进行容错处理（std为0时会出现zb无限大导致指标计算错误）
    df.loc[df['zb'] == np.inf,'zb'] = np.nan
    df.loc[df['zb'] == -np.inf,'zb'] = np.nan
    df['zb'] = df['zb'].fillna(method='ffill')
    df['diff'] = tb.EMA(tb.EMA(df['zb'],n) - tb.EMA(df['zb'],m),z)
    # 计算过滤器指标
    stdvalue = df['close'].rolling(fltn, min_periods=1).std(ddof=0)
    stdMA = tb.EMA(stdvalue,4*fltn)
    df['stdcdt'] = 0
    df.loc[stdvalue <= stdMA*0.75,'stdcdt']=1 
    df['filtercdt'] = df['stdcdt'].rolling(fltn, min_periods=fltn).sum() >= fltn*0.5
    
    '''计算信号'''
    # 找出做多信号
    condition1 = df['close'] >= df['EMA']  
    condition2 = df['diff'] >= df['diff'].shift(1)
    condition3 = df['diff'].shift(1) < df['diff'].shift(2)
    condition4 = df['filtercdt'] == True
    df.loc[condition1 & condition2 & condition3 & condition4, 'signal_long'] = long  # 将产生做多信号的那根K线的signal设置为1，1代表做多
    # 找出做多平仓信号
    condition1 = df['diff'] <= df['diff'].shift(1)  
    condition2 = df['diff'].shift(1) > df['diff'].shift(2)
    df.loc[condition1 & condition2, 'signal_long'] = 0  # 将产生平仓信号当天的signal设置为0，0代表平仓
    df.loc[df.index[0],'signal_long'] = 0
    # =====多头止盈信号
    # 找出多头开仓信号位置
    entry_num = 0
    profitstop_num = 0
    atr_compute = False
    while True:
        df_long = df[df['signal_long'].notnull()]
        df_long = df_long[(df_long['signal_long'] == 1) & (df_long['signal_long'].shift(1) != 1)]
        df['long_entry_price'] = np.nan
        df.loc[df_long.index,'long_entry_price'] = df['close']
        df['long_entry_price'].fillna(method='ffill',inplace=True)
        # 该位置收盘价加上8倍日线atr
        if atr_compute == False:
            long_position = df['signal_long'].fillna(method='ffill') == 1
            if df.loc[long_position].empty == False:
                df.loc[long_position, 'atr_day'] = df[long_position]['candle_begin_time'].apply(lambda x:day_df['atr'].shift(1)[x.strftime('%Y/%m/%d')])
            else:
                df.loc[long_position, 'atr_day'] = np.nan
            atr_compute = True
        df['long_profitstop_price'] = df['long_entry_price'] + atrn * df['atr_day']
        # 如果最高价大于止盈价就平仓
        df.loc[(df['high'] >= df['long_profitstop_price']) & (df['signal_long']!=1),'signal_long'] = 2
        # 需要二次处理
        df.loc[(df['high'] < df['long_profitstop_price']) & (df['signal_long']==2),'signal_long'] = np.nan
        # 验证是否处理完成
        if (len(df_long) == entry_num) and (len(df[df['signal_long']==2]) == profitstop_num):
            break
        entry_num = len(df_long)
        profitstop_num = len(df[df['signal_long']==2])
        # print(entry_num,profitstop_num)
    df.loc[df['signal_long'] == 2,'signal_long'] = 0
    
    # 找出做空信号
    condition1 = df['close'] <= df['EMA']  
    condition2 = df['diff'] <= df['diff'].shift(1)
    condition3 = df['diff'].shift(1) > df['diff'].shift(2)
    condition4 = df['filtercdt'] == True
    df.loc[condition1 & condition2 & condition3 & condition4, 'signal_short'] = short  # 将产生做多信号的那根K线的signal设置为1，1代表做多
    # 找出做空平仓信号
    condition1 = df['diff'] >= df['diff'].shift(1)  
    condition2 = df['diff'].shift(1) < df['diff'].shift(2)
    df.loc[condition1 & condition2, 'signal_short'] = 0  # 将产生平仓信号当天的signal设置为0，0代表平仓
    df.loc[df.index[0],'signal_short'] = 0
    # =====空头止盈信号
    # 找出空头开仓信号位置
    entry_num = 0
    profitstop_num = 0
    atr_compute = False
    while True:
        df_short = df[df['signal_short'].notnull()]
        df_short = df_short[(df_short['signal_short'] == -1) & (df_short['signal_short'].shift(1) != -1)]
        df['short_entry_price'] = np.nan
        df.loc[df_short.index,'short_entry_price'] = df['close']
        df['short_entry_price'].fillna(method='ffill',inplace=True)
        # 该位置收盘价减去8倍日线atr
        if atr_compute == False:
            short_position = df['signal_short'].fillna(method='ffill') == -1
            if df.loc[short_position].empty == False: 
                df.loc[short_position, 'atr_day'] = df[short_position]['candle_begin_time'].apply(lambda x:day_df['atr'].shift(1)[x.strftime('%Y/%m/%d')])
            else: 
                df.loc[short_position, 'atr_day'] = np.nan
            atr_compute = True
        df['short_profitstop_price'] = df['short_entry_price'] - atrn * df['atr_day']
        # 如果最高价大于止盈价就平仓
        df.loc[(df['low'] <= df['short_profitstop_price']) & (df['signal_short']!=-1),'signal_short'] = -2
        # 需要二次处理
        df.loc[(df['low'] > df['short_profitstop_price']) & (df['signal_short']==-2),'signal_short'] = np.nan
        
        # 验证是否处理完成
        if (len(df_short) == entry_num) and (len(df[df['signal_short']==-2]) == profitstop_num):
            break
        entry_num = len(df_short)
        profitstop_num = len(df[df['signal_short']==-2])
#        print(entry_num,profitstop_num)
    df.loc[df['signal_short'] == -2,'signal_short'] = 0
    
    # 合并做多做空信号，去除重复信号
    df['signal'] = df[['signal_long','signal_short']].sum(axis=1, min_count=1, skipna=True) 
    temp = df[df['signal'].notnull()][['signal']]
    temp = temp[temp['signal'] != temp['signal'].shift(1)]
    df['signal'] = temp['signal']
    df.drop(['EMA', 'diff'], axis=1, inplace=True)
    return df




















