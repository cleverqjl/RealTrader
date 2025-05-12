path = r'D:\CTATrade'
import sys
sys.path.append(r'D:\CTATrade\vnstudio\Lib\site-packages')
sys.path.append(path)
from time import sleep
import pandas as pd
import numpy as np
import math
from datetime import datetime,timedelta
import time
import re
import talib as tb
from dingtalkchatbot.chatbot import DingtalkChatbot
# import futureDataManage
from backtest import backtestStrategy,backtestStrategies
import rqdatac as rq
from dingtalkchatbot.chatbot import DingtalkChatbot
from futureDataManage import format_to_RQshare,format_to_onlyLetter
from vnpy.trader.constant import Direction, Offset, OrderType, Interval
from vnpy.trader.object import (
                                OrderRequest,
                                CancelRequest,
                                HistoryRequest,
                                SubscribeRequest,
                                TickData,
                                OrderData,
                                TradeData,
                                PositionData,
                                AccountData,
                                ContractData,
                                LogData,
                                BarData
                                )
from vnpy.trader.converter import OffsetConverter,PositionHolding
# ====钉钉通知
# 钉钉设置
webhook = 'https://oapi.dingtalk.com/robot/send?access_token=d19d8d852970a537af6b052be2fbd12e8b4a2ef43e8d1ae06a98ffc6fd77546e'
secret = 'SECc18045c0546f123ec713a8caae92a46629e2c715bf4e34b2286e1216d7c2c73b' 
# 初始化机器人小丁
xiaoding = DingtalkChatbot(webhook, secret=secret,pc_slide=True) 
def send_dingding_msg(text):
    xiaoding.send_text(msg=text, is_at_all=False)
def send_dingding_and_raise_error(content):
    print(content)
    send_dingding_msg(content)
    raise ValueError(content)
# ====日志输出
log_data=[]
# ====清空log信息
def renew_log():
    global log_data
    log_data = []
# ====添加log信息
def log(data):
    log_data.append(data)
# ====存放log信息
def save_log(log_data,log_file):
    for i in log_data:
        if type(i)==str:
            log_file.write(i)
            log_file.write('\n') 
        elif isinstance(i, pd.DataFrame):
            log_file.write('    ')
            for _ in list(i.columns):
                log_file.write(_+'    ')
            log_file.write('\n') 
            for _ in range(len(i)):
                log_file.write(str(i.index[_])+'    ')
                log_file.writelines([str(k)+'    ' for k in list(i.iloc[_])])
                log_file.write('\n') 
    log_file.close()
    
# ====对有持仓但没订阅过的合约进行订阅
def subcribeMarketPlus(new_list,old_list,main_engine):
    subcribe_list=[]
    if new_list != []:
        subcribe_list = [i for i in new_list if i not in old_list]
    if subcribe_list != []:
        for i in subcribe_list:
             contract = main_engine.get_contract(i)
             req = SubscribeRequest(symbol=contract.symbol,exchange=contract.exchange)
             main_engine.subscribe(req, contract.gateway_name)    
    return subcribe_list
# ====计算策略历史信号
def prepareHistoryData_multiStrategy(df,
                                     strategy_dict,
                                     minpoint,
                                     c_rate ,
                                     drop_days,
                                     slippageMulti,
                                     contractMulti,
                                     capitalmode,
                                     multiCompute,
                                     limitValue,
                                     method):
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
        backtest_df_dict[i] = strategy(data_df,day_df,para=para)
        backtest_df_dict[i]['signal'].fillna(method='ffill', inplace=True)
        backtest_df_dict[i]['signal'].fillna(value=0, inplace=True)  # 将初始行数的signal补全为0
        backtest_df_dict[i]['pos'] = backtest_df_dict[i]['signal']  #此处和回测的区别在于不shift(1)，为了实盘计算手数用
        backtest_df_dict[i].fillna(value=0, inplace=True)  # 将初始行数的pos补全为0
        backtest_df_dict[i].fillna(method='ffill', inplace=True)
        # 找出开仓的k线
        condition1 = backtest_df_dict[i]['pos'] != 0  # 当前周期不为空仓
        condition2 = backtest_df_dict[i]['pos'] != backtest_df_dict[i]['pos'].shift(1)  # 当前周期和上个周期持仓方向不一样。
        open_pos_condition = condition1 & condition2
        backtest_df_dict[i].loc[open_pos_condition, 'atr_day'] = backtest_df_dict[i][open_pos_condition]['candle_begin_time'].apply(lambda x:day_df['atr'].shift(1)[x.strftime('%Y/%m/%d')])
        backtest_df_dict[i].loc[open_pos_condition, 'contract_num'] = capital_unit * abs(backtest_df_dict[i].loc[open_pos_condition, 'pos']) / backtest_df_dict[i].loc[open_pos_condition, 'atr_day'] / contractMulti
        # 头寸取整
        backtest_df_dict[i].loc[open_pos_condition, 'contract_num'] = [max(i,1) for i in round(backtest_df_dict[i].loc[open_pos_condition, 'contract_num'])]
        #计算仓位占用
        backtest_df_dict[i]['contract_num'].fillna(method='ffill',inplace=True)
        backtest_df_dict[i].loc[backtest_df_dict[i]['pos'] == 0, ['contract_num']] = 0
        backtest_df_dict[i]['contract_num'].fillna(0,inplace=True)
        # print('回测%s策略完成'%i)
    '''分别计算信号'''
    # 统计个策略信号/仓位/持仓数量
    backtest_df = pd.DataFrame()
    for i in list(backtest_df_dict.keys()):
        backtest_df[['candle_begin_time','open','high','low','close','atr_day']] = backtest_df_dict[i][['candle_begin_time','open','high','low','close','atr_day']]
        backtest_df[i+'_signal'] = backtest_df_dict[i]['signal']
        backtest_df[i+'_pos'] = backtest_df_dict[i]['pos']
        backtest_df[i+'_contract_num'] = backtest_df_dict[i]['contract_num'] 
        backtest_df[i+'_long_entry_price'] = backtest_df_dict[i]['long_entry_price'] 
        backtest_df[i+'_short_entry_price'] = backtest_df_dict[i]['short_entry_price'] 
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
        # backtest_df['candle_begin_time'] = backtest_df.index
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
        backtest_df = backtest_df.drop(['contract_num'],axis=1)
    elif method == 'capital_unit_sum':
        backtest_df['capital_unit_sum'] = 0
        for i in list(backtest_df_dict.keys()):
            backtest_df['pos'] += backtest_df[i+'_pos']
            backtest_df['capital_unit_sum'] += backtest_df[i+'_pos']*strategy_dict[i]['capital_unit']
        # 找出开仓的k线
        condition1 = backtest_df['pos'] != 0  # 当前周期不为空仓
        condition2 = backtest_df['pos'] != backtest_df['pos'].shift(1)  # 当前周期和上个周期持仓方向不一样。
        open_pos_condition = condition1 & condition2
        # backtest_df['candle_begin_time'] = backtest_df.index
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
        backtest_df = backtest_df.drop(['contract_num','direction'],axis=1)
    return backtest_df

# ====检查当前映射合约是否为主力合约
def checkShareIsMajor(tradeShare_dict):
    checkShare_df = pd.DataFrame()
    today_str = datetime.now().strftime('%Y%m%d')
    for i in list(tradeShare_dict.keys()):
        majorShare = list(rq.futures.get_dominant(i, end_date=today_str))[-1]
        useShare = tradeShare_dict[i]
        if format_to_RQshare(useShare) != majorShare:
            print('***请注意***')
            log('***请注意***')
            print('%s当前映射合约在RQdata中为非主力合约，请检查是否需要更换至主力合约%s'%(format_to_RQshare(useShare),majorShare))
            log('%s当前映射合约在RQdata中为非主力合约，请检查是否需要更换至主力合约%s'%(format_to_RQshare(useShare),majorShare))
            send_dingding_msg('***请注意***'+'\n'+'%s当前映射合约在RQdata中为非主力合约，请检查是否需要更换至主力合约%s'%(format_to_RQshare(useShare),majorShare))
            checkShare_df.loc[i,'useShare'] = format_to_RQshare(useShare)
            checkShare_df.loc[i,'majorShare'] = majorShare
    return checkShare_df

# ====检查当前映射合约是否为主力合约（自己根据持仓量监控）
def checkShareIsMajor_viaInterest(tradeShare_dict):
    #今日日期
    today_str = datetime.now().date().strftime('%Y%m%d')
    before_str =  (datetime.now().date()-timedelta(days=10)).strftime('%Y%m%d')
    #所有需要监控的品种
    future_list = list(tradeShare_dict.keys())
    #获取该品种各合约的持仓量与交易量
    major_df = pd.DataFrame()
    check_df = pd.DataFrame()
    toChange_df = pd.DataFrame()
    for i in future_list:
        volume_df = pd.DataFrame()
        share_list = rq.futures.get_contracts(i.upper(), today_str)
        Share_list_RQ = [format_to_RQshare(i) for i in share_list]  
        volume_data = rq.get_price(Share_list_RQ, fields=['volume','open_interest'],start_date=before_str, end_date=today_str,frequency='60m')  
        for s in volume_data.index.get_level_values(0).unique():
            volume_df.loc[s,'volume'] = volume_data.loc[s].iloc[-1]['volume']
            volume_df.loc[s,'open_interest'] = volume_data.loc[s].iloc[-1]['open_interest']
        volume_df = volume_df.sort_values(['open_interest','volume'],ascending=False).head(2).copy()
        if (volume_df.iloc[1]['open_interest'] > 0.8*volume_df.iloc[0]['open_interest']) and (volume_df.iloc[1]['volume'] > 0.8*volume_df.iloc[0]['volume']) and (volume_df.iloc[1]['open_interest'] < volume_df.iloc[0]['open_interest']) and (volume_df.index[0]<volume_df.index[1]) :
               toChange_df.loc[i,'toChangeShare'] = volume_df.index[1]
        check_df = check_df.append(volume_df) 
        major_df.loc[i,'majorshare'] = volume_df.index[0]
        major_df.loc[i,'useshare'] = format_to_RQshare(tradeShare_dict[i])    
    print('各主/次主力合约持仓量监控')
    print(check_df)
    log('各主/次主力合约持仓量监控')
    log(check_df)
    print('当前主力合约为')
    print(major_df)
    log('当前主力合约为')
    log(major_df)
    #不一致合约
    different_df = major_df[major_df['majorshare']!=major_df['useshare']]
    if different_df.empty == False:
        print('***注意：以下主力合约与使用合约不一致')
        print(different_df)
        log('***注意：以下主力合约与使用合约不一致')
        log(different_df)
        send_data = '【***注意：以下主力合约与使用合约不一致】\n'
        send_data += '------------------------\n'
        send_data += '品种， 主力合约， 使用合约\n'
        for i in range(len(different_df)):
            send_data += str(different_df.index[i]) +',   '
            send_data += str(different_df.iloc[i]['majorshare']) +',   '
            send_data += str(different_df.iloc[i]['useshare']) +',   \n'
        send_dingding_msg(send_data)
    else:
        print('当前使用合约均为主力合约')
        log('当前使用合约均为主力合约')
    #即将换月合约
    if toChange_df.empty == False:
        print('***注意：以下品种即将更换主力合约')
        print(toChange_df)
        log('***注意：以下品种即将更换主力合约')
        log(toChange_df)
        send_data = '【***注意：以下品种即将更换主力合约】\n'
        send_data += '------------------------\n'
        send_data += '品种， 即将更换主力合约\n'
        for i in range(len(toChange_df)):
            send_data += str(toChange_df.index[i]) +',   '
            send_data += str(toChange_df.iloc[i]['toChangeShare']) +',  \n'
        send_dingding_msg(send_data)
    return check_df,major_df,different_df,toChange_df


# ====统计当前分策略持仓
def countPortfolioPos(portfolio_df_dict,strategy_set_dict,TradeShare_dict):
    try:
        portfolio_pos_df = pd.DataFrame()
        for i in list(portfolio_df_dict.keys()):
            portfolio_pos_df.loc[i,'tradeShare'] = TradeShare_dict[format_to_onlyLetter(i)]
            portfolio_pos_df.loc[i,'index_lastestPrice'] = portfolio_df_dict[i].iloc[-1]['close']
            portfolio_pos_df.loc[i,'last_candle_begin_time'] = portfolio_df_dict[i].iloc[-1]['candle_begin_time'].strftime('%Y%m%d %H:%M')
            strategy_dict = strategy_set_dict[i]
            for s in list(strategy_dict.keys()):
                portfolio_pos_df.loc[i,s+'_pos'] =  portfolio_df_dict[i].iloc[-1][s+'_pos']
            portfolio_pos_df.loc[i,'contract_num_drt'] = portfolio_df_dict[i].iloc[-1]['contract_num_drt']
        checkBartime_df = portfolio_pos_df[portfolio_pos_df['last_candle_begin_time']>datetime.now().strftime('%Y%m%d %H:%M')]
        if checkBartime_df.empty == False:
            print('bar数据最后时间超过当前时间，请检查数据源是否出现错误')
            log('bar数据最后时间超过当前时间，请检查数据源是否出现错误')
            send_dingding_msg('！！！注意：bar数据最后时间超过当前时间，请检查数据源是否出现错误')
    except Exception as e:
        log('***ERROR: '+str(e))
        log('【countPortfolioPos在运算%s时出现错误】，程序终止，请检查代码'%i)
        send_dingding_and_raise_error('【countPortfolioPos在运算%s时出现错误】，程序终止，请检查代码'%i)
    return portfolio_pos_df

# ====计算下次运行时间
def next_run_time(time_interval='10m', ahead_seconds=5):
    if time_interval.endswith('m') or time_interval.endswith('h') or  time_interval.endswith('d'):
        pass
    elif time_interval.endswith('T'):
        time_interval = time_interval.replace('T', 'm')
    elif time_interval.endswith('H'):
        time_interval = time_interval.replace('H', 'h')
    elif time_interval.endswith('D'):
        time_interval = time_interval.replace('D', 'd')
#    else:
#        log('time_interval格式不符合规范。程序exit')
#        send_dingding_and_raise_error('time_interval格式不符合规范。程序exit')
    ti = pd.to_timedelta(time_interval)
    now_time = datetime.now()
    # now_time = datetime(2022,10,12,10,16) 
    #设定交易时段
    if now_time < now_time.replace(hour=9, minute=00, second=0, microsecond=0):
        now_time = now_time.replace(hour=9, minute=00, second=0, microsecond=0)
    elif (now_time > now_time.replace(hour=11, minute=30, second=0, microsecond=0)) and (now_time < now_time.replace(hour=13, minute=30, second=0, microsecond=0)):
        now_time = now_time.replace(hour=13, minute=30, second=0, microsecond=0)
    elif (now_time > now_time.replace(hour=10, minute=20, second=0, microsecond=0)) and (now_time < now_time.replace(hour=10, minute=30, second=0, microsecond=0)):
        now_time = now_time.replace(hour=10, minute=30, second=0, microsecond=0)    
#    elif now_time > now_time.replace(hour=15, minute=00, second=0, microsecond=0):   #将此时段写在循环部分
#        now_time = (now_time + timedelta(days=1)).replace(hour=9, minute=30, second=0, microsecond=0)
        # now_time = datetime(2021, 5, 28, 15, 35, 0)  # 修改now_time，可用于测试
    this_midnight = now_time.replace(hour=0, minute=0, second=0, microsecond=0)
    min_step = timedelta(minutes=1)
    target_time = now_time.replace(second=0, microsecond=0)
    while True:
#        if time_interval == '1d':
#            target_time = this_midnight +timedelta(days=1)
#            break
        target_time = target_time + min_step
        delta = target_time - this_midnight
        if delta.seconds % ti.seconds == 0 and (target_time - now_time).seconds >= ahead_seconds:
            # 当符合运行周期，并且目标时间有足够大的余地，默认为60s
            break
    print('程序下次运行的时间：', target_time, '\n')
    log('程序下次运行的时间：'+ target_time.strftime('%Y-%m-%d %H:%M:%S'))
    return target_time  

# ====自动计算并休眠到指定时间
def sleep_until_run_time(time_interval, ahead_time=5):
    # 计算下次运行时间
    run_time = next_run_time(time_interval, ahead_time)
    # sleep
    time.sleep(max(0, (run_time - datetime.now()).seconds))
    while True:  # 在靠近目标时间时
        if datetime.now() >= run_time:
            break
    return run_time 

# ====计算最新总持仓
def countNewPos(data_df,strategy_dict,capital_unit,contractMulti,method):
    '''
    data_df = portfolio_df_dict[i]
    strategy_dict = strategy_set_dict[i]
    capital_unit 只有在后两种模式中用到
    '''
    try:
        if method == 'contract_sum':
            data_df.loc[data_df.index[-1],'pos'] = 0  #初始设置为0，否则相加会为nan
            data_df.loc[data_df.index[-1],'contract_num_drt'] = 0
            for s in strategy_dict.keys():
                data_df.loc[data_df.index[-1],'pos'] += data_df.iloc[-1][s+'_pos']
                data_df.loc[data_df.index[-1],'contract_num_drt'] += data_df.iloc[-1][s+'_contract_num'] * data_df.iloc[-1][s+'_pos'] 
        elif method == 'pos_max':    
            data_df.loc[data_df.index[-1],'pos'] = 0  #初始设置为0，否则相加会为nan
            for s in strategy_dict.keys():
                data_df.loc[data_df.index[-1],'pos'] += data_df.iloc[-1][s+'_pos']
            if data_df.iloc[-1]['pos'] > 0:
                data_df.loc[data_df.index[-1],'pos'] = 1
            elif data_df.iloc[-1]['pos'] < 0:
                data_df.loc[data_df.index[-1],'pos'] = -1
            elif data_df.iloc[-1]['pos'] == 0:
                data_df.loc[data_df.index[-1],'pos'] = 0
            # 计算头寸
            capital_pos_max = 0
            for s in strategy_dict.keys():
                capital_pos_max = max(capital_pos_max, data_df.loc[data_df.index[-1],s+'_pos'] * strategy_dict[s]['capital_unit'])
            if (data_df.iloc[-1]['pos'] !=0) and (data_df.iloc[-1]['pos'] != data_df.iloc[-2]['pos']):
                contract_num = capital_pos_max / data_df.iloc[-1]['atr_day'] / contractMulti
                contract_num = max(round(contract_num),1)
                data_df.loc[data_df.index[-1],'contract_num_drt'] = contract_num
            elif (data_df.iloc[-1]['pos'] !=0) and (data_df.iloc[-1]['pos'] == data_df.iloc[-2]['pos']):
                data_df.loc[data_df.index[-1],'contract_num_drt'] = data_df.loc[data_df.index[-2],'contract_num_drt']  
            elif data_df.iloc[-1]['pos'] == 0:
                data_df.loc[data_df.index[-1],'contract_num_drt'] = 0
        elif method == 'capital_unit_sum':
            data_df.loc[data_df.index[-1],'pos'] = 0  #初始设置为0，否则相加会为nan
            capital_unit_sum = 0
            for s in strategy_dict.keys():
                data_df.loc[data_df.index[-1],'pos'] += data_df.iloc[-1][s+'_pos']
                capital_unit_sum += data_df.loc[data_df.index[-1],s+'_pos']*strategy_dict[s]['capital_unit']
            # 计算头寸
            if (data_df.iloc[-1]['pos'] !=0) and (data_df.iloc[-1]['pos'] != data_df.iloc[-2]['pos']):
                contract_num = abs(capital_unit_sum) / data_df.iloc[-1]['atr_day'] / contractMulti
                contract_num = max(round(contract_num),1)
                if data_df.iloc[-1]['pos'] > 0:
                    direction = 1
                elif data_df.iloc[-1]['pos'] < 0:
                    direction = -1    
                data_df.loc[data_df.index[-1],'contract_num_drt'] = contract_num * direction
            elif (data_df.iloc[-1]['pos'] !=0) and (data_df.iloc[-1]['pos'] == data_df.iloc[-2]['pos']):
                data_df.loc[data_df.index[-1],'contract_num_drt'] = data_df.loc[data_df.index[-2],'contract_num_drt']  
            elif data_df.iloc[-1]['pos'] == 0:
                data_df.loc[data_df.index[-1],'contract_num_drt'] = 0
    except Exception as e:
        log('***ERROR: '+str(e))
        log('【countNewPos出现错误】，程序终止，请检查代码')
        send_dingding_and_raise_error('【countNewPos出现错误】，程序终止，请检查代码')
    return data_df

# ====由头寸变动转换最新交易信号
def computeTradeSingal(portfolio_pos_df,portfolio_pos_df_new,firstTradeLock=False,netpos=True):
    '''
    如果firstTradeLock=True（优先平对锁仓），则在策略发出信号时要将portfolio_pos_df输入为账户持仓而不是原策略持仓，因为账户持仓才有锁仓信息
    '''
    # data_old = pd.DataFrame(portfolio_pos_df.set_index('tradeShare')).copy()
    #为防止出现对锁仓的情况，计算净持仓
    if netpos == True:
        data_old = portfolio_pos_df.groupby(by='tradeShare')[['contract_num_drt']].sum()
        data_new = portfolio_pos_df_new.groupby(by='tradeShare')[['contract_num_drt']].sum()
        change_df = pd.concat([data_old['contract_num_drt'],data_new['contract_num_drt']],axis=1,join='outer',sort=False).fillna(0)
        change_df.columns = ['old_pos','new_pos']
        change_df['change_pos'] = change_df['new_pos']-change_df['old_pos']
    else:
        data_old = portfolio_pos_df[['tradeShare','contract_num_drt']].copy()
        data_old = data_old.set_index('tradeShare')
        data_new = portfolio_pos_df_new[['tradeShare','contract_num_drt']].copy()
        data_new = data_new.set_index('tradeShare')
        change_df_l = pd.concat([data_old[data_old['contract_num_drt']>0],data_new[data_new['contract_num_drt']>0]],axis=1,join='outer',sort=False).fillna(0)
        change_df_s = pd.concat([data_old[data_old['contract_num_drt']<0],data_new[data_new['contract_num_drt']<0]],axis=1,join='outer',sort=False).fillna(0)
        change_df_l.columns = ['old_pos','new_pos']
        change_df_l['change_pos'] = change_df_l['new_pos']-change_df_l['old_pos']
        change_df_s.columns = ['old_pos','new_pos']
        change_df_s['change_pos'] = change_df_s['new_pos']-change_df_s['old_pos']
        change_df = change_df_l.append(change_df_s)
    #计算交易信号
    trade_df = pd.DataFrame()
    for i in range(len(change_df)):
        #计算信号
        if change_df.iloc[i]['change_pos'] != 0:
            share = change_df.index[i]
            #如果优先平对锁仓（适合check_pos时，portfolio_pos_df输入为账户持仓）
            if firstTradeLock == True:
                longlock = list(portfolio_pos_df[(portfolio_pos_df['tradeShare'] == share) & (portfolio_pos_df['contract_num_drt']>0)]['contract_num_drt'])
                if longlock == []:
                    long_num = 0
                else:
                    long_num = longlock[0]
                shortlock = list(portfolio_pos_df[(portfolio_pos_df['tradeShare'] == share) & (portfolio_pos_df['contract_num_drt']<0)]['contract_num_drt'])
                if shortlock == []:
                    short_num = 0
                else:
                    short_num = abs(shortlock[0])
                # 多头开仓/多头增仓/多头减仓/多头平仓
                if change_df.iloc[i]['old_pos'] >= 0 and change_df.iloc[i]['new_pos'] >= 0 :
                    if change_df.iloc[i]['change_pos'] > 0:
                        trade_df = trade_df.append([[share,'long','close',min(abs(change_df.iloc[i]['change_pos']),short_num)]])
                        trade_df = trade_df.append([[share,'long','open',abs(change_df.iloc[i]['change_pos']-min(abs(change_df.iloc[i]['change_pos']),short_num))]])
                    elif change_df.iloc[i]['change_pos'] < 0: 
                        trade_df = trade_df.append([[share,'short','close',abs(change_df.iloc[i]['change_pos'])]])
                # 空头开仓/空头增仓/空头减仓/空头平仓
                elif change_df.iloc[i]['old_pos'] <= 0 and change_df.iloc[i]['new_pos'] <= 0:
                    if change_df.iloc[i]['change_pos'] < 0:
                        trade_df = trade_df.append([[share,'short','close',min(abs(change_df.iloc[i]['change_pos']),long_num)]])
                        trade_df = trade_df.append([[share,'short','open',(abs(change_df.iloc[i]['change_pos'])-min(abs(change_df.iloc[i]['change_pos']),long_num))]])
                    elif change_df.iloc[i]['change_pos'] > 0: 
                        trade_df = trade_df.append([[share,'long','close',abs(change_df.iloc[i]['change_pos'])]])
                #方向变化(多转空)
                elif change_df.iloc[i]['old_pos'] > 0 and change_df.iloc[i]['new_pos'] < 0:
                    trade_df = trade_df.append([[share,'short','close',abs(change_df.iloc[i]['old_pos'])]])
                    trade_df = trade_df.append([[share,'short','close',min(abs(change_df.iloc[i]['new_pos']),long_num-abs(change_df.iloc[i]['old_pos']))]])
                    trade_df = trade_df.append([[share,'short','open',abs(change_df.iloc[i]['new_pos'])-min(abs(change_df.iloc[i]['new_pos']),long_num-abs(change_df.iloc[i]['old_pos']))]])
                #方向变化(空转多)
                elif change_df.iloc[i]['old_pos'] < 0 and change_df.iloc[i]['new_pos'] > 0:
                    trade_df = trade_df.append([[share,'long','close',abs(change_df.iloc[i]['old_pos'])]])
                    trade_df = trade_df.append([[share,'long','close',min(abs(change_df.iloc[i]['new_pos']),short_num-abs(change_df.iloc[i]['old_pos']))]])
                    trade_df = trade_df.append([[share,'long','open',abs(change_df.iloc[i]['new_pos'])-min(abs(change_df.iloc[i]['new_pos']),short_num-abs(change_df.iloc[i]['old_pos']))]])
            #如果优先开仓（适合计算信号开仓时，portfolio_pos_df输入为原策略持仓））
            else:
                 # 多头开仓/多头增仓/多头减仓/多头平仓
                if change_df.iloc[i]['old_pos'] >= 0 and change_df.iloc[i]['new_pos'] >= 0 :
                    if change_df.iloc[i]['change_pos'] > 0:
                        trade_df = trade_df.append([[share,'long','open',abs(change_df.iloc[i]['change_pos'])]])
                    elif change_df.iloc[i]['change_pos'] < 0: 
                        trade_df = trade_df.append([[share,'short','close',abs(change_df.iloc[i]['change_pos'])]])
                # 空头开仓/空头增仓/空头减仓/空头平仓
                elif change_df.iloc[i]['old_pos'] <= 0 and change_df.iloc[i]['new_pos'] <= 0:
                    if change_df.iloc[i]['change_pos'] < 0:
                        trade_df = trade_df.append([[share,'short','open',abs(change_df.iloc[i]['change_pos'])]])
                    elif change_df.iloc[i]['change_pos'] > 0: 
                        trade_df = trade_df.append([[share,'long','close',abs(change_df.iloc[i]['change_pos'])]])
                #方向变化(多转空)
                elif change_df.iloc[i]['old_pos'] > 0 and change_df.iloc[i]['new_pos'] < 0:
                    trade_df = trade_df.append([[share,'short','close',abs(change_df.iloc[i]['old_pos'])]])
                    trade_df = trade_df.append([[share,'short','open',abs(change_df.iloc[i]['new_pos'])]])
                #方向变化(空转多)
                elif change_df.iloc[i]['old_pos'] < 0 and change_df.iloc[i]['new_pos'] > 0:
                    trade_df = trade_df.append([[share,'long','close',abs(change_df.iloc[i]['old_pos'])]])
                    trade_df = trade_df.append([[share,'long','open',abs(change_df.iloc[i]['new_pos'])]])
        else:
            pass
    if trade_df.empty == False:
        trade_df.columns=['share','direction','offset','num']
        trade_df.reset_index(inplace=True, drop=True)
        trade_df = trade_df[trade_df['num']!=0]
    else:
        trade_df = pd.DataFrame(columns=['share','direction','offset','num'])
    return trade_df,change_df

# ====由头寸变动转换最新交易信号(针对股指)
def computeTradeSingal_gz(portfolio_pos_df,portfolio_pos_df_new):
    '''
    如果firstTradeLock=True（优先平对锁仓），则在策略发出信号时要将portfolio_pos_df输入为账户持仓而不是原策略持仓，因为账户持仓才有锁仓信息
    '''
    data_old = portfolio_pos_df[['tradeShare','contract_num_drt']].copy()
    data_old = data_old.set_index('tradeShare')
    data_new = portfolio_pos_df_new[['tradeShare','contract_num_drt']].copy()
    data_new = data_new.set_index('tradeShare')
    change_df_l = pd.concat([data_old[data_old['contract_num_drt']>0],data_new[data_new['contract_num_drt']>0]],axis=1,join='outer',sort=False).fillna(0)
    change_df_s = pd.concat([data_old[data_old['contract_num_drt']<0],data_new[data_new['contract_num_drt']<0]],axis=1,join='outer',sort=False).fillna(0)
    change_df_l.columns = ['old_pos','new_pos']
    change_df_l['change_pos'] = change_df_l['new_pos']-change_df_l['old_pos']
    change_df_s.columns = ['old_pos','new_pos']
    change_df_s['change_pos'] = change_df_s['new_pos']-change_df_s['old_pos']
    change_df = change_df_l.append(change_df_s)
    #计算交易信号
    trade_df = pd.DataFrame()
    for i in range(len(change_df)):
        #计算信号
        if change_df.iloc[i]['change_pos'] != 0:
            #锁仓情况
            share = change_df.index[i]
            long_pos = portfolio_pos_df[(portfolio_pos_df['tradeShare'] == share) & (portfolio_pos_df['contract_num_drt']>0)]
            longlock = list(long_pos['contract_num_drt'])
            if longlock == []:
                long_num = 0
            else:
                long_num = longlock[0]
            short_pos = portfolio_pos_df[(portfolio_pos_df['tradeShare'] == share) & (portfolio_pos_df['contract_num_drt']<0)]
            shortlock = list(short_pos['contract_num_drt'])
            if shortlock == []:
                short_num = 0
            else:
                short_num = abs(shortlock[0])
            #做多
            if change_df.iloc[i]['change_pos'] > 0:
                #---如果没有空单，则开多
                if short_num == 0 :
                    trade_df = trade_df.append([[share,'long','open',abs(change_df.iloc[i]['change_pos'])]])
                #---如果只有空单昨仓，没有空单今仓，则先平空后开多
                elif (short_pos.iloc[0]['yesterday_num_drt']<0) and (short_pos.iloc[0]['today_num_drt']==0):
                    trade_df = trade_df.append([[share,'long','close',min(abs(change_df.iloc[i]['change_pos']),short_num)]])
                    trade_df = trade_df.append([[share,'long','open',abs(change_df.iloc[i]['change_pos']-min(abs(change_df.iloc[i]['change_pos']),short_num))]])
                #---如果有空单今仓，则开多
                elif short_pos.iloc[0]['today_num_drt']<0:
                    trade_df = trade_df.append([[share,'long','open',abs(change_df.iloc[i]['change_pos'])]])
            #做空
            elif change_df.iloc[i]['change_pos'] < 0:
                #---如果没有多单，则开空
                if long_num == 0 :
                    trade_df = trade_df.append([[share,'short','open',abs(change_df.iloc[i]['change_pos'])]])
                #---如果只有多单昨仓，没有多单今仓，则先平多后开空
                elif (long_pos.iloc[0]['yesterday_num_drt']>0) and (long_pos.iloc[0]['today_num_drt']==0):
                    trade_df = trade_df.append([[share,'short','close',min(abs(change_df.iloc[i]['change_pos']),long_num)]])
                    trade_df = trade_df.append([[share,'short','open',(abs(change_df.iloc[i]['change_pos'])-min(abs(change_df.iloc[i]['change_pos']),long_num))]])
                #---如果有多单今仓，则开空
                elif long_pos.iloc[0]['today_num_drt']>0:
                    trade_df = trade_df.append([[share,'short','open',abs(change_df.iloc[i]['change_pos'])]])
        else:
            pass
    if trade_df.empty == False:
        trade_df.columns=['share','direction','offset','num']
        trade_df.reset_index(inplace=True, drop=True)
        trade_df = trade_df[trade_df['num']!=0]
    else:
        trade_df = pd.DataFrame(columns=['share','direction','offset','num'])
    return trade_df,change_df
# ====今昨仓转换（上期所，上能源所优先平今，设置为close平仓时如果无昨仓会报错）
def convertOrderClass(trade_df,pos_df,sendDing=True):
    try:
        convert_trade_df = pd.DataFrame()
        for i in range(len(trade_df)):
            tradeShare = trade_df.iloc[i]['share']
            if tradeShare.split('.')[1] in ['SHFE','INE']:
                if trade_df.iloc[i]['offset'] == 'close':
                    # 今仓数量
                    if trade_df.iloc[i]['direction'] == 'long':
                        today_num = list(pos_df[(pos_df['tradeShare']==tradeShare) &(pos_df['contract_num_drt']<0)]['today_num_drt'])[0]
                    elif trade_df.iloc[i]['direction'] == 'short':
                        today_num = list(pos_df[(pos_df['tradeShare']==tradeShare) &(pos_df['contract_num_drt']>0)]['today_num_drt'])[0]
                    today_num = abs(today_num)
                    # 如果无今仓，直接平昨仓
                    if today_num == 0:
                        convert_trade_df = convert_trade_df.append([[tradeShare,trade_df.iloc[i]['direction'],'closeyesterday',trade_df.iloc[i]['num']]])  
                    # 如果有今仓，则优先平今仓
                    else:
                        if trade_df.iloc[i]['num'] <= today_num:
                            #平今仓
                            convert_trade_df = convert_trade_df.append([[tradeShare,trade_df.iloc[i]['direction'],'closetoday',trade_df.iloc[i]['num']]])
                        elif trade_df.iloc[i]['num'] > today_num:
                            #先平进仓
                            convert_trade_df = convert_trade_df.append([[tradeShare,trade_df.iloc[i]['direction'],'closetoday',today_num]])
                            #再平昨仓
                            convert_trade_df = convert_trade_df.append([[tradeShare,trade_df.iloc[i]['direction'],'closeyesterday',trade_df.iloc[i]['num'] - today_num]])  
                elif trade_df.iloc[i]['offset'] == 'open':
                    convert_trade_df = convert_trade_df.append([[tradeShare,trade_df.iloc[i]['direction'],'open',trade_df.iloc[i]['num']]])
            else:
                convert_trade_df = convert_trade_df.append([[tradeShare,trade_df.iloc[i]['direction'],trade_df.iloc[i]['offset'],trade_df.iloc[i]['num']]])
        convert_trade_df.columns=['share','direction','offset','num']
        convert_trade_df.reset_index(inplace=True, drop=True)  
    except Exception as e:
        print(e)
        log('***ERROR: '+str(e))
        log('【convertOrderClass在运算%s出现错误】，程序终止，请检查代码'%trade_df.iloc[i]['share'])
        if sendDing == True:
            send_dingding_and_raise_error('【convertOrderClass在运算%s出现错误】，程序终止，请检查代码'%trade_df.iloc[i]['share'])
    return convert_trade_df
# ====提交委托单
def postOrder(main_engine,trade_df,sendDing=True):
    if sendDing == True:
        send_data = '【报送委托单】\n当前时间为：%s'%datetime.now().strftime('%Y%m%d %H:%M:%S') + '\n'
        send_data += '------------------------\n'
        send_data += '合约， 方向， 开平， 数量\n'
        for i in range(len(trade_df)):
            send_data += str(trade_df.iloc[i]['share']).split('.')[0] +',  '
            send_data += str(trade_df.iloc[i]['direction']) +',  '
            send_data += str(trade_df.iloc[i]['offset']) +',  '
            send_data += str(int(trade_df.iloc[i]['num'])) +',  \n'
        send_dingding_msg(send_data)
    orderid_list = []
    for i in range(len(trade_df)):
        try:
            tradeShare = trade_df.iloc[i]['share']
            #交易合约
            contract = main_engine.get_contract(tradeShare)
            #交易方向
            if trade_df.iloc[i]['direction'] == 'long':
                order_direction = Direction.LONG
            elif trade_df.iloc[i]['direction'] == 'short':
                order_direction = Direction.SHORT   
            #交易数量
            num = trade_df.iloc[i]['num']
            #交易开平仓
            if trade_df.iloc[i]['offset'] == 'open':
                order_offset = Offset.OPEN
            elif trade_df.iloc[i]['offset'] == 'close':
                order_offset = Offset.CLOSE
            elif trade_df.iloc[i]['offset'] == 'closetoday':
                order_offset = Offset.CLOSETODAY
            elif trade_df.iloc[i]['offset'] == 'closeyesterday':
                order_offset = Offset.CLOSEYESTERDAY
            #委托价格
            pricedata = main_engine.get_tick(tradeShare) 
            limit_up = pricedata.limit_up
            limit_down = pricedata.limit_down
            if trade_df.iloc[i]['direction'] == 'long':
                orderprice = pricedata.last_price + 10*contract.pricetick
                orderprice = min(orderprice,limit_up)
            elif trade_df.iloc[i]['direction'] == 'short':
                orderprice = pricedata.last_price - 10*contract.pricetick
                orderprice = max(orderprice,limit_down)
            #涨跌停预警（如果合约在涨跌停状态，则发送钉钉，需要人工手动挂单）
            if pricedata.last_price == limit_up:
                print('%s当前处于涨停状态,请做手动挂单处理,%s,%s,%s'%(tradeShare,trade_df.iloc[i]['direction'],trade_df.iloc[i]['offset'],num))
                log('%s当前处于涨停状态,请做手动挂单处理,%s,%s,%s'%(tradeShare,trade_df.iloc[i]['direction'],trade_df.iloc[i]['offset'],num))
                send_dingding_msg('%s当前处于涨停状态,请做手动挂单处理,%s,%s,%s'%(tradeShare,trade_df.iloc[i]['direction'],trade_df.iloc[i]['offset'],num))
                continue
            elif pricedata.last_price == limit_down:
                print('%s当前处于跌停状态,请做手动挂单处理,%s,%s,%s'%(tradeShare,trade_df.iloc[i]['direction'],trade_df.iloc[i]['offset'],num))
                log('%s当前处于跌停状态,请做手动挂单处理,%s,%s,%s'%(tradeShare,trade_df.iloc[i]['direction'],trade_df.iloc[i]['offset'],num))
                send_dingding_msg('%s当前处于跌停状态,请做手动挂单处理,%s,%s,%s'%(tradeShare,trade_df.iloc[i]['direction'],trade_df.iloc[i]['offset'],num))
                continue
            else:
                pass
            #委托单
            req = OrderRequest(
                                symbol = contract.symbol,
                                exchange  =contract.exchange,
                                direction = order_direction,
                                type = OrderType.LIMIT,
                                volume = num,
                                price = round(orderprice,3),
                                offset = order_offset
                                )
            vt_orderid = main_engine.send_order(req, contract.gateway_name)
            orderid_list.append(vt_orderid)
        except Exception as e:
            print(e)
            log('***ERROR: '+str(e))
            log('【postOrder在运算%s出现错误】，委托单继续下一个报单，程序继续运行，请检查代码'%tradeShare)
            if sendDing == True:
                send_dingding_msg('【postOrder在运算%s出现错误】，委托单继续下一个报单，程序继续运行，请检查代码'%tradeShare)
    return orderid_list

# ====检查委托单成交状态
def checkOderStatus(main_engine,orderid_list,sendDing=True):
    status_df = pd.DataFrame()
    if orderid_list == []:
        return status_df
    for i in orderid_list:
        try:
            status = main_engine.get_order(i)
            status_df.loc[i,'交易合约'] = status.symbol
            status_df.loc[i,'方向'] = status.direction.value
            status_df.loc[i,'开平'] = status.offset.value
            status_df.loc[i,'成交价格'] = status.price
            status_df.loc[i,'委托数量'] = status.volume
            status_df.loc[i,'成交数量'] = status.traded
            status_df.loc[i,'成交状态'] = status.status.value
        except Exception as e:
            print(e)
            log('***ERROR: '+str(e))
            log('【checkOderStatus在运算%s出现错误】，委托单继续查询，程序继续运行，请检查代码'%i)  
            if sendDing == True:
                send_dingding_msg('【checkOderStatus在运算%s出现错误】，委托单继续查询，程序继续运行，请检查代码'%i)
    failTrade_df = status_df[status_df['成交状态']!='全部成交']
    print(status_df)
    log(status_df)
    send_data = '【当前所有委托单成交状态为】\n'
    send_data += '合约,方向,开平,成交价格,委托数量,成交数量,成交状态\n'
    send_data += '------------------\n'
    for i in range(len(status_df)):
        send_data += str(status_df.iloc[i]['交易合约'])+', '
        send_data += str(status_df.iloc[i]['方向'])+', '
        send_data += str(status_df.iloc[i]['开平'])+', '
        send_data += str(status_df.iloc[i]['成交价格'])+', '
        send_data += str(int(status_df.iloc[i]['委托数量']))+', '
        send_data += str(int(status_df.iloc[i]['成交数量']))+', '
        send_data += str(status_df.iloc[i]['成交状态']) + '\n'
    if failTrade_df.empty == False:
        print('【注意】当前有未成交单，请检查')
        log('【注意】当前有未成交单，请检查')
        send_data += '------------------\n'
        send_data += '【注意】当前有未成交单，请检查'
    else:
        print('所有委托单全部成交')
        log('所有委托单全部成交')
        send_data += '------------------\n'
        send_data += '所有委托单全部成交'
    if sendDing == True:
        send_dingding_msg(send_data)
    return status_df

# ====查看当前账户持仓
def getAcoountPos(main_engine):
    try:
        pos_df = pd.DataFrame(columns=['tradeShare','contract_num_drt'])
        pos_data = main_engine.get_all_positions()
        if pos_data!= []:
            for i in range(len(pos_data)):
                pos_df.loc[i,'tradeShare'] = pos_data[i].symbol +'.'+ pos_data[i].exchange.value
                if  pos_data[i].direction.value == '多':
                    direction = 1
                elif pos_data[i].direction.value == '空':
                    direction = -1
                pos_df.loc[i,'contract_num_drt'] = pos_data[i].volume * direction
                pos_df.loc[i,'yesterday_num_drt'] = pos_data[i].yd_volume * direction
                pos_df.loc[i,'today_num_drt'] = pos_df.loc[i,'contract_num_drt']-pos_df.loc[i,'yesterday_num_drt']
            pos_df = pos_df[pos_df['contract_num_drt']!=0]
    except Exception as e:
        log('***ERROR: '+str(e))
        log('【getAcoountPos出现错误】，程序终止，请检查代码')  
        send_dingding_and_raise_error('【getAcoountPos出现错误】，程序终止，请检查代码')
    return pos_df


# ====撤销所有委托单
def cancelAllOrders(main_engine):
    try:
        allOrders_list = main_engine.get_all_active_orders()
        if allOrders_list != []:
            for i in allOrders_list:
                tradeShare = i.symbol + '.' + i.exchange.value
                contract = main_engine.get_contract(tradeShare)
                req = CancelRequest(
                                    orderid = i.orderid,
                                    symbol = contract.symbol,  
                                    exchange  =contract.exchange
                                    )
                main_engine.cancel_order(req, contract.gateway_name)
            print('所有委托单已撤销')
    except Exception as e:
        print(e)
        log('***ERROR: '+str(e))
        log('【cancelAllOrders出现错误】，程序继续运行，请检查代码并撤销所有委托单') 
        send_dingding_msg('【cancelAllOrders出现错误】，程序继续运行，请检查代码并撤销所有委托单')
# ====检查仓位
def checkAccountPos(main_engine,portfolio_pos_df_new,ignore_share_df=pd.DataFrame([],columns=['share']),max_try_amount=5,needfix=True,sendDing=True,ignore_list=[]):
    i=0
    for s in range(max_try_amount):
        try:
            # sleep(10)  #防止持仓未更新
            have_send_ding = False
            account_pos_df = getAcoountPos(main_engine)
            correct_df,check_df = computeTradeSingal(account_pos_df,portfolio_pos_df_new,firstTradeLock=True)
            correct_df = pd.DataFrame(correct_df[[i not in list(ignore_share_df['share']) for i in correct_df['share']]]).copy()
            if correct_df.empty == True:
                correct_df = pd.DataFrame(columns=['share','direction','offset','num'])
            check_df.columns = ['账户净持仓','策略仓位','相差头寸']
            #过滤合约
            check_df = pd.DataFrame(check_df[[i not in list(ignore_share_df['share']) for i in check_df.index]]).copy()
            #过滤品种
            correct_df = pd.DataFrame(correct_df[[format_to_onlyLetter(i) not in ignore_list for i in correct_df['share']]]).copy()
            check_df = pd.DataFrame(check_df[[format_to_onlyLetter(i) not in ignore_list for i in check_df.index]]).copy()
            #过滤组合持仓
            if correct_df.empty == True:
                correct_df = pd.DataFrame(columns=['share','direction','offset','num'])
            correct_df = pd.DataFrame(correct_df[['&' not in i for i in correct_df['share']]]).copy()
            check_df = pd.DataFrame(check_df[['&' not in i  for i in check_df.index]]).copy()
            if correct_df.empty == True:
                status_df = pd.DataFrame()
                print('策略净持仓与实际持仓一致')
                log('策略净持仓与实际持仓一致')
                if i > 0: #如果是校准之后的，则向钉钉发送信息
                    if sendDing == True:
                        send_dingding_msg('策略净持仓与实际持仓一致')
                        sendLastestPos(check_df)
                        have_send_ding = True
                break
            elif correct_df.empty == False:
                #先撤销所有委托单（为防止出现行情变动太快而导致加了10tick仍然未能成交的情况）
                cancelAllOrders(main_engine)
                status_df = pd.DataFrame()
                check_df = check_df[check_df['相差头寸']!=0]
                convert_correct_df = convertOrderClass(correct_df,account_pos_df)
                print('策略净持仓与实际持仓不一致')
                log('策略净持仓与实际持仓不一致')
                print(check_df)
                log(check_df)
                if sendDing == True:
                    send_data = '【策略持仓与实际持仓不一致】\n'
                    send_data += '-------------------------------\n'
                    send_data += '合约,账户净持仓,策略仓位,相差头寸\n'
                    for j in range(len(check_df)):
                        send_data += str(check_df.index[j]).split('.')[0] +',   '
                        send_data += str(check_df.iloc[j]['账户净持仓']) +',   '
                        send_data += str(check_df.iloc[j]['策略仓位']) +',   '
                        send_data += str(check_df.iloc[j]['相差头寸']) + '\n'
                    send_dingding_msg(send_data)
                    have_send_ding = True
                # 仓位修复
                if needfix == True:
                    '''
                    为防止api出现延迟，等待10s后仍然成交结果不更新到最新持仓，避免出现重复下单的情况，第二次修复仓位需要手动确认
                    '''
                    if i >= 1:
                        print('！！！注意\n上一次修复仓位未成功，为避免重复下单，请检查实际成交结果后前往后台程序输入是否再次修复仓位\n！！！')
                        log('！！！注意\n上一次修复仓位未成功，为避免重复下单，请检查实际成交结果后前往后台程序输入是否再次修复仓位\n！！！')
                        send_dingding_msg('！！！注意\n第一次修复仓位未成功，为避免重复下单，请检查实际成交结果后前往后台程序输入是否再次修复仓位\n！！！')
                        confirmfix = input('请确认是否要进行修复（y/n）')
                        if confirmfix == 'y':
                            # #先撤销所有委托单（为防止出现行情变动太快而导致加了10tick仍然未能成交的情况）
                            # cancelAllOrders(main_engine)
                            i=0
                            continue
                            # orderid_list = postOrder(main_engine,convert_correct_df,sendDing)
                        else:
                            print('暂不进行修复')
                            break
                    elif i == 0:
                        print('即将进行仓位修复')
                        log('即将进行仓位修复')
                        # #先撤销所有委托单（为防止出现行情变动太快而导致加了10tick仍然未能成交的情况）
                        # cancelAllOrders(main_engine)
                        # 出现大于5个品种仓位不一致则先确认是否是由数据错误引起的，确认确实需要修复后再进行修复
                        if len(check_df) >= 5:
                            print('！！！注意\需要修复的品种大于5个，请检查是否正确后前往后台程序输入是否再次修复仓位\n！！！')
                            log('！！！注意\n需要修复的品种大于5个，请检查是否正确后前往后台程序输入是否再次修复仓位\n！！！')
                            send_dingding_msg('！！！注意\n需要修复的品种大于5个，请检查是否正确后前往后台程序输入是否再次修复仓位\n！！！')
                            confirmfix = input('请确认是否要进行修复（y/n）')
                        else:
                            confirmfix = 'y'
                        if confirmfix == 'y': 
                            orderid_list = postOrder(main_engine,convert_correct_df,sendDing)
                            # 成交检查
                            sleep(1)  #防止成交结果未更新
                            status_df = checkOderStatus(main_engine,orderid_list,sendDing)
                            sleep(10)  #防止持仓未更新
                            cancelAllOrders(main_engine)
                            i += 1
                        else:
                            print('暂不进行修复')
                            break
                else:
                    status_df = pd.DataFrame()
                    print('暂不进行修复')
                    log('暂不进行修复')
                    if sendDing == True:
                        send_dingding_msg('暂不进行修复')
                    break
        except Exception as e:
            print('***ERROR: '+str(e))
            log('***ERROR: '+str(e))
            if s == (max_try_amount - 1):
                print('【checkAccountPos出现错误】，程序中止，请检查】')
                log('【checkAccountPos出现错误】，程序中止，请检查】')
                if sendDing == True:
                    send_dingding_and_raise_error('【checkAccountPos出现错误】，程序中止，请检查】')
    return correct_df,check_df,status_df,have_send_ding

# ====统计当日成交信息
def countPortfolioTrade(portfolio_df_dict,strategy_set_dict,tradedate):
    signal_count_df = pd.DataFrame()
    trade_count_df = pd.DataFrame()
    for i in  portfolio_df_dict.keys():
        #策略信号汇总
        for s in strategy_set_dict[i].keys():
            pick_df = pd.DataFrame(portfolio_df_dict[i]).copy()
            signal_df = pick_df[pick_df[s+'_signal']!=pick_df[s+'_signal'].shift(1)]
            signal_df = signal_df[['candle_begin_time',s+'_signal']]
            signal_df = signal_df[[t.strftime('%Y%m%d')==tradedate for t in signal_df['candle_begin_time']]]
            signal_df.columns = ['candle_begin_time','signal']
            signal_df['signalname'] = s
            signal_df['indexshare'] = i
            signal_count_df = pd.concat([signal_count_df,signal_df])
        #头寸变动汇总
        pick_df = pd.DataFrame(portfolio_df_dict[i]).copy()
        pick_df['trade_contract'] = (pick_df['contract_num_drt']-pick_df['contract_num_drt'].shift(1)).fillna(0)
        contract_df = pick_df[pick_df['trade_contract']!=0]
        contract_df = contract_df[[t.strftime('%Y%m%d')==tradedate for t in contract_df['candle_begin_time']]]
        contract_df = contract_df[['candle_begin_time','trade_contract']]
        contract_df['indexshare'] = i
        trade_count_df = pd.concat([trade_count_df,contract_df])
    signal_count_df.reset_index(inplace=True, drop=True)   
    trade_count_df.reset_index(inplace=True, drop=True)  
    return signal_count_df,trade_count_df

# ====获取当日全部成交记录
def getAllTrades(main_engine):
    trade_list = main_engine.get_all_trades()
    trade_df = pd.DataFrame()
    trade_df['交易合约'] = [i.symbol+'.'+i.exchange.value for i in trade_list]
    trade_df['方向'] = [i.direction.value for i in trade_list]
    trade_df['开平'] = [i.offset.value for i in trade_list]
    trade_df['成交价格'] = [i.price for i in trade_list]
    trade_df['成交数量'] = [i.volume for i in trade_list]
    trade_df['成交时间'] = [i.datetime.strftime('%Y%m%d %H:%M') for i in trade_list]
    return trade_df

# ====发送最新持仓信息至钉钉
def sendLastestPos(check_df):          
    check_df.columns=['账户净持仓','策略持仓','相差数量']
    check_df = check_df[((check_df['账户净持仓']==0) & (check_df['策略持仓']==0))==False]
    if check_df.empty == True:
        send_data = '\n【当前账户无仓位，策略无仓位】\n'
    elif check_df.empty == False:   
        send_data = '\n【当前持仓统计为】\n'
        send_data += '-------------------------------\n'
        send_data += '合约， 账户净持仓,  策略持仓,  相差数量\n'
        for i in range(len(check_df)):
            send_data += str(check_df.index[i]).split('.')[0] +',  '
            send_data += str(int(check_df.iloc[i]['账户净持仓'])) +',  '
            send_data += str(int(check_df.iloc[i]['策略持仓'])) +',  '
            send_data += str(int(check_df.iloc[i]['相差数量'])) +',  \n'
    send_dingding_msg(send_data)

# ====计算最大持仓限制
def computeMaxHold(portfolio_pos_df,limit_value,contract_multiplier_dict):
    # 获取前一个交易日的结算价    
    today_str = datetime.now().strftime('%Y/%m/%d')   
    beforeday_str = (datetime.now()-timedelta(days=15)).strftime('%Y/%m/%d')  
    settle_df = rq.get_price([format_to_RQshare(i) for i in list(portfolio_pos_df['tradeShare'])], fields=['settlement'],start_date=beforeday_str, end_date=today_str,frequency='1d')
    # risk_df
    risk_df = pd.DataFrame(index=portfolio_pos_df.index)
    risk_df['tradeShare'] = portfolio_pos_df['tradeShare']
    risk_df['contract_multiplier'] = [contract_multiplier_dict[re.sub("\d", "", i.split('.')[0])] for i in risk_df.index]
    risk_df['settle'] =  [float(settle_df.loc[format_to_RQshare(i)].iloc[-1]) for i in risk_df['tradeShare']]
    risk_df['limit_shares'] = round(limit_value/risk_df['contract_multiplier']/risk_df['settle'],0)
    return risk_df

# ====风控模块    
def riskControl(portfolio_pos_df,max_holdShares_df):
    control_list = []
    riskMsg_list = []
    for i in range(len(portfolio_pos_df)):
        share = portfolio_pos_df.index[i]
        num = portfolio_pos_df.iloc[i]['contract_num_drt']        
        if num >= 0:
            if num > max_holdShares_df.loc[share]['limit_shares']:
                print('%s 超过最大仓位限制 %s>%s'%(share,num,max_holdShares_df.loc[share]['limit_shares']))
                log('%s 超过最大仓位限制 %s>%s'%(share,num,max_holdShares_df.loc[share]['limit_shares']))
                # send_dingding_msg('%s 超过最大仓位限制 %s>%s'%(share,num,max_holdShares_df.loc[share]['limit_shares']))
                riskMsg_list.append('%s 超过最大仓位限制 %s>%s'%(share,num,max_holdShares_df.loc[share]['limit_shares']))
            control_list.append(min(num,max_holdShares_df.loc[share]['limit_shares']))
        elif num < 0:
            if num < -max_holdShares_df.loc[share]['limit_shares']:
                print('%s 超过最大仓位限制 %s<-%s'%(share,num,max_holdShares_df.loc[share]['limit_shares']))
                log('%s 超过最大仓位限制 %s<-%s'%(share,num,max_holdShares_df.loc[share]['limit_shares']))
                # send_dingding_msg('%s 超过最大仓位限制 %s<-%s'%(share,num,max_holdShares_df.loc[share]['limit_shares']))
                riskMsg_list.append('%s 超过最大仓位限制 %s>%s'%(share,num,max_holdShares_df.loc[share]['limit_shares']))
            control_list.append(max(num,-max_holdShares_df.loc[share]['limit_shares']))
    portfolio_pos_df['contract_num_drt'] = control_list
    return portfolio_pos_df,riskMsg_list










