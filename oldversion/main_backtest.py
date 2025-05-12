path = r'D:\CTATrade'
data_path = r'D:\futureData'
import sys
sys.path.append(path)
sys.path.append(data_path)
import futureDataManage
import datetime
import time
from datetime import timedelta,datetime
import pandas as pd
import numpy as np
import re
from multiprocessing import Pool
import multiprocessing
from strategy import brandy,tequila,whisky,whiskyMix
from backtest import backtestStrategy
import BacktestParameter as BP
import matplotlib.pyplot as plt
from tradeConfig import rqdata_info
from contract_info import price_tick_dict,contract_multiplier_dict
from portfolio_set import *
import rqdatac as rq
# rq.init(rqdata_info['user'], rqdata_info['password'])
plt.rcParams['font.sans-serif']=['SimHei'] #用来正常显示中文标签
plt.rcParams['axes.unicode_minus']=False #用来正常显示负号
'''定义策略回测方法'''
def BacktestPnL(symbol,
                strategy,
                para,
                time_interval = '10m',
                drop_days = 21,
                slippageMulti = 1,
                dataUpdate = True,
                capitalmode = 'atr',
                capital_unit = 10000,
                multiCompute = False,
                limitValue = None,
                starttime = datetime(2011,1,1),
                margin_percent = 0.20,
                save_csv = False,
                long=1,
                short=-1):   
    '''基础信息 ''' 
    # symbol = 'rb99.SHFE'
    #定义数据路径
    h5_dataPath = data_path +'/h5_Data' 
    #合约乘数
    # contractMulti = rq.instruments(symbol.split('.')[0].upper()).contract_multiplier
    contractMulti = contract_multiplier_dict[re.sub("\d", "", symbol.split('.')[0])]
    #最小波动/手续费
    minpoint = price_tick_dict[re.sub("\d", "", symbol.split('.')[0])]
    c_rate = 0.23/10000
    #今日日期  
    today_date = (datetime.now()).date().strftime('%Y-%m-%d')  
    '''数据回补'''
    if dataUpdate == True:
        futureDataManage.updateDATA(symbol,time_interval,dayStartTime='09:00',dayEndTime='15:00',get_beforeData=False,remake_h5=False)
    '''回测策略'''
    print(symbol+' 开始回测')
    df = pd.read_hdf(h5_dataPath+'/%s.h5' % '_'.join([symbol.split('.')[0],time_interval]), key='df')
    df = df[df['candle_begin_time']>starttime]
    df = backtestStrategy(df,
                          strategy,
                          minpoint,
                          para,
                          c_rate ,
                          drop_days,
                          slippageMulti,
                          contractMulti,
                          capitalmode,
                          capital_unit=capital_unit,
                          multiCompute=multiCompute,
                          limitValue=limitValue,
                          margin_percent=margin_percent,
                          long=long,
                          short=short
                          )

    print(symbol+' 回测完成')
    '''存储csv'''
    if save_csv == True:
        df.to_csv(path+'/backtest_report/%s.csv'%'_'.join([symbol.split('.')[0],today_date,time_interval]))
    return df

'''回测单策略'''
# 设置投资组合
portfolio_set_dict = {}
# portfolio_set_dict.update(portfolio_set_dict1)
# portfolio_set_dict.update(portfolio_set_dict2)
# portfolio_set_dict.update(portfolio_set_dict3)
# portfolio_set_dict.update(portfolio_set_dict4)
# portfolio_set_dict.update(portfolio_set_dict5)
# portfolio_set_dict.update(portfolio_set_dict6)
# portfolio_set_dict.update(portfolio_set_dict7)
# portfolio_set_dict.update(portfolio_set_dict8)
portfolio_set_dict.update(portfolio_set_dict9)

#定义回测主方法
def run_backtest(strategyname):
    df = BacktestPnL(symbol = portfolio_set_dict[strategyname]['symbol'],
                     strategy = portfolio_set_dict[strategyname]['strategy'],
                     para = portfolio_set_dict[strategyname]['para'],
                     time_interval = portfolio_set_dict[strategyname]['time_interval'],
                     drop_days = 21,
                     slippageMulti = 1,
                     dataUpdate = False,   #不在这里回补数据，在下面main统一回补数据
                     capitalmode = portfolio_set_dict[strategyname]['capitalmode'],
                     capital_unit = portfolio_set_dict[strategyname]['capital_unit'],
                     multiCompute = False,
                     limitValue = portfolio_set_dict[strategyname]['limitValue'],
                     starttime = datetime(2011,1,1),
                     long = portfolio_set_dict[strategyname]['long'],
                     short = portfolio_set_dict[strategyname]['short'],
                        )
    return {strategyname:df}

if __name__ == '__main__':
    #回补数据
    symbol_list = list(set([portfolio_set_dict[i]['symbol'] for i in portfolio_set_dict.keys()]))
    dataUpdate = False
    if dataUpdate == True:
        for symbol in symbol_list:
            futureDataManage.updateDATA(symbol,time_interval='10m',dayStartTime='09:00',dayEndTime='15:00',get_beforeData=False,remake_h5=False)
    #设置多进程
    second1 = time.time()
    multiprocessing.freeze_support() 
    pool = Pool(12) #对应逻辑处理器数量
    #设置投资组合
    portfolio_list =  list(portfolio_set_dict.keys())
    result_list = pool.imap_unordered(run_backtest, portfolio_list) 
    #运行多进程
    portfolio_backtest_dict = {}
    for result in result_list:
        print(list(result.keys())[0] + ' 回测完成')
        portfolio_backtest_dict[list(result.keys())[0]] = list(result.values())[0] 
    #关闭进程池，不再接受新的进程
    pool.close()
    #主进程阻塞等待子进程的退出 
    pool.join()
    second2 = time.time()
    print('全部回测完成')
    print('回测用时%0.02fs'%(second2-second1))
    sys.exit(0)
    
    '''组合计算'''
    #定义投资组合
    portfolio_list = [portfolio_backtest_dict[i] for i in portfolio_set_dict.keys()]
    # a = portfolio_backtest_dict['IF_Brandy'].tail(1000)
    # portfilio_df.cumsum().plot(figsize=(12,7),grid=True)
    # portfolio_backtest_dict['ru_Brandy']['close'].plot()
    # (portfolio_backtest_dict['IF_Tequila']['profit_cum']/2000000).plot(figsize=(12,7),grid=True)
    #组合盈亏
    portfilio_df = pd.concat([i['net_profit'].resample('1d').sum() for i in portfolio_list],axis=1,join='outer')
    portfilio_df['2012':].apply(lambda x:x.sum(),axis=1).cumsum().plot(figsize=(12,5),grid=True)
    sys.exit(0)
    print('总盈利：%s'%list(portfilio_df.apply(lambda x:x.sum(),axis=1).cumsum())[-1])
    
    #单策略组各品种盈亏
    for i in list(portfolio_set_dict5.keys()):
        print(i,portfolio_backtest_dict[i].iloc[-1]['profit_cum'])
        portfolio_backtest_dict[i]['profit_cum'].plot(figsize=(12,7),grid=True)
    
    #自定义组合盈亏
    portfolio_df_sub = pd.DataFrame()
    for symbolname in ['ss','eg','SA','SM','SM']:
        for i in ['Brandy','Whisky2','Whisky3']:
            portfolio_backtest_dict[symbolname+'_'+i]['net_profit'].cumsum().plot(figsize=(10,5),grid=True)
            portfolio_df_sub = pd.concat([portfolio_df_sub, portfolio_backtest_dict[symbolname+'_'+i]['net_profit'].cumsum()],axis=1)
    portfolio_df_sub.index = pd.to_datetime(portfolio_df_sub.index)
    portfolio_df_sub['2018':].apply(lambda x:x.sum(),axis=1).plot(figsize=(10,5),grid=True)

    #组合占用资金
    portfilio_df_capital = pd.concat([i['capital_use'].resample('1d').max() for i in portfolio_list],axis=1,join='outer')
    portfilio_df_capital.apply(lambda x:x.sum(),axis=1).plot(figsize=(12,7),grid=True)
    print('保证金最高占用：%s'%portfilio_df_capital.apply(lambda x:x.sum(),axis=1).max())
    
    #分策略相关性
    print('分策略相关性矩阵：')
    print(portfilio_df.corr())
    
    #组合相关性
    portfolio_dict_list = [portfolio_set_dict1,portfolio_set_dict2,portfolio_set_dict4,portfolio_set_dict5]
    corr_list = []
    for i in portfolio_dict_list:
        return_list = [portfolio_backtest_dict[i] for i in i.keys()]
        return_df = pd.concat([i['net_profit'].resample('1d').sum() for i in return_list],axis=1,join='outer')
        return_s = return_df.apply(lambda x:x.sum(),axis=1)
        corr_list.append(return_s)
    corr_df = pd.concat(corr_list,axis=1)
    corr_df.cumsum().plot(figsize=(10,4),grid=True)
    print(corr_df.corr())
    print(corr_df.std())
    print(corr_df.mean())
    
    '''组合策略评价参数'''
    amount_money = 6000000
    dayNAV_s=(portfilio_df.apply(lambda x:x.sum(),axis=1)).resample('1d').sum()/amount_money
    # dayNAV_s['20120101':].to_excel(r'D:\CTATrade\tequila2.xlsx')
    #单利曲线（风险敞口不变）
    (dayNAV_s.resample('d').sum().cumsum()+1).plot(figsize=(10,4),grid=True,c='r')
    #复利曲线（风险敞口动态变化）
    (dayNAV_s.resample('w').sum()+1).cumprod().plot(figsize=(10,5),grid=True)
    print('AROR: %s'%BP.AROR(dayNAV_s,freq='allday'))#
    print('AVOl: %s'%BP.AVol(dayNAV_s,freq='allday'))
    print('Sharp: %s'%BP.Sharp(dayNAV_s,0.0,freq='allday'))
    print('MDD: %s'%BP.Max_dd(dayNAV_s))
    print('Calmar: %s'%BP.Calmar(dayNAV_s,freq='allday'))
    print('最大回撤%0.0f元'%(BP.Max_dd(dayNAV_s)[0]*amount_money))
    #动态回撤
    NAVcum=np.array(dayNAV_s).cumsum()
    count_df=pd.DataFrame(dayNAV_s)
    count_df.columns = ['日收益率'] 
    count_df['累计收益率']=NAVcum
    count_df['动态回撤']=list(-np.maximum.accumulate(NAVcum) + NAVcum)
    (count_df['动态回撤']*amount_money).plot(figsize=(12,5))
    print('当前回撤幅度%0.02f'%((count_df['动态回撤'])[-1]/BP.Max_dd(dayNAV_s)[0]))
    print('当前回撤金额%s元'%(int((abs(count_df['动态回撤'][-1]*amount_money)))))
    print('当前距离最大回撤%s元'%(int(((BP.Max_dd(dayNAV_s)[0]-abs(count_df['动态回撤'][-1]))*amount_money))))
    #最大未创新高时间
    drawdownday=[]
    for i in range(len(count_df)):
        if count_df['动态回撤'][i] == 0:
            count=0
        elif count_df['动态回撤'][i] < 0:
            count=count+1
        drawdownday.append(count)
    count_df['回撤时间']= drawdownday
    fig, ax = plt.subplots(figsize = (12, 5))
    ax.bar(count_df['回撤时间'].index,count_df['回撤时间'])
    print('最长回撤时间为%s个交易日'%count_df['回撤时间'].max())
    
    #分年回报汇总
    print('每年收益率为：')
    print(dayNAV_s.resample('Y').sum())
    print('每年收益为：')
    print((dayNAV_s.resample('Y').sum()*amount_money).round(0))

    '''风险控制'''
    return_s = dayNAV_s[dayNAV_s!=0]
    riskcontrol_df = pd.DataFrame(return_s.cumsum(),columns=['profit_cum']).copy()
    # riskcontrol_df['low20'] = riskcontrol_df['profit_cum'].rolling(60).min()
    # riskcontrol_df['high20'] = riskcontrol_df['profit_cum'].rolling(20).max()
    riskcontrol_df['mid'] = riskcontrol_df['profit_cum'].rolling(60).mean()
    riskcontrol_df['up'] = riskcontrol_df['mid'] + riskcontrol_df['profit_cum'].rolling(60).std()*1
    riskcontrol_df['down'] = riskcontrol_df['mid'] - riskcontrol_df['profit_cum'].rolling(60).std()*1
    riskcontrol_df['2020':].plot(figsize=(12,5),grid=True)
    #择时
    riskcontrol_df['signal'] = 0
    signal = 0
    signal_list = []
    for i in range(len(riskcontrol_df)):
        if (riskcontrol_df.iloc[i]['profit_cum'] < riskcontrol_df.iloc[i]['down']) and (signal ==0):
            signal = -1
        elif (riskcontrol_df.iloc[i]['profit_cum'] > riskcontrol_df.iloc[i]['mid']) and (signal ==-1):
            signal = 0 
        signal_list.append(signal)
    riskcontrol_df['signal'] = signal_list
    # riskcontrol_df['2021']['signal'].plot(figsize=(12,5),grid=True) 
    
    riskcontrol_df['signal'] = riskcontrol_df['signal']-riskcontrol_df['signal'].shift(1)
    zeshi_df = pd.DataFrame(riskcontrol_df).copy()
    zeshi_df.iloc[0]['signal'] = 1
    zeshi_df.loc[zeshi_df['signal']==-1,'pos'] = 0.5
    zeshi_df.loc[zeshi_df['signal']==1,'pos'] = 1
    zeshi_df['pos'].fillna(method='ffill',inplace=True)
    zeshi_df['pos']=zeshi_df['pos'].shift(1)
    zeshi_df['profit_zs']=((zeshi_df['profit_cum']-zeshi_df['profit_cum'].shift(1))*zeshi_df['pos'])
    zeshi_df['profit_zs']['2020':].cumsum().plot(figsize=(12,5),grid=True)
    zeshi_df['2020':]['profit_cum'].plot(figsize=(12,5),grid=True)
       
    
    riskcontrol_df['std'] = riskcontrol_df['profit_cum'].rolling(30).std().rolling(5).mean()
    riskcontrol_df['2018']['profit_cum'].plot(figsize=(12,5),grid=True)
    riskcontrol_df['2016':]['std'].plot(figsize=(12,5),grid=True)
    #择时
    riskcontrol_df['signal'] = 0
    signal = 0
    signal_list = [0,]
    for i in range(1,len(riskcontrol_df)):
        if (riskcontrol_df.iloc[i]['std'] < riskcontrol_df.iloc[i-1]['std']) and (signal ==0):
            signal = -1
        elif (riskcontrol_df.iloc[i]['std'] > riskcontrol_df.iloc[i-1]['std']) and (signal ==-1):
            signal = 0 
        signal_list.append(signal)
    riskcontrol_df['signal'] = signal_list





