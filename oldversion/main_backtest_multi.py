path = r'D:\CTATrade'
data_path = r'D:\futureData'
import sys
sys.path.append(path)
sys.path.append(data_path)
sys.path.append(r'D:\veighna_studio\Lib\site-packages')
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
from backtest import backtestStrategies
import BacktestParameter as BP
import matplotlib.pyplot as plt
from tradeConfig import rqdata_info
from contract_info import price_tick_dict,contract_multiplier_dict
from portfolio_set import *
from strategy_set import *
# from product_trade.huajin.strategy_set import *
import rqdatac as rq
import mathStatistics as MS 
import random
# rq.init(rqdata_info['user'], rqdata_info['password'])
plt.rcParams['font.sans-serif']=['SimHei'] #用来正常显示中文标签
plt.rcParams['axes.unicode_minus']=False #用来正常显示负号
'''定义策略回测方法'''
def BacktestPnL_multi(  symbol,
                        strategy_dict,
                        time_interval = '10m',
                        drop_days = 21,
                        slippageMulti = 1,
                        dataUpdate = True,
                        capitalmode = 'atr',
                        multiCompute = False,
                        limitValue = None,
                        starttime = datetime(2017,1,1),
                        method='contract_sum',
                        c_rate = 1/10000,
                        ): 
    
    '''基础信息 ''' 
    # symbol = 'rb99.SHFE'
    #定义数据路径
    h5_dataPath = data_path +'/h5_Data' 
    #合约乘数
    # contractMulti = rq.instruments(symbol.split('.')[0].upper()).contract_multiplier
    contractMulti = contract_multiplier_dict[re.sub("\d", "", symbol.split('.')[0])]
    #最小波动/手续费
    minpoint = price_tick_dict[re.sub("\d", "", symbol.split('.')[0])]
    c_rate = c_rate
    #今日日期  
    today_date = (datetime.now()).date().strftime('%Y-%m-%d')  
    '''数据回补'''
    if dataUpdate == True:
        futureDataManage.updateDATA(symbol,time_interval,dayStartTime='09:00',dayEndTime='15:00',get_beforeData=False,remake_h5=False)
    '''回测策略'''
    print(symbol+' 开始回测')
    df = pd.read_hdf(h5_dataPath+'/%s.h5' % '_'.join([symbol.split('.')[0],time_interval]), key='df')
    df = df[df['candle_begin_time']>starttime]
    df = backtestStrategies( df,
                             strategy_dict,
                             minpoint,
                             c_rate ,
                             drop_days,
                             slippageMulti,
                             contractMulti,
                             capitalmode,
                             multiCompute=multiCompute,
                             limitValue=limitValue,
                             method=method
                              )
    print(symbol+' 回测完成')
    '''存储csv'''
    df.to_csv(path+'/backtest_report/%s.csv'%'_'.join([symbol.split('.')[0],today_date,time_interval]))
    return df
'''回测策略'''
strategy_set_dict = {}
strategy_set_dict.update(strategy_set_dict4)
#定义回测主方法
def run_backtest(strategyname):
    df = BacktestPnL_multi( symbol = strategyname,
                            strategy_dict = strategy_set_dict[strategyname],
                            time_interval = '10m',
                            drop_days = 21,
                            slippageMulti = 1.5,
                            dataUpdate = False,
                            capitalmode = 'atr',
                            multiCompute = False,
                            limitValue = None,
                            starttime = datetime(2012,1,1),
                            method='capital_unit_sum',
                            c_rate = 1/10000,
                            )
    return {strategyname:df}

if __name__ == '__main__':
    #回补数据
    # dataUpdate = True
    dataUpdate = False
    if dataUpdate == True:
        for symbol in strategy_set_dict.keys():
            futureDataManage.updateDATA(symbol,time_interval='10m',dayStartTime='09:00',dayEndTime='15:00',get_beforeData=False,remake_h5=False)
    #设置多进程
    second1 = time.time()
    multiprocessing.freeze_support() 
    pool = Pool(12)  #对应逻辑处理器数量
    #设置投资组合
    portfolio_list =  list(strategy_set_dict.keys())
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
    print('全部回测完成')
    second2 = time.time()
    print('回测用时%0.02fs'%(second2-second1))
    # sys.exit(0)
    
    '''组合计算'''
    #定义投资组合
    portfolio_list = [portfolio_backtest_dict[i] for i in strategy_set_dict.keys()]
    # portfilio_df.cumsum().plot(figsize=(12,7),grid=True)
    # a=portfolio_backtest_dict['rb99.SHFE'].tail(100)['contract_num_drt'].resample('1d').sum().cumsum().plot(figsize=(12,7),grid=True)
    #     portfolio_backtest_dict[i]['profit_cum'].plot(figsize=(12,7),grid=True)
    # aa = portfolio_backtest_dict['IF99.CFFEX'].tail(1000)
    # portfolio_backtest_dict['IF99.CFFEX']['contract_num_drt'].plot()
    # portfolio_backtest_dict['IF99.CFFEX']['close'].plot(figsize=(12,5),grid=True)
    # portfolio_backtest_dict['CF99.CZCE']['net_profit'].resample('1d').sum()55
    #组合盈亏
    portfilio_df = pd.concat([i['net_profit'].resample('1d').sum() for i in portfolio_list],axis=1,join='outer')
    # portfilio_df['2020':].apply(lambda x:x.sum(),axis=1).cumsum().plot(figsize=(12,5),grid=True)
    sys.exit(0)
    print('总盈利：%s'%list(portfilio_df.apply(lambda x:x.sum(),axis=1).cumsum())[-1])
    for i in range(2012,2023):
        portfilio_df[str(i)].apply(lambda x:x.sum(),axis=1).cumsum().plot(figsize=(32,7),grid=True)
    #近五日盈亏
    portfilio_df.apply(lambda x:x.sum(),axis=1).tail(5)
    #最新交易日盈亏详情
    portfilio_df.columns = strategy_set_dict.keys()
    portfilio_df.iloc[-1]
        
    #加权净值统计 
    def countnum(data):
        pick_s = data[np.isnan(data) == False]
        # num = pd.Series([portfolio_set_dict[i]['weight'] for i in pick_s.index]).sum()
        num = len(pick_s)
        return num
    weighting_df = pd.DataFrame(portfilio_df.apply(lambda x:x.sum(),axis=1),columns=['profit'])
    weighting_df['weightsum']= [countnum(portfilio_df.iloc[i]) for i in range(len(portfilio_df))]
    weighting_df['profit_w'] = weighting_df['profit']/weighting_df['weightsum']
    weighting_df['profit_w_sum'] = (weighting_df['profit']/weighting_df['weightsum']).cumsum()
    (weighting_df['profit_w_sum']*weighting_df['weightsum'][-1])['2012':].plot(figsize=(12,5),grid=True)


    #分品种统计
    portfilio_df.columns = strategy_set_dict.keys()
    portfilio_df.iloc[-1].sum()
    portfilio_df.iloc[-8:-1].sum().sort_values()
    n=28
    print(portfilio_df.columns[n])
    portfilio_df.iloc[:,n].cumsum().plot(figsize=(10,4),grid=True)
     
    #组合相关性
    portfolio_dict_list = [portfolio_set_dict2,portfolio_set_dict3,portfolio_set_dict4,portfolio_set_dict5]
    corr_list = []
    for i in portfolio_dict_list:
        return_list = [portfolio_backtest_dict[i] for i in i.keys()]
        return_df = pd.concat([i['net_profit'].resample('1d').sum() for i in return_list],axis=1,join='outer')
        return_s = return_df.apply(lambda x:x.sum(),axis=1)
        corr_list.append(return_s)
    corr_df = pd.concat(corr_list,axis=1)
    print(corr_df.corr())
    '''组合策略评价参数'''
    amount_money = 10000000
    # dayNAV_s=(portfilio_df['2012':].apply(lambda x:x.sum(),axis=1)).resample('1d').sum()/amount_money
    dayNAV_s = (weighting_df['profit_w_sum'])*weighting_df['weightsum'][-1]/amount_money #加权
    dayNAV_s = (dayNAV_s-dayNAV_s.shift(1)).fillna(0)
    # (dayNAV_s.cumsum()+1).plot(figsize=(12,5),grid=True,color='r')
    # (dayNAV_s.resample('m').sum()+1).cumprod().plot(figsize=(12,5),grid=True)
    # dayNAV_s.to_excel(r'D:\CTATrade\nav.xlsx')
    #周净值
    df0=(dayNAV_s['2012':].resample('d').sum().cumsum()+1).plot(figsize=(12,5),grid=True)
    # (dayNAV_s['20191010':].resample('W-FRI').sum().cumsum()+1).to_excel(r'D:\CTATrade\nav.xlsx')
    week_df = (dayNAV_s['20191025':].resample('W-FRI').sum().cumsum()+1)
    ws = (week_df-week_df.iloc[0])
    ws-ws.shift(1)
    #单利
    (dayNAV_s.resample('d').sum().cumsum()+1).plot(figsize=(10,4),grid=True,c='r')
    #复利
    (dayNAV_s.resample('w').sum()+1).cumprod().plot(figsize=(10,4),grid=True)
    #组合占用资金
    portfilio_df_capital = pd.concat([i['capital_use'].resample('1d').max() for i in portfolio_list],axis=1,join='outer')
    (portfilio_df_capital['2016':].apply(lambda x:x.sum(),axis=1)/amount_money).plot(figsize=(12,3),grid=True)
    print('保证金最高占用：%s'%portfilio_df_capital.apply(lambda x:x.sum(),axis=1).max())
    print('分策略相关性矩阵：')
    print(portfilio_df.corr())
    
    # dayNAV_s.to_excel(r'D:\CTATrade\nav.xlsx')
    #df0=(dayNAV_s.resample('m').sum()*2+1).cumprod().plot()
    print('AROR: %s'%BP.AROR(dayNAV_s,freq='allday'))
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
    (count_df['动态回撤']*1).plot(figsize=(12,3),grid=True)
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

    #统计分布
    # 统计分布
    # --日度
    return_s = dayNAV_s
    return_s = return_s[return_s!=0]
    return_s.sort_values()
    weights = np.ones_like(return_s) / len(return_s)
    return_s.hist(bins=20,figsize=(12,4),weights=weights)
    len(return_s[(return_s>-10000) & (return_s<-5000)])/len(return_s)
    MS.prob(return_s,0)
    MS.distrib(return_s)
    return_s.mean()
    -return_s[(return_s>0)].mean()/return_s[(return_s<0)].mean()
    # --周度
    return_s_week = dayNAV_s.resample('w').sum()
    return_s_week.sort_values()
    return_s_week = return_s_week[return_s_week!=0]
    weights_week = np.ones_like(return_s_week) / len(return_s_week)
    return_s_week.hist(bins=20,figsize=(12,4),weights=weights_week)
    MS.ParameterNormDistrib(return_s_week,20)
    MS.prob(return_s_week,0)
    MS.distrib(return_s_week)
    len(return_s_week[(return_s_week>0)])/len(return_s_week)
    return_s_week.mean()
    -return_s_week[(return_s_week>0)].mean()/return_s_week[(return_s_week<0)].mean()
    # --月度
    return_s_month = dayNAV_s.resample('m').sum()
    return_s_month.sort_values()
    return_s_month = return_s_month[return_s_month!=0]
    weights_month = np.ones_like(return_s_month) / len(return_s_month)
    return_s_month.hist(bins=10,figsize=(12,4),weights=weights_month)
    MS.ParameterNormDistrib(return_s_month,6)
    MS.prob(return_s_month,0)
    MS.distrib(return_s_month)
    len(return_s_month[(return_s_month>0)])/len(return_s_month)
    return_s_month.mean()
    -return_s_month[(return_s_month>0)].mean()/return_s_month[(return_s_month<0)].mean()
    '''蒙特卡洛模拟'''
    def montecarlo_simulate():
        profit_list=[]
        for i in range(252*3):  #感受多少天
            random_int = random.randint(0,len(return_s)-1)
            cta_profit = return_s.iloc[random_int]
            profit_list.append(cta_profit)
        simulate_s = pd.Series(profit_list)    
        simulate_s.cumsum().plot(figsize=(12,7))
        return simulate_s
    profit_list = []
    for i in range(0,1000): #模拟多少次
        simulate_s = montecarlo_simulate()
        profit_list.append(simulate_s.sum())
    profit_s = pd.Series(profit_list) 
    len(profit_s[(profit_s>0.60)])/len(profit_s)  
    len(profit_s[(profit_s>200000) & (profit_s<1000000)])/len(profit_s)  
    u=profit_s.mean()
    sigma=profit_s.std()
    print(u-1*sigma,u+1*sigma)
    weights = np.ones_like(profit_s) / len(profit_s)
    profit_s.hist(bins=20,figsize=(12,7),weights=weights)
    MS.prob(profit_s,0)    

    '''风险控制'''
    riskcontrol_df = pd.DataFrame(return_s.cumsum(),columns=['profit_cum'])
    # riskcontrol_df['low20'] = riskcontrol_df['profit_cum'].rolling(60).min()
    # riskcontrol_df['high20'] = riskcontrol_df['profit_cum'].rolling(20).max()
    riskcontrol_df['mid'] = riskcontrol_df['profit_cum'].rolling(60).mean()
    riskcontrol_df['up'] = riskcontrol_df['mid'] + riskcontrol_df['profit_cum'].rolling(60).std()*2
    riskcontrol_df['down'] = riskcontrol_df['mid'] - riskcontrol_df['profit_cum'].rolling(60).std()*2
    
    riskcontrol_df['std'] = riskcontrol_df['profit_cum'].rolling(60).std().rolling(5).mean()
    riskcontrol_df['2018']['profit_cum'].plot(figsize=(12,5),grid=True)
    riskcontrol_df['2018']['std'].plot(figsize=(12,5),grid=True)




