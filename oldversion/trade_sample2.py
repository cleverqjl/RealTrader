path = r'D:\CTATrade'
data_path = r'D:\futureData'
h5_dataPath = data_path + '/h5_Data_trade'
import sys
sys.path.append(path)
sys.path.append(data_path)
sys.path.append(r'D:\veighna_studio\Lib\site-packages')
import os
import multiprocessing
from multiprocessing import Pool
import threading
import pandas as pd
import time
from time import sleep
from datetime import datetime, time

import re
import rqdatac as rq
import futureDataManage 
import importlib
import talib
import talib as tb
from tradeFunction2 import *
import contract_info
from contract_info import price_tick_dict,tradeShare_dict,contract_multiplier_dict
# from strategy_set import *
from product_trade.sample.strategy_set import *
# 当列太多时不换行
pd.set_option('display.max_rows', 1000)
pd.set_option('expand_frame_repr', False)  
# 设置命令行输出时的列对齐功能
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)
'''api连接'''
from logging import INFO
from vnpy.event import EventEngine
from vnpy.trader.setting import SETTINGS
from vnpy.trader.engine import MainEngine
# from vnpy.gateway.ctp import CtpGateway
from vnpy_ctp.gateway import CtpGateway
# from vnpy.gateway.ctptest661 import CtptestGateway
# from vnpy.gateway.ctptest6319 import CtptestGateway
# from vnpy.app.cta_strategy.base import EVENT_CTA_LOG
# from vnpy.app.script_trader import ScriptEngine
from vnpy.trader.constant import Direction, Offset, OrderType, Interval
from vnpy.trader.object import (
                                OrderRequest,
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
SETTINGS["log.active"] = True
SETTINGS["log.level"] = INFO
SETTINGS["log.console"] = True
SETTINGS["log.file"] = True

'''基础设定'''
# api设定
ctp_setting = {
                "用户名": "106951",
                "密码": "12345678abc",
                "经纪商代码": "9999",
                "交易服务器": "180.168.146.187:10202",  #开盘时用
                "行情服务器": "180.168.146.187:10212",
            #    "交易服务器": "180.168.146.187:10202", #交易日，16：00～次日09：00；非交易日，16：00～次日15：00
            #    "行情服务器": "180.168.146.187:10212",
                "产品名称": "simnow_client_test",
                "授权编码": "0000000000000000",
                "产品信息": ""
                }
#策略设置
strategy_set_dict = strategy_set_dict0
time_interval = '10m'
method = 'capital_unit_sum'
capitalmode = 'atr'
c_rate = 1/10000
drop_days = 20
slippageMulti =1

'''多进程模块'''
# 定义回测主方法
def run_backtest(symbol_index,update):
    strategy_dict = strategy_set_dict[symbol_index]
    minpoint = price_tick_dict[re.sub("\d", "", symbol_index.split('.')[0])]
    contractMulti = contract_multiplier_dict[re.sub("\d", "", symbol_index.split('.')[0])]
    multiCompute = False
    limitValue = None
    df = pd.read_hdf(h5_dataPath+'/%s.h5' % '_'.join([symbol_index.split('.')[0],time_interval]), key='df')
    backtest_df = prepareHistoryData_multiStrategy( df,
                                                    strategy_dict,
                                                    minpoint,
                                                    c_rate ,
                                                    drop_days,
                                                    slippageMulti,
                                                    contractMulti,
                                                    capitalmode,
                                                    multiCompute,
                                                    limitValue,
                                                    method) 
    return {symbol_index:backtest_df}
def multi_wrapper(args):
    return run_backtest(*args)

# 定义计算信号方法
def run_computeSignal(i,portfolio_df_dict_i,thisBarSignal_list):
    try:
        #分策略回测
        for s in strategy_set_dict[i].keys():
            # s='wh3'
            use_strategyForTrade = strategy_set_dict[i][s]['strategy'][1]
            para = strategy_set_dict[i][s]['para']
            contractMulti = contract_multiplier_dict[re.sub("\d", "", i.split('.')[0])]
            capital_unit = strategy_set_dict[i][s]['capital_unit']
            long = strategy_set_dict[i][s]['long']
            short = strategy_set_dict[i][s]['short']
            signal_df,thisBarSignal  = use_strategyForTrade( data_df = portfolio_df_dict_i,
                                                                        strategyname = s,
                                                                        contractMulti = contractMulti,
                                                                        para = para,
                                                                        capital_unit = capital_unit,
                                                                        long = long,
                                                                        short = short)
            if thisBarSignal != '':
                thisBarSignal_list.append(i.split('.')[0] +'   '+ thisBarSignal)
        #策略头寸汇总
        signal_df= countNewPos(signal_df,strategy_set_dict[i],capital_unit,contractMulti,method)
        portfolio_df_dict[i] = signal_df
        # return ({i:signal_df},thisBarSignal_list)
    except Exception as e:
        print(str(e))
        log(str(e))
        log('【%s计算最新信号%s时出现错误】，程序中止，请检查】'%(i,s))
        send_dingding_and_raise_error('【%s计算最新信号%s时出现错误】，程序中止，请检查】'%(i,s)) 
def multi_computeSignal(args):
    return run_computeSignal(*args)

'''主程序'''
if __name__ == '__main__':
    # 连接api
    event_engine = EventEngine()
    main_engine = MainEngine(event_engine)
    main_engine.add_gateway(CtpGateway)
    # main_engine.add_gateway(CtptestGateway)
    main_engine.write_log("主引擎创建成功")
    log_engine = main_engine.get_engine("log")
    # event_engine.register(EVENT_CTA_LOG, log_engine.process_log_event)
    main_engine.write_log("注册日志事件监听")
    main_engine.write_log("连接CTP接口")
    # ====连接行情、交易服务器
    main_engine.connect(ctp_setting, "CTP")
    # main_engine.get_account("CTP.115333558")
    # main_engine.connect(ctp_setting, "CTPTEST")
    # ====订阅主力合约行情
    sleep(20)
    for r in range(5):
        try:
            for i in tradeShare_dict.keys():
                contract = main_engine.get_contract(tradeShare_dict[i])
                req = SubscribeRequest(symbol=contract.symbol,exchange=contract.exchange)
                main_engine.subscribe(req, contract.gateway_name)
            break
        except Exception as e:
            print(e)
            log(e)
            sleep(3)
            if r == 4:
                print('订阅主力合约行情,请检查api是否成功连接')
                log('订阅主力合约行情,请检查api是否成功连接')
                send_dingding_msg('订阅主力合约行情,请检查api是否成功连接')
                input('输入任意键退出')
                sys.exit(0)
    print('合约行情订阅成功')
    
    '''加载部分'''
    #产品名称
    product_name = 'sample'
    
    #今日日期
    today_str = datetime.now().strftime('%Y/%m/%d')
    
    #日志文件存放目录
    error_path = path+'/trade_report/%s/error_data'%product_name
    if os.path.exists(error_path) == False:
        os.mkdir(error_path)
    tradeData_path = path+'/trade_report/%s/trade_data'%product_name
    if os.path.exists(tradeData_path) == False:
        os.mkdir(tradeData_path)
    priceData_path = path+'/trade_report/%s/price_data'%product_name
    if os.path.exists(priceData_path) == False:
        os.mkdir(priceData_path)
    log_path = path+'/trade_report/%s/log'%product_name
    if os.path.exists(log_path) == False:
        os.mkdir(log_path)
        
    #初始log文件  
    log_file = open(log_path+'/%s.log'%datetime.now().strftime('%Y%m%d'), mode = 'a',encoding='utf-8')
    
    #检查主力合约映射
    checkMajorShare_df = checkShareIsMajor_viaInterest(tradeShare_dict)
    
    #历史数据回补
    # future_set_dict = future_set_dict
    indexShare_list = []
    # for exchange in future_set_dict.keys():
    #     for i in future_set_dict[exchange]:
    #         symbol_index = i+'99.%s'%exchange
    for i in strategy_set_dict.keys():
        symbol_index = i
        futureDataManage.updateDATA(symbol_index,time_interval='10m',startdate='20200101',use='trade')
        indexShare_list.append(symbol_index)
    
    #指数合约列表
    indexShare_list_RQ = [futureDataManage.format_to_RQshare(i) for i in indexShare_list]  
    
    #计算信号
    portfolio_df_dict = {}
    #设置多进程
    multiprocessing.freeze_support() 
    pool = Pool(3)
    #设置投资组合
    portfolio_list =  list(strategy_set_dict.keys())
    #运行多进程
    backtest_list = [(i,False) for i in portfolio_list]
    result_list = pool.imap_unordered(multi_wrapper, backtest_list)
    for result in result_list:
        print(list(result.keys())[0] + ' 回测完成')
        portfolio_df_dict[list(result.keys())[0]] = list(result.values())[0] 
    #关闭进程池，不再接受新的进程
    pool.close()
    #主进程阻塞等待子进程的退出 
    pool.join()
    # sys.exit()
    #统计当前策略持仓
    # rb_df = portfolio_df_dict['rb99.SHFE']
    portfolio_pos_df = countPortfolioPos(portfolio_df_dict,strategy_set_dict,tradeShare_dict)
    print('当前最新策略持仓（分策略）%s'%datetime.now().strftime('%Y%m%d %H:%M:%S'))
    print(portfolio_pos_df)
    log('当前最新策略持仓（分策略）%s'%datetime.now().strftime('%Y%m%d %H:%M:%S'))
    log(portfolio_pos_df)
    #最大市值风控
    limit_value_sp = 3000000*0.3
    limit_value_gz = 3000000*1
    # sys.exit()
    #计算当天单品种最大持仓的限制
    portfolio_pos_df_gz = portfolio_pos_df[['.CFFEX' in i for i in portfolio_pos_df['tradeShare']]]
    portfolio_pos_df_sp = portfolio_pos_df[['.CFFEX' not in i for i in portfolio_pos_df['tradeShare']]]
    max_holdShares_df_gz = computeMaxHold(portfolio_pos_df_gz,limit_value_gz,contract_multiplier_dict)
    max_holdShares_df_sp = computeMaxHold(portfolio_pos_df_sp,limit_value_sp,contract_multiplier_dict)
    max_holdShares_df = max_holdShares_df_gz.append(max_holdShares_df_sp)
    print('当前最新单品种持仓上限%s'%datetime.now().strftime('%Y%m%d %H:%M:%S'))
    print(max_holdShares_df)
    log('当前最新单品种持仓上限%s'%datetime.now().strftime('%Y%m%d %H:%M:%S'))
    log(max_holdShares_df)
    #过风控
    portfolio_pos_df,riskMsg_list_old = riskControl(portfolio_pos_df,max_holdShares_df)
    if riskMsg_list_old != []:
        for msg in riskMsg_list_old:
            send_dingding_msg(msg)
    #当前账户持仓，订阅持仓中没订阅过的合约行情
    account_pos_df =  getAcoountPos(main_engine)
    subcribe_list = subcribeMarketPlus(list(account_pos_df['tradeShare']),list(tradeShare_dict.values()),main_engine)
    #仓位检查
    #----判断当前是否在交易时段
    now_time = datetime.now()
    if datetime.now().strftime('%H:%M') < '09:00':
        run_time = now_time.replace(hour=9, minute=00, second=0, microsecond=0)
    elif (datetime.now().strftime('%H:%M') >= '10:15' and datetime.now().strftime('%H:%M') < '10:20'):#为防止10点16执行程序，判定成10：30而错过10：10的k先数据
        run_time = now_time.replace(hour=10, minute=20, second=0, microsecond=0)
    elif (datetime.now().strftime('%H:%M') >= '10:20' and datetime.now().strftime('%H:%M') < '10:30'):  
        run_time = now_time.replace(hour=10, minute=30, second=0, microsecond=0)
    elif (datetime.now().strftime('%H:%M') >= '11:30' and datetime.now().strftime('%H:%M') < '13:00'):
        run_time = now_time.replace(hour=13, minute=00, second=0, microsecond=0)
    elif (datetime.now().strftime('%H:%M') >= '15:00' and datetime.now().strftime('%H:%M') < '21:00'):
        run_time = now_time.replace(hour=21, minute=00, second=0, microsecond=0)  
    else: 
        run_time = ''
    #----如果在非交易时段，则等待至开盘
    ignore_share_df = pd.read_excel(path+'/product_trade/%s/交易设定.xlsx'%product_name)
    if run_time == '':   #如果在交易时段
        if (datetime.now().strftime('%H:%M') >= '09:00') and ((datetime.now().strftime('%H:%M') < '09:30')):
            correct_df,check_df,status_df,have_send_ding = checkAccountPos(main_engine,portfolio_pos_df,ignore_share_df,max_try_amount=5,needfix=False,ignore_list=[],gz='just') #maxtry设置为2次，防止发生出现错误一直开仓的情况
            correct_df,check_df,status_df,have_send_ding = checkAccountPos(main_engine,portfolio_pos_df,ignore_share_df,max_try_amount=5,needfix=True,ignore_list=[],gz='out')
            if have_send_ding == False: #如果持仓检查不需修复，则发送最新持仓到钉钉
                sendLastestPos(check_df)
        elif (datetime.now().strftime('%H:%M') >= '10:15') and ((datetime.now().strftime('%H:%M') < '10:30')):
            correct_df,check_df,status_df,have_send_ding = checkAccountPos(main_engine,portfolio_pos_df,ignore_share_df,max_try_amount=5,needfix=True,ignore_list=[],gz='just') #maxtry设置为2次，防止发生出现错误一直开仓的情况
            if have_send_ding == False: #如果持仓检查不需修复，则发送最新持仓到钉钉
                sendLastestPos(check_df)
            correct_df,check_df,status_df,have_send_ding = checkAccountPos(main_engine,portfolio_pos_df,ignore_share_df,max_try_amount=5,needfix=False,ignore_list=[],gz='out')     
        elif (datetime.now().strftime('%H:%M') >= '13:00') and ((datetime.now().strftime('%H:%M') < '13:30')):
            correct_df,check_df,status_df,have_send_ding = checkAccountPos(main_engine,portfolio_pos_df,ignore_share_df,max_try_amount=5,needfix=True,ignore_list=[],gz='just') #maxtry设置为2次，防止发生出现错误一直开仓的情况
            if have_send_ding == False: #如果持仓检查不需修复，则发送最新持仓到钉钉
                sendLastestPos(check_df)
            correct_df,check_df,status_df,have_send_ding = checkAccountPos(main_engine,portfolio_pos_df,ignore_share_df,max_try_amount=5,needfix=False,ignore_list=[],gz='out')     
        else:
            correct_df,check_df,status_df,have_send_ding = checkAccountPos(main_engine,portfolio_pos_df,ignore_share_df,max_try_amount=5,needfix=True,ignore_list=[],gz='in')
            if have_send_ding == False: #如果持仓检查不需修复，则发送最新持仓到钉钉
                sendLastestPos(check_df)
    elif run_time.strftime('%H:%M') in ['10:20','10:30']:
        correct_df,check_df,status_df,have_send_ding = checkAccountPos(main_engine,portfolio_pos_df,ignore_share_df,max_try_amount=5,needfix=True,ignore_list=[],gz='just') #maxtry设置为2次，防止发生出现错误一直开仓的情况
        if have_send_ding == False: #如果持仓检查不需修复，则发送最新持仓到钉钉
            sendLastestPos(check_df)
        correct_df,check_df,status_df,have_send_ding = checkAccountPos(main_engine,portfolio_pos_df,ignore_share_df,max_try_amount=5,needfix=False,ignore_list=[],gz='out')     
    else:                 #如果非交易时段
        correct_df,check_df,status_df,have_send_ding = checkAccountPos(main_engine,portfolio_pos_df,ignore_share_df,max_try_amount=5,needfix=False,ignore_list=[],gz='in')
        if correct_df.empty == False:
            print('当前为非交易时段，仓位需要修复，等待至开盘时间%s'%run_time.strftime('%Y/%m/%d %H:%M:%S'))
            log('当前为非交易时段，仓位需要修复，等待至开盘时间%s'%run_time.strftime('%Y/%m/%d %H:%M:%S'))
            send_dingding_msg('当前为非交易时段，仓位需要修复，等待至开盘时间%s'%run_time.strftime('%Y/%m/%d %H:%M:%S'))
            time.sleep(max(0, (run_time - datetime.now()).seconds))
            while True:  # 在靠近目标时间时
                if datetime.now() >= run_time:
                    break
            sleep(5) #开盘后等待5s，以防止开盘瞬间出现剧烈波动
            print('【开始修正仓位】')
            log('【开始修正仓位】')
            send_dingding_msg('【开始修正仓位】')
            if run_time.strftime('%H:%M') in ['09:00','21:00']:
                correct_df,check_df,status_df,have_send_ding = checkAccountPos(main_engine,portfolio_pos_df,ignore_share_df,max_try_amount=5,needfix=True,ignore_list=[],gz='out')
                correct_df,check_df,status_df,have_send_ding = checkAccountPos(main_engine,portfolio_pos_df,ignore_share_df,max_try_amount=5,needfix=False,ignore_list=[],gz='just')
            elif run_time.strftime('%H:%M') == '13:00':
                correct_df,check_df,status_df,have_send_ding = checkAccountPos(main_engine,portfolio_pos_df,ignore_share_df,max_try_amount=5,needfix=False,ignore_list=[],gz='out')
                correct_df,check_df,status_df,have_send_ding = checkAccountPos(main_engine,portfolio_pos_df,ignore_share_df,max_try_amount=5,needfix=True,ignore_list=[],gz='just')
        else:  #如果不需要修正持仓则发送最新持仓到钉钉
            sendLastestPos(check_df)
    
    # sys.exit(0)
    '''循环部分'''
    while True:
        try:
            #清空log信息
            renew_log()
            from tradeFunction import log_data
            # 开启log文件写入权限
            log_file = open(log_path+'/%s.log'%datetime.now().strftime('%Y%m%d'), mode = 'a',encoding='utf-8')
            if datetime.now().strftime('%H:%M:%S.%f') > '15:00:00.000000':
                #存储今日交易和持仓信息
                today_date = datetime.now().strftime('%Y%m%d')
                writer =  pd.ExcelWriter(path+'/trade_report/%s/trade_data/%s.xlsx'%(product_name,today_date))
                #----分策略持仓
                portfolio_pos_df.to_excel(writer,'策略持仓')
                #----账户持仓及统计
                check_df.to_excel(writer,'账户持仓')
                #----策略当日交易信号
                signal_count_df,trade_count_df = countPortfolioTrade(portfolio_df_dict,strategy_set_dict,tradedate=today_date)
                signal_count_df.to_excel(writer,'当日策略信号')
                trade_count_df.to_excel(writer,'当日策略仓位变动')
                #----账户当日交易记录
                trades_log_df = getAllTrades(main_engine)
                trades_log_df.to_excel(writer,'账户交易记录')
                writer.save()
                print('今日交易完毕，交易数据已保存')
                log('今日交易完毕，交易数据已保存')
                # save_log(log_data,log_file) 
                #存储当日的data_df信息
                writer =  pd.ExcelWriter(path+'/trade_report/%s/price_data/%s.xlsx'%(product_name,today_date))
                for i in portfolio_df_dict.keys():
                    portfolio_df_dict[i].tail(500).to_excel(writer,i)
                writer.save() 
                print('今日行情数据已保存')
                log('今日行情数据已保存')
                save_log(log_data,log_file) 
                #----再次检查主力合约
                checkMajorShare_df = checkShareIsMajor_viaInterest(tradeShare_dict)
                break
            # 计算下根k线时间
            print('------------------------------------------------------\n\n')
            run_time = sleep_until_run_time(time_interval=time_interval, ahead_time=5)
            # run_time = datetime(2023,9,14,11,0)  #用于测试
            second0 = time.time()
            # 重新引入tradeShare_dict（方便盘中进行更替主力合约）
            old_tradeShare_list = list(tradeShare_dict.values())
            importlib.reload(contract_info) 
            from contract_info import tradeShare_dict
            # 订阅没有订阅过的持仓合约行情
            account_pos_df =  getAcoountPos(main_engine)
            new_tradeShare_list = list(account_pos_df['tradeShare']) + list(tradeShare_dict.values())
            subcribe_list = subcribeMarketPlus(new_tradeShare_list,old_tradeShare_list,main_engine)
            # 重新读取过滤的合约
            ignore_share_df = pd.read_excel(path+'/product_trade/%s/交易设定.xlsx'%product_name)
            #如果是11:30或者15：00收盘的话，为防止溢出成交，等待10s后获取收盘时最后的数据
            if run_time.strftime('%H:%M') in ['11:30','15:00']:
                sleep(15) 
            else:
                sleep(0.5) #为防止整点获取的价格不稳定，做延迟1s的处理
            # 获取最新行情
            time_interval_int = int(re.sub("\D", "", time_interval))
            endtime = run_time.strftime('%Y-%m-%d %H:%M:%S')
            starttime = (run_time - timedelta(minutes=time_interval_int)).strftime('%Y-%m-%d %H:%M:%S')
            timestr1 = (run_time - timedelta(minutes=time_interval_int)).strftime('%H:%M')
            timestr2 = run_time.strftime('%H:%M')
            max_try_amount = 5
            second1 = time.time()
            for i in range(max_try_amount):
                try: 
                    new_barData = rq.get_price(indexShare_list_RQ, fields=['high','open','low','close'],start_date=today_str, end_date=today_str,frequency='1m',time_slice=(timestr1, timestr2))  
                    break
                except Exception as e:
                    print(e)
                    log(str(e))
                    sleep(1)
                    if i == (max_try_amount - 1):
                        log('【rq.get_price获取最新行情出现错误】，程序中止，请检查】')
                        send_dingding_and_raise_error('【rq.get_price获取最新行情出现错误】，程序中止，请检查】')
            print('获取最新行情完成')   
            second2 = time.time()
            print('用时%0.02fs'%(second2-second1))
            
            # 合并分钟行情
            second1 = time.time()
            for i in portfolio_df_dict.keys():
                # i = 'rb99.SHFE'
                symbol_RQ = futureDataManage.format_to_RQshare(i)
                try:
                    # print(datetime.now()) 
                    if symbol_RQ in list(new_barData.reset_index()['order_book_id']):
                        recent_data_df = new_barData.loc[(symbol_RQ,)]  #索引该品种的最新行情
                        recent_data_df['candle_begin_time'] = recent_data_df.index - timedelta(minutes=1)
                        recent_data_df = recent_data_df[recent_data_df['candle_begin_time'] >= starttime]
                        #对数据做截取，防止最后一根k线未走完
                        recent_data_df = recent_data_df[recent_data_df['candle_begin_time'] < run_time]
                        #重采样
                        time_interval_resample = '10T'  #此处暂时设置为10分钟
                        recent_data_df = recent_data_df.resample(rule=time_interval_resample, on='candle_begin_time', label='left', closed='left').agg(
                                        {'open': 'first',
                                         'high': 'max',
                                         'low': 'min',
                                         'close': 'last',
                                         })
                        recent_data_df.dropna(inplace=True)
                        recent_data_df['candle_begin_time'] = recent_data_df.index
                        #合并
                        portfolio_df_dict[i] = portfolio_df_dict[i].append(recent_data_df, ignore_index=True,sort=True)
                        portfolio_df_dict[i].drop_duplicates(subset=['candle_begin_time'], keep='first', inplace=True)
                        portfolio_df_dict[i].sort_values(by='candle_begin_time', inplace=True)  # 排序，理论上这步应该可以省略，加快速度
                        # portfolio_df_dict[i] = pd.DataFrame(portfolio_df_dict[i].iloc[-max_len:])  # 保持最大K线数量不会超过max_len个
                        portfolio_df_dict[i].reset_index(drop=True, inplace=True)
                    else:
                        pass
                except Exception as e:
                    log(str(e))
                    log('【合并%s分钟行情出现错误】，程序中止，请检查】'%i)
                    send_dingding_and_raise_error('【合并%s分钟行情出现错误】，程序中止，请检查】'%i)
            print('合并分钟行情完成')   
            second2 = time.time()
            print('用时%0.02fs'%(second2-second1))
            
            # 填补atr
            second1 = time.time()
            for i in portfolio_df_dict.keys():
                try:
                    recent_data_df_day = portfolio_df_dict[i].resample(rule='1d', on='candle_begin_time', label='left', closed='left').agg(
                                                                        {'open': 'first',
                                                                         'high': 'max',
                                                                         'low': 'min',
                                                                         'close': 'last',
                                                                         })
                    recent_data_df_day.dropna(subset=['open'], inplace=True)  # 去除一天都没有交易的周期
                    # recent_data_df_day = recent_data_df_day[recent_data_df_day['volume'] > 0]                     
                    recent_data_df_day['atr'] = tb.ATR(recent_data_df_day['high'],recent_data_df_day['low'],recent_data_df_day['close'],20)
                    recent_data_df_day['atr'] = recent_data_df_day['atr'].shift(1) 
                    portfolio_df_dict[i].loc[portfolio_df_dict[i].index[-1],'atr_day'] = recent_data_df_day.iloc[-1]['atr']
                   
                except Exception as e:
                    log(str(e))
                    log('【%s填补atr出现错误】，程序中止，请检查】'%i)
                    send_dingding_and_raise_error('【%s填补atr出现错误】，程序中止，请检查】'%i)
            print('填补atr完成')   
            second2 = time.time()
            print('用时%0.02fs'%(second2-second1)) 
            
            # 计算最新信号(多线程)
            second1 = time.time()
            thread_dict = {}
            thisBarSignal_list = []
            for i in portfolio_df_dict.keys():
                thread_dict[i] = threading.Thread(name=i,target= run_computeSignal,args=(i,portfolio_df_dict[i],thisBarSignal_list))
                # thread_dict[i].start()
            [thread_dict[i].start() for i in thread_dict.keys()]
            second2 = time.time()
            print('最新信号汇总完成')  
            print('用时%0.02fs'%(second2-second1)) 
            second1 = time.time()
            # for i in thread_dict.keys():
            #     thread_dict[i].join()
            [thread_dict[i].join() for i in thread_dict.keys()]
            second2 = time.time()
            print('阻塞线程')  
            print('用时%0.02fs'%(second2-second1))
   
            send_data = '【产生交易信号】\n'
            if thisBarSignal_list != []:
                for i in thisBarSignal_list:
                    print(i)
                    log(i)
                    send_data += i+'\n' 
                send_dingding_msg(send_data)
            # 最新策略持仓
            portfolio_pos_df_new = countPortfolioPos(portfolio_df_dict,strategy_set_dict,tradeShare_dict)
            # 过风控
            portfolio_pos_df_new, riskMsg_list = riskControl(portfolio_pos_df_new,max_holdShares_df)
            risk_send_Msg = [i for i in riskMsg_list if i not in riskMsg_list_old]  
            if risk_send_Msg != []:
                for msg in risk_send_Msg:
                    send_dingding_msg(msg)
            riskMsg_list_old = riskMsg_list
            print('当前最新策略持仓（分策略）%s'%datetime.now().strftime('%Y%m%d %H:%M:%S'))
            print(portfolio_pos_df_new)
            log(portfolio_pos_df_new)
            # 根据最新持仓计算交易信号
            # --股指
            account_pos_df =  getAcoountPos(main_engine)
            account_pos_df_gz = account_pos_df[['.CFFEX' in i for i in account_pos_df['tradeShare']]]
            portfolio_pos_df_new_gz = portfolio_pos_df_new[['.CFFEX' in i for i in portfolio_pos_df_new['tradeShare']]]
            trade_df_gz,change_df_gz = computeTradeSingal_gz(account_pos_df_gz,portfolio_pos_df_new_gz)
            # --商品
            portfolio_pos_df_sp = portfolio_pos_df[['.CFFEX' not in i for i in portfolio_pos_df['tradeShare']]]
            portfolio_pos_df_new_sp = portfolio_pos_df_new[['.CFFEX' not in i for i in portfolio_pos_df_new['tradeShare']]]
            trade_df_sp,change_df_sp = computeTradeSingal(portfolio_pos_df_sp,portfolio_pos_df_new_sp)
            # --合并
            trade_df = trade_df_gz.append(trade_df_sp)
            change_df = change_df_gz.append(change_df_sp)
            # 过滤掉忽略的合约
            if trade_df.empty == False:
                trade_df = pd.DataFrame(trade_df[[i not in list(ignore_share_df['share']) for i in trade_df['share']]]).copy()
            second3 = time.time()
            print('总用时%0.02fs'%(second3-second0)) 
            # sys.exit()
            
            # 根据最新信号进行交易
            if trade_df.empty == False:
                #判断当前是否在交易时段
                now_time = datetime.now()
                # if (datetime.now().strftime('%H:%M') >= '10:15' and datetime.now().strftime('%H:%M') < '10:30'):
                #     run_time = now_time.replace(hour=10, minute=30, second=0, microsecond=0)
                if (datetime.now().strftime('%H:%M') == '11:30'):
                    run_time = now_time.replace(hour=13, minute=00, second=0, microsecond=0) 
                elif (datetime.now().strftime('%H:%M') == '15:00'):
                    print('今日已收盘，当前有信号产生，请于下个交易日开盘回补仓位')  
                    log('今日已收盘，当前有信号产生，请于下个交易日开盘回补仓位')
                    send_dingding_msg('今日已收盘，当前有信号产生，请于下个交易日开盘回补仓位')
                    #保存log文件
                    save_log(log_data,log_file)
                    # 替换portfolio_pos_df
                    portfolio_pos_df = portfolio_pos_df_new
                    continue
                else: 
                    run_time = ''
                #如果在非交易时段则等待至开盘
                if  run_time == '':   #如果在交易时段
                    pass
                else:                 #如果非交易时段
                    print('当前为非交易时段，有交易信号发生，将等待至开盘时间%s'%run_time.strftime('%Y/%m/%d %H:%M:%S'))
                    log('当前为非交易时段，有交易信号发生，将等待至开盘时间%s'%run_time.strftime('%Y/%m/%d %H:%M:%S'))
                    send_dingding_msg('当前为非交易时段，有交易信号发生，将等待至开盘时间%s'%run_time.strftime('%Y/%m/%d %H:%M:%S'))
                    time.sleep(max(0, (run_time - datetime.now()).seconds))
                    while True:  # 在靠近目标时间时
                        if datetime.now() >= run_time:
                            break
                    sleep(3) #开盘后等待3s，以防止开盘瞬间出现剧烈波动   
                #区分商品和股指
                if (datetime.now().strftime('%H:%M') >= '10:15' and datetime.now().strftime('%H:%M') < '10:30'):
                    trade_df = trade_df_gz
                elif (datetime.now().strftime('%H:%M') >= '09:00' and datetime.now().strftime('%H:%M') < '09:30'):
                    trade_df = trade_df_sp
                send_dingding_msg('当前K线发生交易，K线起始时间为%s'%starttime)
                print('当前K线发生交易，K线起始时间为%s'%starttime)
                print(trade_df)
                log('当前K线发生交易，K线起始时间为%s'%starttime)
                log(trade_df)
                # 转换委托单（上期所上能源所开平仓）
                pos_df = getAcoountPos(main_engine)
                convert_trade_df = convertOrderClass(trade_df,pos_df)
                
                #撤掉所有委托单（如有，防止出现自成交）
                cancelAllOrders(main_engine)
                # 报送委托单
                orderid_list = postOrder(main_engine,convert_trade_df)
                # 检查是否成交
                sleep(5)  #防止成交结果未更新
                if orderid_list != []:
                    status_df = checkOderStatus(main_engine,orderid_list)
                # 成交后发送最新持仓到钉钉(暂不发送)
                sleep(10)  #防止持仓未更新
                pos_df = getAcoountPos(main_engine)
                # --股指
                account_pos_df_gz = pos_df[['.CFFEX' in i for i in pos_df['tradeShare']]]
                portfolio_pos_df_new_gz = portfolio_pos_df_new[['.CFFEX' in i for i in portfolio_pos_df_new['tradeShare']]]
                trade_df_gz,change_df_gz = computeTradeSingal_gz(account_pos_df_gz,portfolio_pos_df_new_gz)
                # --商品
                portfolio_pos_df_sp = pos_df[['.CFFEX' not in i for i in pos_df['tradeShare']]]
                portfolio_pos_df_new_sp = portfolio_pos_df_new[['.CFFEX' not in i for i in portfolio_pos_df_new['tradeShare']]]
                trade_df_sp,change_df_sp = computeTradeSingal(portfolio_pos_df_sp,portfolio_pos_df_new_sp)
                # --合并
                trade_df = trade_df_gz.append(trade_df_sp)
                change_df = change_df_gz.append(change_df_sp)
                sendLastestPos(change_df)
            else:
                print('当前K线无交易信号')
            # 检查策略持仓与实际持仓
            sleep(5) #防止持仓未更新    
            if  (datetime.now().strftime('%H:%M') >= '09:00' and datetime.now().strftime('%H:%M') < '09:30'):
                correct_df,check_df,status_df,have_send_ding = checkAccountPos(main_engine,portfolio_pos_df_new,ignore_share_df,max_try_amount=5,needfix=True,gz='out')
            elif (datetime.now().strftime('%H:%M') >= '13:00' and datetime.now().strftime('%H:%M') < '13:30'):
                correct_df,check_df,status_df,have_send_ding = checkAccountPos(main_engine,portfolio_pos_df_new,ignore_share_df,max_try_amount=5,needfix=True,gz='just')
            else:
                correct_df,check_df,status_df,have_send_ding = checkAccountPos(main_engine,portfolio_pos_df_new,ignore_share_df,max_try_amount=5,needfix=True,gz='in')
            # 替换portfolio_pos_df
            portfolio_pos_df = portfolio_pos_df_new
            # 保存log文件
            save_log(log_data,log_file)
           
        except Exception as e:
            print(e)   
            log('***ERROR: '+str(e))
            save_log(log_data,log_file)
            send_dingding_msg('程序出现错误，已终止运行，请检查代码')
            # 存放前一根k线的data_df作为出错时debug用(由于单线程工作占用时间过长，暂不每根k线都保存，只在出错时保存)
            writer = pd.ExcelWriter(error_path+'/data_df_%s.xlsx'%datetime.now().strftime('%Y%m%d_%H%M'))
            for i in portfolio_df_dict.keys():
                data_df = portfolio_df_dict[i]
                data_df.index.name = str(datetime.now())
                data_df.to_excel(writer,i)
            writer.save()
            print('data_df文件已保存，请检查错误信息')
            new_barData.to_excel(error_path+'/new_barData_%s.xlsx'%datetime.now().strftime('%Y%m%d_%H%M'))
            print('new_barData文件已保存，请检查错误信息')
            break
    input('程序结束，输入任意键退出')
        

















