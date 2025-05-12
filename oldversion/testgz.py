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
from product_trade.huajin.strategy_set import *
# 当列太多时不换行
pd.set_option('display.max_rows', 1000)
pd.set_option('expand_frame_repr', False)  
# 设置命令行输出时的列对齐功能
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)
'''api连接'''
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
# main_engine.connect(ctp_setting, "CTPTEST")
 
# ====查询账户资金
main_engine.get_account("CTP.106951")
# ====订阅合约行情
contract = main_engine.get_contract('MA205.CZCE')
contract = main_engine.get_contract('si2308.GFEX')
contract = main_engine.get_contract('IF2309.CFFEX')
req = SubscribeRequest(symbol=contract.symbol,exchange=contract.exchange)
main_engine.subscribe(req, contract.gateway_name)
 
main_engine.get_all_positions()

'''股指相关交易'''
#统计股指实盘账户持仓
account_pos_df =  getAcoountPos(main_engine)
account_pos_df_gz = account_pos_df[['.CFFEX' in i for i in account_pos_df['tradeShare']]]
portfolio_pos_df = account_pos_df_gz
#根据下单指令和持仓情况判定下单方式
portfolio_pos_df_new_gz = pd.DataFrame()
portfolio_pos_df_new_gz['tradeShare'] = ['IF2309.CFFEX'] 
portfolio_pos_df_new_gz['contract_num_drt'] = [-1]
portfolio_pos_df_new = portfolio_pos_df_new_gz
#计算交易指令
trade_df,change_df = computeTradeSingal_gz(account_pos_df_gz,portfolio_pos_df)

 data_old = account_pos_df_gz.groupby(by='tradeShare')[['contract_num_drt']].sum()
 if data_old.empty == True:
     data_old = pd.DataFrame(columns=['contract_num_drt'])
     data_old.index_name = 'tradeShare'
 data_new = portfolio_pos_df.groupby(by='tradehare')[['contract_num_drt']].sum()
 change_df = pd.concat([data_old['contract_num_drt'],data_new['contract_num_drt']],axis=1,join='outer',sort=False).fillna(0)
 change_df.columns = ['old_pos','new_pos']
 change_df['change_pos'] = change_df['new_pos']-change_df['old_pos']


# 转换委托单（上期所上能源所开平仓）
account_pos_df =  getAcoountPos(main_engine)
account_pos_df_gz = account_pos_df[['.CFFEX' in i for i in account_pos_df['tradeShare']]]
convert_trade_df = convertOrderClass(trade_df,account_pos_df_gz)
orderid_list = postOrder(main_engine,convert_trade_df)

 
 
 
 
 
 
 
 
 
 
 
 
 
 
 