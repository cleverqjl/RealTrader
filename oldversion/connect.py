
import sys
sys.path.append(r'D:\CTATrade\vnstudio\Lib\site-packages')
import multiprocessing
from time import sleep
from datetime import datetime, time
from logging import INFO
from vnpy.event import EventEngine
from vnpy.trader.setting import SETTINGS
from vnpy.trader.engine import MainEngine
from vnpy.gateway.ctp import CtpGateway
from vnpy.gateway.ctptest6319 import CtptestGateway
# from vnpy.app.cta_strategy import CtaStrategyApp
from vnpy.app.cta_strategy.base import EVENT_CTA_LOG
from vnpy.app.script_trader import ScriptEngine
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
    


event_engine = EventEngine()
main_engine = MainEngine(event_engine)
main_engine.add_gateway(CtptestGateway)
#cta_engine = main_engine.add_app(CtaStrategyApp)
#main_engine.write_log("主引擎创建成功")
#log_engine = main_engine.get_engine("log")
#event_engine.register(EVENT_CTA_LOG, log_engine.process_log_event)
#main_engine.write_log("注册日志事件监听")
#main_engine.write_log("连接CTP接口")

# ====连接行情、交易服务器
main_engine.connect(ctp_setting, "CTPTEST")
# ====断开连接行情、交易服务器
main_engine.close()
# ====查询账户资金
main_engine.get_account("CTPTEST.106951")
# ====订阅合约行情
contract = main_engine.get_contract('rb2110.SHFE')
req = SubscribeRequest(symbol=contract.symbol,exchange=contract.exchange)
main_engine.subscribe(req, contract.gateway_name)
# ====查询合约最新价格
main_engine.get_tick('rb2110.SHFE')
# ====发送委托单
req = OrderRequest(
                    symbol=contract.symbol,
                    exchange=contract.exchange,
                    direction=Direction.LONG,
                    type=OrderType.LIMIT,
                    volume=1,
                    price=5200,
                    offset=Offset.OPEN
                    )
vt_orderid = main_engine.send_order(req, contract.gateway_name)
# ====查询账户持仓
main_engine.get_all_positions()
# ====查询委托单的成交情况
main_engine.get_order(vt_orderid)









