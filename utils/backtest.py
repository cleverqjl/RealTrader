import pandas as pd
import numpy as np
import pandas_ta as ta


# 由持仓信号生成持仓手数
def pos_to_contract_num(data_df:pd.DataFrame,minpoint:float,slippageMulti:float,capitalmode:str,capital_unit:float,contractMulti:float,limitValue:float=None,min_contract_num:int=1,is_round:bool=True)->pd.DataFrame:
    """  
    使用说明：

    参数说明：
        data_1d: pd.DataFrame
            必须包含以下字段：'open', 'close', 'high','low', 'pos'，分别为开盘价、收盘价、最高价、最低价、持仓信号。
            pos列：1为多头，-1为空头，0为空仓。
        minpoint: float
            最小价格变动单位（如最小跳动点）。
        slippageMulti: float
            滑点倍数，实际滑点= minpoint * slippageMulti。

    返回值：
        函数直接在data_1d内增加以下字段：
            'next_open'：下一根K线开盘价
            'open_pos_price'：开仓价格
            'close_pos_price'：平仓价格
            'contract_num'：持仓手数
            'profit'：每根K线盈亏
            'fee'：手续费
            'net_profit'：净盈利
            'profit_cum'：累计净盈利
    """
    # -计算下一根k线开盘价（计算平仓价格用）
    data_df['next_open'] = data_df['open'].shift(-1)  # 下根K线的开盘价
    data_df['next_open'].fillna(value=data_df['close'], inplace=True)
    # -找出开仓的k线
    condition1 = data_df['pos'] != 0  # 当前周期不为空仓
    condition2 = data_df['pos'] != data_df['pos'].shift(1)  # 当前周期和上个周期持仓方向不一样。
    open_pos_condition = condition1 & condition2
    # -找出平仓的k线
    condition1 = data_df['pos'] != 0  # 当前周期不为空仓
    condition2 = data_df['pos'] != data_df['pos'].shift(-1).fillna(method='ffill')  # 当前周期和下个周期持仓方向不一样。
    close_pos_condition = condition1 & condition2    
    # -滑点
    slippage = minpoint * slippageMulti
    # -交易价格，默认按照开盘价成交
    data_df['trade_price'] = data_df['open'] + slippage*(data_df['pos'] - data_df['pos'].shift(1))
    # -开仓手数计算
    # --atr模式
    if capitalmode in ['atr_minute','atr_day']:
        # 计算ATR
        if capitalmode == 'atr_minute':
            data_df['atr'] = cal_atr(data_df, 1000)
        elif capitalmode == 'atr_day':
            day_df = minute_to_day_df(data_df)
            day_df['atr'] = cal_atr(day_df, 20)
            day_df = day_df.set_index('candle_begin_time')
            day_df.index = pd.to_datetime(day_df.index)
            data_df.loc[open_pos_condition, 'atr'] = data_df[open_pos_condition]['candle_begin_time'].apply(
                lambda x: day_df['atr'].shift(1).get(pd.to_datetime(x).strftime('%Y-%m-%d'), 0)
            )
        # 计算合约数量
        if limitValue:
            data_df.loc[open_pos_condition, 'contract_num1'] = capital_unit / data_df['atr'] / contractMulti
            data_df.loc[open_pos_condition, 'contract_num2'] = limitValue / data_df['trade_price'] / contractMulti
            data_df.loc[open_pos_condition, 'contract_num'] = data_df.loc[open_pos_condition, ['contract_num1', 'contract_num2']].min(axis=1)
            data_df.drop(['contract_num1', 'contract_num2'], axis=1, inplace=True)
        else:
            data_df.loc[open_pos_condition, 'contract_num'] = capital_unit / data_df['atr'] / contractMulti
    # 固定手数模式
    elif capitalmode == 'contractnum':
        data_df.loc[open_pos_condition, 'contract_num'] = capital_unit
    # 市值模式
    elif capitalmode == 'marketValue':
        data_df.loc[open_pos_condition, 'contract_num'] = capital_unit /  data_df['trade_price'] / contractMulti
    # 头寸取整
    contract_num_list = data_df.loc[open_pos_condition, 'contract_num']
    if is_round:
        contract_num_list = [max(i,min_contract_num) for i in round(contract_num_list)]
    else:
        contract_num_list = [max(i,min_contract_num) for i in contract_num_list]
    data_df.loc[open_pos_condition, 'contract_num'] = contract_num_list
    data_df['contract_num'].fillna(method='ffill',inplace=True)
    data_df.loc[data_df['pos']==0,['contract_num']] = 0
    data_df['contract_num'].fillna(0,inplace=True)
    return data_df


# 由持仓手数计算盈亏
def contract_num_to_profit(data_df: pd.DataFrame, contractMulti: float, minpoint:float, slippageMulti:float, cost: float, cost_type='percent') -> pd.DataFrame:
    """
    计算量化交易策略的盈亏，将持仓盈亏与交易盈亏分开计算，适用于多种交易场景。
    
    本函数采用向量化计算方式，支持加仓、减仓、换仓等复杂交易行为，并可根据不同计费方式计算手续费。
    
    计算方法：
    1. 持仓盈亏(Holding PnL)：已有仓位在当日的市值变动
       - 计算公式：前一日持仓手数 × (当日收盘价 - 前日收盘价) × 合约乘数 × 持仓方向
       
    2. 交易盈亏(Trading PnL)：当日新开仓位的盈亏
       - 计算公式：新增持仓手数 × (当日收盘价 - 当日交易价格) × 合约乘数 × 持仓方向
    
    3. 总盈亏 = 持仓盈亏 + 交易盈亏
    
    4. 净盈亏 = 总盈亏 - 手续费

    参数说明：
        data_df: pd.DataFrame
            数据框，必须包含以下字段：
            - 'open': 开盘价
            - 'close': 收盘价
            - 'contract_num': 每根K线的持仓手数（正值表示多头，负值表示空头，0表示空仓）
            - 'pos': 持仓方向（1=多头，-1=空头，0=空仓）
            
        contractMulti: float
            合约乘数，用于将价格变动转换为实际盈亏金额
            
        minpoint: float
            最小变动价位，用于计算滑点
            
        slippageMulti: float
            滑点倍数，实际滑点 = minpoint × slippageMulti
            
        cost: float
            交易成本，可以是：
            - 手续费率（当cost_tpye='percent'时，表示成交金额的百分比）
            - 固定手续费（当cost_tpye='amount'时，表示每手固定金额）
            
        cost_tpye: str, default='percent'
            手续费计算方式：
            - 'percent': 按成交金额百分比收取
            - 'amount': 按每手固定金额收取
            
    返回值：
        data_df: pd.DataFrame
            在输入数据框的基础上，增加以下计算字段：
            - 'contract_num_hold': 上一期持仓手数
            - 'contract_num_trade': 当期交易手数（正值为加仓，负值为减仓）
            - 'holding_pnl': 持仓盈亏（已有仓位产生的盈亏）
            - 'trade_profit': 交易盈亏（新开仓位产生的盈亏）
            - 'profit': 总盈亏（持仓盈亏+交易盈亏）
            - 'fee': 交易手续费
            - 'net_profit': 净盈亏（总盈亏-手续费）
            - 'profit_cum': 累计净盈亏
    """
    df = data_df
    # 1. 计算持仓手数变化（换手/加仓/减仓）
    df['contract_num_hold'] =  df['contract_num'].shift(1)
    df['contract_num_trade'] = df['contract_num'] - df['contract_num'].shift(1)
    # 2. 持仓盈亏（MTM）：上一时刻持仓 × (本周期收盘价-上周期收盘价) × 方向 × 合约乘数
    df['holding_pnl'] = df['contract_num_hold'] * (df['close'] - df['close'].shift(1)) * contractMulti * df['pos']
    # 3. 交易盈亏（换手）：本周期换手部分 × (本周期成交价-上周期收盘价) × 方向 × 合约乘数
    if  'trade_price' not in df.columns:
        slippage = minpoint * slippageMulti
        df['trade_price'] =df['open'] + slippage*(df['pos'] - df['pos'].shift(1))
    df['trade_profit'] = df['contract_num_trade'] * (df['close'] - df['trade_price']) * contractMulti * df['pos']
    # 4. 总盈亏
    df['profit'] = df['holding_pnl'] + df['trade_profit']
    # 5. 手续费（仅对换手部分收取）
    if cost_type == 'percent':
        df['fee'] = abs(df['contract_num_trade']) * cost * contractMulti * df['open_pos_price']
    elif cost_type == 'amount':
        df['fee'] = abs(df['contract_num_trade']) * cost 
    df['fee'].fillna(0,inplace=True)
    # 6. 净盈利
    df['net_profit'] = df['profit'] - df['fee']
    df['profit_cum'] = df['net_profit'].cumsum()
    return df

# 将分钟线数据转换为日线数据
def minute_to_day_df(data_df:pd.DataFrame,drop_nan_price_day:bool=True,drop_zero_volume_day:bool=False)->pd.DataFrame:
    """
    将分钟线数据转换为日线数据

    参数说明：
        data_df: pd.DataFrame
            分钟线数据
            必须包含以下字段：'candle_begin_time','trading_date','open', 'close', 'high','low', 'volume'，分别为开盘价、收盘价、最高价、最低价、成交量。
            
    """
    # -转换为日线数据
    period_df_day = data_df.groupby('trading_date').agg(
                                                    {'open': 'first',
                                                      'high': 'max',
                                                      'low': 'min',
                                                      'close': 'last',
                                                      'volume': 'sum',
                                                      })
    if drop_nan_price_day:
        period_df_day.dropna(subset=['open'], inplace=True)  # 去除一天都没有行情的日期
    if drop_zero_volume_day:
        period_df_day = period_df_day[period_df_day['volume'] > 0]  # 去除成交量为0的交易日期(默认不去除，因为有可能一字板)
    period_df_day.reset_index(inplace=True)
    period_df_day['candle_begin_time'] = period_df_day['trading_date']
    day_df = period_df_day[['candle_begin_time', 'open', 'high', 'low', 'close', 'volume']]
    day_df.reset_index(inplace=True, drop=True)
    return day_df

# 生成ATR
def cal_atr(data_df:pd.DataFrame,atr_period:int)->pd.DataFrame:
    atr_series = ta.atr(data_df['high'],data_df['low'],data_df['close'],length=atr_period)
    return atr_series