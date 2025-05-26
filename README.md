# RealTrader
a new quick backtest&amp;trade lab for real trader

# Workflow
- strategy 
    - data_processing
    - expression
    - process_signal
- backtest
    - cal_position
    - cal_profit
- analysis
    - pnl_analysis
    - position_analysis
    - risk_analysis

# signal-->position-->contract_num-->profit
┌───────────────────────────────────────┐
│ 1. 策略信号生成 (Signal Generation)    │
└───────────────┬───────────────────────┘
                ↓
┌───────────────────────────────────────┐
│ - 因子计算 (Factor Calculation)        │
│   * 收益率 (return_lag2)               │
│   * 技术指标 (布林带 bb_upper/bb_lower) │
│                                        │
│ - 信号生成 (Signal Generation)         │
│   * short_condition: return_lag2 > 0.0003 & close < bb_upper ⟹ signal = -1  │
│   * long_condition: return_lag2 < -0.0003 & close > bb_lower ⟹ signal = 1   │
│   * 其他情况 ⟹ signal = 0                                                   │
└───────────────┬───────────────────────┘
                ↓
┌───────────────────────────────────────┐
│ 2. 持仓转换 (Position Conversion)      │
└───────────────┬───────────────────────┘
                ↓
┌───────────────────────────────────────┐
│ - 信号到持仓 (Signal to Position)      │
│   * pos = signal.shift(1)             │
│   * 1: 多头持仓                        │
│   * -1: 空头持仓                       │
│   * 0: 空仓                           │
└───────────────┬───────────────────────┘
                ↓
┌───────────────────────────────────────┐
│ 3. 持仓转换为合约手数                   │
│    (Position to Contract Number)      │
└───────────────┬───────────────────────┘
                ↓
┌───────────────────────────────────────┐
│ - pos_to_contract_num 函数               │
│   * 计算开仓条件: condition1 & condition  2│
│   * 计算平仓条件                        │
│   * 计算开仓价格: open + slippage*pos    │
│   * 计算平仓价格: next_open - slippage*pos│
│   * contract_num = 1 (持仓手数)         │
└───────────────┬───────────────────────┘
                ↓
┌───────────────────────────────────────┐
│ 4. 合约手数转换为盈亏                   │
│    (Contract Number to Profit)        │
└───────────────┬───────────────────────┘
                ↓
┌───────────────────────────────────────┐
│ - contract_num_to_profit 函数         │
│                                       │
│ - 持仓盈亏 (Holding PnL)               │
│   * holding_pnl = contract_num_hold   │
│     * (close - close.shift(1))        │
│     * contractMulti * pos             │
│                                       │
│ - 交易盈亏 (Trading PnL)               │
│   * trade_profit = contract_num_trade │
│     * (close - trade_price)           │
│     * contractMulti * pos             │
│                                       │
│ - 总盈亏 (Total PnL)                   │
│   * profit = holding_pnl + trade_profit│
│                                       │
│ - 手续费 (Fee)                         │
│   * 百分比或固定金额                     │
│                                       │
│ - 净盈亏 (Net Profit)                  │
│   * net_profit = profit - fee         │
│                                       │
│ - 累计净盈亏 (Cumulative Net Profit)    │
│   * profit_cum = net_profit.cumsum()  │
└───────────────┬───────────────────────┘
                ↓
┌───────────────────────────────────────┐
│ 5. 绩效分析 (Performance Analysis)     │
└───────────────┬───────────────────────┘
                ↓
┌───────────────────────────────────────┐
│ - 净值曲线 (Equity Curve)              │
│ - 回撤分析 (Drawdown Analysis)         │
│ - 收益统计 (Return Statistics)         │
│ - 风险分析 (Risk Analysis)             │
└───────────────────────────────────────┘

