import numpy as np
import pandas as pd

#%%计算回报指标
def AROR(s,freq='tradeday'):
    if freq=='tradeday':
        n=252
    elif freq=='allday':
        n=365
    elif freq=='w':
        n=52
    nav=s.cumsum()[-1]
    runday=s.count()
    AROR=(nav)*(n/runday)
    return AROR

#计算年化波动率
def AVol(s,freq='tradeday'):
    if freq=='tradeday':
        n=252
    elif freq=='allday':
        n=365    
    elif freq=='w':
        n=52
    AVol=(n)**0.5*(s).std()
    return AVol

#计算组合夏普率
def Sharp(s,rfree,freq='tradeday'):
    Sharp=(AROR(s,freq)-rfree)/AVol(s,freq)
    return Sharp

#计算组合最大回撤(最大回撤率，最大回撤开始时间，最大回撤终止时间，最大回撤覆盖时间)
def Max_dd(s: np.array):
    if len(s) <= 1:
        return 0
    x = np.array(s).cumsum()
    i = np.argmax(np.maximum.accumulate(x) - x)  # end of the period
    if i!=0:
        j = np.argmax(x[:i])  # start of period
        z= np.where(x>x[j])
        if list(z[0])==[]:
            coverdate='not yet'
        elif list(z[0])!=[]:
            coverdate=s.index[z[0][0]].strftime('%Y-%m-%d')
        return [x[j] - x[i],s.index[j].strftime('%Y-%m-%d'),s.index[i].strftime('%Y-%m-%d'),coverdate]
    elif i==0:
        return [0,'无回撤','无回撤','无回撤']

#计算组合卡码率
def Calmar(s,freq='tradeday'):
    if Max_dd(s)[0]!=0:
        Calmar=AROR(s,freq)/Max_dd(s)[0]
    else:
        Calmar='-'
    return Calmar

