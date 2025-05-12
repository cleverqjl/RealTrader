#%% 
import numpy as np
from pandas import Series,DataFrame
from datetime import datetime
import time
import pandas as pd 
import math
import sys
import talib 
import matplotlib.pyplot as plt
import matplotlib.mlab as mlab
from scipy import stats
from statsmodels.formula.api import ols
import statsmodels.api as sm
import statsmodels.stats.anova as anova
#%% 
''' s务必为Series，不能为dataframe'''
#%% 
'''数据离散度统计'''
#求平均数
def mean(s):
    return s.mean()
#求中位数
def median(s):
    return s.median()
#求分位数
def quantile(s,n):
    return s.quantile(n)
#求极差
def distance(s):
    return s.max()-s.min()
#求平均绝对偏差
def mad(s):
    return s.mad()
#求标准差
def std(s):
    return s.std()  
#求方差
def var(s):
    return s.var()
#%% 
'''分布特征统计'''
#求大于x概率，{上涨，收益率序列，x=0})
def prob(s,x):
    p=len(s[s>x])/len(s)
    return p
#求x在s中的百分位（即小于x概率，{上涨，收益率序列，x=0})
def percent(x,s):
    p=len(s[s>x])/len(s)
    return 1-p
#估算未来m天内至少有n天上涨的概率
def probfuture(n,m,p):
    return stats.binom.cdf(m-n,m,1-p)
#绘制概率分布图和累计分布图
def distrib(s):
    density=stats.kde.gaussian_kde(s)
    bins=np.arange(s.min(),s.max(),(s.max()-s.min())/1000)
    plt.rcParams['font.sans-serif']=['SimHei'] #用来正常显示中文标签
    plt.rcParams['axes.unicode_minus']=False #用来正常显示负号
    plt.figure(figsize=(12, 7))
    plt.subplot(211)
    plt.plot(bins,density(bins))
    plt.title('概率密度曲线函数')
    plt.subplot(212)
    plt.plot(bins,density(bins).cumsum())
    plt.title('累计分布函数图')
    #plt.savefig('plot123_2.png', dpi=300)
#求VaR值 （该日有p%的概率损失不会超过的值,假设收益率为正态分布）
def VaR(s,p):
    return stats.norm.ppf(1-p,mean(s),std(s))
#绘制某两列数据的散点图
def scatter(s1,s2,s1name,s2name):
    plt.figure(figsize=(12, 7))
    plt.scatter(s1,s2)
    plt.title('散点图')
    plt.xlabel(s1name)
    plt.ylabel(s2name)
 #%%
'''参数估计&正太分布检验'''    
#画分布直方图以及正态分布拟合线
def ParameterNormDistrib(s,n):
    plt.figure(figsize=(12, 7))
    num_bins = n #直方图柱子的数量 
    n, bins, patches = plt.hist(s.dropna(), num_bins,density=1) 
    #直方图函数，x为x轴的值，normed=1表示为概率密度，即和为一，绿色方块，色深参数0.5.返回n个概率，直方块左边线的x值，及各个方块对象 
    y = mlab.normpdf(bins, s.mean(), s.std())#拟合一条最佳正态分布曲线y 
    plt.plot(bins, y, 'r--') #绘制y的曲线 
    plt.xlabel('value') #绘制x轴 
    plt.ylabel('Probability') #绘制y轴 
    plt.title(r'Histogram : $\mu=%s$, $\sigma=%s$'%(round(s.mean(),2),round(s.std(),2)))#中文标题 u'xxx' 
    #plt.subplots_adjust(left=0.15)#左边距 
    plt.show()
#正态分布检验(pvalue>0.05 差异非显著，符合正态分布)
from scipy.stats import kstest
def Normtest(s):
    sm.qqplot(s,line = 's',)
    plt.grid(True)
    plt.xlabel('theoretical quantiles')
    plt.ylabel('sample quantiles')
    plt.show()
    return kstest(s, 'norm')   
#求置信水平为a的置信区间
def confidenceintv(s,a):
    return stats.t.interval(a,len(s)-1,s.mean(),stats.sem(s))    

#单样本t检验 (pvalue>0.05,则认为不拒绝原假设，差异非显著)(用于检验样本均值与总体均值是否差异显著)
def ttest_1samp(s,n):  #n=假设的总体均值
    return stats.ttest_1samp(s,0)

#独立样本t检验 (pvalue>0.05,则认为不拒绝原假设，差异非显著)(用于检验两来自独立总体的样本其独立总体均值是否一致)
def ttest_ind(s1,s2):
    return stats.ttest_ind(s1,s2)

#配对样本t检验 (pvalue>0.05,则认为不拒绝原假设，差异非显著)(用于检验两个相关样本是否来自具有相同均值的总体）
def ttest_rel(s1,s2):
    return stats.ttest_rel(s1,s2)
#%%
'''方差分析'''     
#定义分组函数（按分位数分为n组,做根据数值分组的单因素方差分析）
def group(s,groupnum):
    grouplist=[]
    for i in list(s.index):
        group=math.ceil(percent(s.ix[i],s)/(1/groupnum))
        grouplist.append('Group '+str(group))
    groupseries=pd.Series(grouplist)
    
    groupdict={}
    groupdict_keys=[]
    groupdict_values=[]
    for i in range(0,groupnum):
        groupdict_keys.append('Group '+str(i+1))
        groupdict_values.append([str(int((i)*(1/groupnum)*100))+'%分位 : '+str(int((i+1)*(1/groupnum)*100))+'%分位',\
                                 str(quantile(s,(i)*(1/groupnum)))+' : '+str(quantile(s,(i+1)*(1/groupnum)))])
    groupdict=dict(zip(groupdict_keys,groupdict_values))
    return groupseries,groupdict

#单因素方差分析(需要输入因变量s(s应满足正态分布)，自变量分组group，其中group用group函数处理因变量数值序列s2，即group(s2,n))
from statsmodels.formula.api import ols
import statsmodels.api as sm
import statsmodels.stats.anova as anova
def anova_single(s,group):
    data=pd.concat([s,group],axis=1,join='outer')
    data.columns=['y','x']
    model=ols('y~C(x)',data=data.dropna()).fit()
    table1=anova.anova_lm(model)
    Pvalue=table1.ix['C(x)','PR(>F)']
    return Pvalue

    
#双因素方差分析(检验两个因子每个因子是否对因变量有重要影响，<0.05则显著)
def anova_two(s,group1,group2):
    data=pd.concat([s,group1,group2],axis=1,join='outer')
    data.columns=['y','x1','x2']
    model=ols('y~C(x1)+C(x2)',data=data.dropna()).fit()
    table1=anova.anova_lm(model)
    Pvalue1=table1.ix['C(x1)','PR(>F)']
    Pvalue2=table1.ix['C(x2)','PR(>F)']
    return Pvalue1,Pvalue2

#双因素析因方差分析(检验这两个因子对因变量的影响是否与其中另一个因子水平有关，p1,p2<0.05,p3>0.05,则两因子不依赖对方，并影响因变量)
def anova_two_xy(s,group1,group2):
    data=pd.concat([s,group1,group2],axis=1,join='outer')
    data.columns=['y','x1','x2']
    model=ols('y~C(x1)*C(x2)',data=data.dropna()).fit()
    table1=anova.anova_lm(model)
    Pvalue1=table1.ix['C(x1)','PR(>F)']
    Pvalue2=table1.ix['C(x2)','PR(>F)']
    Pvalue3=table1.ix['C(x1):C(x2)','PR(>F)']
    return Pvalue1,Pvalue2,Pvalue3
#%%
'''回归分析''' 
#一元线性回归模型
#计算拟合模型 (x为自变量，y为因变量，注意不要有Nan值)
            #( 可查看: .summary()概况 .params参数 .resid残差 .fittedvalues拟合值)
def regression_simple(x,y):
    model=sm.OLS(y,sm.add_constant(x)).fit()
    return model
#拟合结果绘图 
def regression_simple_plot(x,y,model):
    y_fitted = model.fittedvalues
    fig, ax = plt.subplots(figsize=(12,7))
    ax.plot(x, y, 'o', label='样本值')
    ax.plot(x, y_fitted, 'r--.',label='拟合值')
    ax.legend(loc='best')
    #ax.axis((2000, 2500, 2000, 2500))
#拟合残差分布绘图 (残差值和拟合值应没有任何关联，呈现出围绕着0的随机分布状态)
def regression_simple_residplot(model):
    y_fitted = model.fittedvalues
    plt.subplots(figsize=(12,7))
    plt.scatter(y_fitted,model.resid)
    plt.xlabel('拟合值')
    plt.ylabel('残差')
    plt.plot
#拟合残差正态性绘图 (当因变量为正态分布时，那么模型的残差项应该是一个均值为0的正态分布，图上的点应该落在一条直线上)
def regression_simple_residNormplot(model):
    sm.qqplot(model.resid_pearson,stats.norm,line='45')
#拟合残差同方差性绘图(若满足不变方差假定，各点分布应该呈现出一条水平的、宽度一致的条带形状)
def regression_simple_residVarplot(model):
    plt.subplots(figsize=(12,7))
    plt.scatter(model.fittedvalues,model.resid_pearson**0.5)
    plt.xlabel('拟合值')
    plt.ylabel('标准化残差的平方根')
    plt.plot












