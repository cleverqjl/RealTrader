path = r'D:\CTATrade'
import sys
sys.path.append(path)
from strategy import brandy,tequila,whisky,whiskyMix,AbsolutVodka
from strategyForTrade import whiskySignal,tequilaSignal,brandySignal
import pandas as pd
import numpy as np
'''所有交易品种'''
future_set_dict_all = {
                    'SHFE': ['rb','hc','ru','au','ag','cu','al','zn','ni','bu'],
                    'DCE' : ['l','pp','i','j','jm','m','p','y','a','c'],
                    'CZCE': ['SR','CF','OI','AP','TA','ZC','RM','MA']
                        }
future_set_dict = {
                    'SHFE': ['rb','hc','ru','au','ag','cu','al','ni','bu','ss'],
                    'DCE' : ['l','pp','i','j','jm','m','p','y','a','eg'],
                    'CZCE': ['SR','CF','OI','TA','RM','MA','SA','SM']
                        }
'''投资组合dict赋值'''
#wh123tequila
strategy_set_dict1 = {}
for exchange in future_set_dict.keys():
    for i in future_set_dict[exchange]:
        strategy_set_dict1[i+'99.%s'%exchange] = {
                                                  'wh1':{'strategy':[whisky,whiskySignal],
                                                         'para':[100,200,1,6],
                                                         'capital_unit':3000,
                                                         'long':1,
                                                         'short':-1},
                                                  'wh2':{'strategy':[whisky,whiskySignal],
                                                         'para':[200,400,1.2,8],
                                                         'capital_unit':3000,
                                                         'long':1,
                                                         'short':-1},
                                                  'wh3':{'strategy':[whisky,whiskySignal],
                                                         'para':[300,500,1.5,8],
                                                         'capital_unit':3000,
                                                         'long':1,
                                                         'short':-1},
                                                  'tequila':{'strategy':[tequila,tequilaSignal],
                                                            'para':[100,200,50,200,4],
                                                            'capital_unit':3000,
                                                            'long':1,
                                                            'short':-1},
                                                    }
#brandy/tequila
strategy_set_dict2 = {}
for exchange in future_set_dict.keys():
    for i in future_set_dict[exchange]:
        strategy_set_dict2[i+'99.%s'%exchange] = {
                                                  'brandy':{'strategy':[brandy,''],
                                                         'para':[400,1.5,8],
                                                         'capital_unit':10000,
                                                         'long':1,
                                                         'short':-1},
                                                  'tequila':{'strategy':[tequila,''],
                                                            'para':[100,200,50,200,4],
                                                            'capital_unit':10000,
                                                            'long':1,
                                                            'short':-1},
                                                    }
#brandy/wh23
strategy_set_dict3 = {}
for exchange in future_set_dict.keys():
    for i in future_set_dict[exchange]:
        strategy_set_dict3[i+'99.%s'%exchange] = {
                                                  'brandy':{'strategy':[brandy,brandySignal],
                                                         'para':[400,1.5,8],
                                                         'capital_unit':2500,
                                                         'long':1,
                                                         'short':-1},
                                                  'wh2':{'strategy':[whisky,whiskySignal],
                                                         'para':[200,400,1.2,8],
                                                         'capital_unit':5000,
                                                         'long':1,
                                                         'short':-1},
                                                  'wh3':{'strategy':[whisky,whiskySignal],
                                                         'para':[300,500,1.5,8],
                                                         'capital_unit':5000,
                                                         'long':1,
                                                         'short':-1},
                                                    }
#brandy/wh23/tequila
strategy_set_dict4 = {}
for exchange in future_set_dict.keys():
    for i in future_set_dict[exchange]:
        strategy_set_dict4[i+'99.%s'%exchange] = {
                                                  'brandy':{'strategy':[brandy,brandySignal],
                                                         'para':[400,1.5,8],
                                                         'capital_unit':2000,
                                                         'long':1,
                                                         'short':-1},
                                                  'tequila':{'strategy':[tequila,tequilaSignal],
                                                            'para':[100,200,50,200,4],
                                                            'capital_unit':2200,
                                                            'long':1,
                                                            'short':-1},
                                                  'wh2':{'strategy':[whisky,whiskySignal],
                                                         'para':[200,400,1.2,8],
                                                         'capital_unit':5000,
                                                         'long':1,
                                                         'short':-1},
                                                  'wh3':{'strategy':[whisky,whiskySignal],
                                                         'para':[300,500,1.5,8],
                                                         'capital_unit':5000,
                                                         'long':1,
                                                         'short':-1},
                                                    }
        
# strategy_set_dict4 = {}
# strategy_set_dict4['IF99.CFFEX'] = { 
#                                     'brandy':{'strategy':[brandy,brandySignal],
#                                               'para':[400,1.5,8],
#                                               'capital_unit':40000,
#                                               'long':1,
#                                               'short':0},
#                                     'tequila':{'strategy':[tequila,tequilaSignal],
#                                                'para':[100,200,50,200,4],
#                                                'capital_unit':40000,
#                                                'long':1,
#                                                'short':0},
#                                     'wh2':{'strategy':[whisky,whiskySignal],
#                                            'para':[200,400,1.2,8],
#                                            'capital_unit':40000,
#                                            'long':1,
#                                            'short':0},
#                                     'wh3':{'strategy':[whisky,whiskySignal],
#                                            'para':[300,500,1.5,8],
#                                            'capital_unit':40000,
#                                            'long':1,
#                                            'short':0,}
#                                     }
        
        
        
        
        
        
        
        
        