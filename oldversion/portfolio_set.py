path = r'D:\CTATrade'
import sys
sys.path.append(path)
from strategy import brandy,tequila,whisky,whiskyMix,AbsolutVodka
import pandas as pd
import numpy as np
'''所有交易品种'''
# future_set_dict = {
#                     'SHFE': ['rb','hc','ru','au','ag','cu','al','zn','ni','bu'],
#                     'DCE' : ['l','pp','jd','i','j','jm','m','p','y','a'],
#                     'CZCE': ['SR','CF','OI','AP','TA','ZC','RM','MA']
#                         }
# future_set_dict = {
#                     'SHFE': ['rb','ag','al'],
#                     'DCE' : ['i','jm','l','p'],
#                     'CZCE': ['ZC','CF','SR','MA']
#                         }
future_set_dict = {
                    'SHFE': ['rb','hc','ru','au','ag','cu','al','ni','bu','ss'],
                    'DCE' : ['l','pp','i','j','jm','m','p','y','a','eg'],
                    'CZCE': ['SR','CF','OI','AP','TA','RM','MA','SA','SM']
                        }

'''投资组合dict赋值'''
#brandy
portfolio_set_dict1 = {}
for exchange in future_set_dict.keys():
    for i in future_set_dict[exchange]:
        portfolio_set_dict1[i+'_Brandy'] = {  'symbol' : i+'99.%s'%exchange,
                                              'strategy' : brandy,
                                              'para' : [400,1.5,8],
                                              'time_interval' : '10m',
                                              'capitalmode' : 'atr',
                                              'capital_unit' : 2000,
                                              'limitValue' : None,
                                              'long':1,
                                              'short':-1}
#tequila
portfolio_set_dict2 = {}
for exchange in future_set_dict.keys():
    for i in future_set_dict[exchange]:
        portfolio_set_dict2[i+'_Tequila'] = { 'symbol' : i+'99.%s'%exchange,
                                              'strategy' : tequila,
                                              'para' : [100,200,50,200,4],
                                              'time_interval' : '10m',
                                              'capitalmode' : 'atr',
                                              'capital_unit' : 2200,
                                              'limitValue' : None,
                                              'long':1,
                                              'short':-1}
#whisky1
portfolio_set_dict3 = {}
for exchange in future_set_dict.keys():
    for i in future_set_dict[exchange]:
        portfolio_set_dict3[i+'_Whisky1'] = { 'symbol' : i+'99.%s'%exchange,
                                              'strategy' : whisky,
                                              'para' : [100,200,1,6],
                                              'time_interval' : '10m',
                                              'capitalmode' : 'atr',
                                              'capital_unit' : 5000,
                                              'limitValue' : None,
                                              'long':1,
                                              'short':-1}  
#whisky2
portfolio_set_dict4 = {}
for exchange in future_set_dict.keys():
    for i in future_set_dict[exchange]:
        portfolio_set_dict4[i+'_Whisky2'] = { 'symbol' : i+'99.%s'%exchange,
                                              'strategy' : whisky,
                                              'para' : [200,400,1.2,8],
                                              'time_interval' : '10m',
                                              'capitalmode' : 'atr',
                                              'capital_unit' : 5000,
                                              'limitValue' : None,
                                              'long':1,
                                              'short':-1}  
#whisky3
portfolio_set_dict5 = {}
for exchange in future_set_dict.keys():
    for i in future_set_dict[exchange]:
        portfolio_set_dict5[i+'_Whisky3'] = { 'symbol' : i+'99.%s'%exchange,
                                              'strategy' : whisky,
                                              'para' : [300,500,1.5,8],
                                              'time_interval' : '10m',
                                              'capitalmode' : 'atr',
                                              'capital_unit' : 5000,
                                              'limitValue' : None,
                                              'long':1,
                                              'short':-1}  
#whiskyMix
portfolio_set_dict6 = {}
for exchange in future_set_dict.keys():
    for i in future_set_dict[exchange]:
        portfolio_set_dict6[i+'_WhiskyMix'] = { 'symbol' : i+'99.%s'%exchange,
                                                'strategy' : whiskyMix,
                                                'para' : [[400,400,1.5,8],[200,400,1.2,8],[300,500,1.5,8]],
                                                'time_interval' : '10m',
                                                'capitalmode' : 'atr',
                                                'capital_unit' : 10000,
                                                'limitValue' : None,
                                                'long':1,
                                                'short':-1}   
#AbsoluteVodka
portfolio_set_dict7 = {}
for exchange in future_set_dict.keys():
    for i in future_set_dict[exchange]:
        portfolio_set_dict7[i+'_AbsoluteVodka'] = { 'symbol' : i+'99.%s'%exchange,
                                                'strategy' : AbsolutVodka,
                                                'para' : [100,200,50,200,4,400],
                                                'time_interval' : '10m',
                                                'capitalmode' : 'atr',
                                                'capital_unit' : 10000,
                                                'limitValue' : None,
                                                'long':1,
                                                'short':-1}   
        
#whisky4
portfolio_set_dict8 = {}
for exchange in future_set_dict.keys():
    for i in future_set_dict[exchange]:
        portfolio_set_dict8[i+'_Whisky4'] = { 'symbol' : i+'99.%s'%exchange,
                                              'strategy' : whisky,
                                              'para' : [400,400,1.5,8],
                                              'time_interval' : '10m',
                                              'capitalmode' : 'atr',
                                              'capital_unit' : 5000,
                                              'limitValue' : None,
                                              'long':1,
                                              'short':-1}
        
#gz
portfolio_set_dict9 = {}
portfolio_set_dict9['IF_Brandy'] = {  'symbol' : 'IF99.CFFEX',
                                              'strategy' : brandy,
                                              'para' : [400,1.5,8],
                                              'time_interval' : '10m',
                                              'capitalmode' : 'atr',
                                              'capital_unit' : 100000,
                                              'limitValue' : None,
                                              'long':1,
                                              'short':0}
portfolio_set_dict9['IF_Tequila'] = {  'symbol' : 'IF99.CFFEX',
                                                'strategy' : tequila,
                                                'para' : [100,200,50,200,4],
                                                'time_interval' : '10m',
                                                'capitalmode' : 'atr',
                                                'capital_unit' : 100000,
                                                'limitValue' : None,
                                                'long':1,
                                                'short':0}  
portfolio_set_dict9['IF_wh2'] = {  'symbol' : 'IF99.CFFEX',
                                               'strategy' : whisky,
                                               'para' : [200,400,1.2,8],
                                               'time_interval' : '10m',
                                               'capitalmode' : 'atr',
                                               'capital_unit' : 100000,
                                               'limitValue' : None,
                                               'long':1,
                                               'short':0}   
portfolio_set_dict9['IF_wh3'] = {  'symbol' : 'IF99.CFFEX',
                                               'strategy' : whisky,
                                               'para' : [300,500,1.5,8],
                                               'time_interval' : '10m',
                                               'capitalmode' : 'atr',
                                               'capital_unit' : 100000,
                                               'limitValue' : None,
                                               'long':1,
                                               'short':0} 
                

# i='rb'
# exchange='SHFE'
# portfolio_set_dict = {
#                      '%s_brandy'%i:{'symbol' : i+'99.%s'%exchange,
#                                               'strategy' : brandy,
#                                               'para' : [400,1.5,8],
#                                               'time_interval' : '10m',
#                                               'capitalmode' : 'atr',
#                                               'capital_unit' : 3000,
#                                               'limitValue' : None},
#                       '%s_wh2'%i:{'symbol' : i+'99.%s'%exchange,
#                                               'strategy' : whisky,
#                                               'para' : [200,400,1.2,8],
#                                               'time_interval' : '10m',
#                                               'capitalmode' : 'atr',
#                                               'capital_unit' : 3000,
#                                               'limitValue' : None},
#                       '%s_wh3'%i:{'symbol' : i+'99.%s'%exchange,
#                                               'strategy' : whisky,
#                                               'para' : [300,500,1.5,8],
#                                               'time_interval' : '10m',
#                                               'capitalmode' : 'atr',
#                                               'capital_unit' : 3000,
#                                               'limitValue' : None},
#                       '%s_tequila'%i:{'symbol' : i+'99.%s'%exchange,
#                                               'strategy' : tequila,
#                                               'para' : [100,200,50,200,4],
#                                               'time_interval' : '10m',
#                                               'capitalmode' : 'atr',
#                                               'capital_unit' : 3000,
#                                               'limitValue' : None},
#                       }
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        