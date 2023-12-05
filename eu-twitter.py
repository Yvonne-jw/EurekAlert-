# -*- coding: utf-8 -*-
"""
Created on Thu Mar  9 10:51:14 2023

@author: zhangz10
"""

import json
import pandas as pd
from tqdm import tqdm
import os


def json_to_columns(df,col_name):
    for i in df[col_name][0].keys():
        list2 = [j[i] for j in df[col_name]]
        df[col_name+i] = list2
    df.drop(col_name,axis = 1, inplace = True)
    return df

def json_parse(df):
    for i in df.keys():
        if type(df[i][0]) == dict and df[i][0]!={}:
            df = json_to_columns(df,i)
    return df

def list_parse(df,i):
    if type(df[i][0]) == list and df[i][0] != []:
        list1 =[j[0] if j!=[] else np.nan for j in df[i]]
        df[i] = list1
    return df

def json_to_df(res):
    df = pd.DataFrame.from_dict(pd.json_normalize(res), orient='columns')
    return df

def get_urls(df,dic):
    for i in range(0,len(dic)):
        url = 'url_'+ str(i)
        exurl = 'expanded_url_'+ str(i)
        df[url] = dic[i]['url']
        df[exurl] = dic[i]['expanded_url']
    return df

def get_data(jess_dic):
    df = pd.DataFrame()
    twdf = pd.DataFrame()
    for i in range(len(jess_dic)):  #len(jess_dic)
        try:
            for twdata in jess_dic[i]['data']:
                tem_df = json_to_df(twdata)
                retweet= 'referenced_tweets'
                if retweet in tem_df.keys():
                    if type(tem_df[retweet][0]) == list and tem_df[retweet][0] != []:
                        tem_df = list_parse(tem_df,retweet)
                    twdf = json_to_columns(tem_df,retweet)
                df = pd.concat([df,twdf],ignore_index = True)
        except:
            print(i)
    return df

#import data
alldata = pd.DataFrame()
names = os.listdir('D:/Users/dudekj/Eurekalert_recollect/')
for t in tqdm(range(len(names))):
    tw = open('D:/Users/dudekj/Eurekalert_recollect/'+ names[t],'r')
    jess_dic = json.load(tw)
    datadf = get_data(jess_dic)
    alldata = pd.concat([alldata,datadf],ignore_index = True)
alldata.to_csv('C:/Users/zhangz10/ER/newtwitter_data.csv', mode='a', index=False, sep=';')


    '''
    if enurl in tem_df.keys():
        dic = tem_df[enurl][0]
        tem_df = get_urls(tem_df,dic)
        tem_df.drop(enurl,axis = 1, inplace = True)
        df = pd.concat([df,tem_df],ignore_index = True)
    else:
        df = pd.concat([df,tem_df],ignore_index = True)
    '''
    if i%30000 == 0:
        df1 = pd.concat([df1,df],ignore_index = True)
        df = pd.DataFrame()
    

   # data = pd.DataFrame(columns =jess_dic[i].keys())
    #data =  data.append(jess_dic[0],ignore_index = True) twitter_data
df1.to_csv('C:/Users/zhangz10/ER/atwitter_data.csv', mode='a', index=False, sep=';')


url12 = []
a = 0
for st in df1['expanded_url_12']:
    if str(st) == 'nan':
        a =a+1
    else:
        url12.append(st)
        print(a)
