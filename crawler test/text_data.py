import requests
import pandas as pd
import numpy as np
import psycopg2
import re
import datetime
import time
from io import StringIO
from sqlalchemy import create_engine

sdt_str = '20120502'
edt_str = '20120503'

sdt = datetime.datetime.strptime(sdt_str,"%Y%m%d")
edt = datetime.datetime.strptime(edt_str,"%Y%m%d")
engine = create_engine('postgresql://apuser:apuser@10.101.60.88:5432/stock')
engine = engine.execution_options(autocommit=True)

while sdt < edt :
    #datestr = sdt.strftime('%Y%m%d')
    #fp = open('C:\\Users\\user\\Documents\\GitHub\\T86_ALL_20120502.csv' ,'r')
    #lines = fp.readlines()

    datestr = sdt.strftime('%Y%m%d')
    sdt = sdt + datetime.timedelta(days = 1)
    r = requests.post('http://www.twse.com.tw/fund/T86?response=csv&date=' + datestr + '&selectType=ALL')
    #print(r.text)
    regex_str = '"證券代號","證券名稱",(.*?)"三大法人買賣超股數",'
    match_obj = re.search(regex_str, r.text)
    if match_obj:
        print(match_obj.group())
        print(len(match_obj.group().split('",')))



    #df = pd.read_csv(StringIO("\n".join(
    #    [re.sub('"--"' ,'' ,i) for i in lines if len(i.split('",')) == 17 and i[0] != '=']
    #    )
    #    ) , thousands="," ,header = 0 )
    
    #df.columns = ['security_code' ,'code_name' ,'trade_volume' ,'transactions' ,'trade_value' ,'opening_price' ,'highest_price' ,'lowest_price' ,'closing_price' ,'dir'
    # ,'change' ,'last_best_bid_price' ,'last_best_bid_volume' ,'last_best_ask_price' ,'last_best_ask_volume' ,'price_eaming_ratio' ,'data_dt']
    #trans_dict  = {'+' : '1' ,'-':'-1' ,' ':'0' ,'X':'-2'}
    #trans_table ="+- X".maketrans(trans_dict) 
    #df["dir"]= df["dir"].str.translate(trans_table) # https://vimsky.com/zh-tw/examples/usage/python-pandas-series-str-translate.html
    
    #df.drop(["unname"], axis = 1, inplace = True)
    '''
    df['data_dt'] = datestr
    
    print(datestr)
        
    df.to_sql(
        'daily_stock_info', 
        engine,
        index=False,
        if_exists='append' 
    )
    sdt = sdt + datetime.timedelta(days = 1)
    '''


