import requests
import pandas as pd
import numpy as np
import psycopg2
import re
import datetime
import time
import random
from io import StringIO
from sqlalchemy import create_engine

sdt_str = '20201012'
edt_str = '20201013'
#sdt_str = '20200922'
#edt_str = '20200925'

sdt = datetime.datetime.strptime(sdt_str,"%Y%m%d")
edt = datetime.datetime.strptime(edt_str,"%Y%m%d")
engine = create_engine('postgresql://apuser:apuser@10.101.60.88:5432/stock')
engine = engine.execution_options(autocommit=True)

while sdt < edt :
    datestr = sdt.strftime('%Y%m%d')
    sdt = sdt + datetime.timedelta(days = 1)
    r = requests.post('http://www.twse.com.tw/exchangeReport/MI_INDEX?response=csv&date=' + datestr + '&type=ALL')
    #time.sleep(random.randint(15,25))
    if r.text == '' :
        print('tt')
        continue
    df = pd.read_csv(StringIO("\n".join(
        # i.translate({ord(c): None for c in ' '})
        [re.sub('"--"' ,'' ,i) for i in r.text.split('\n') if len(i.split('",')) == 17 and i[0] != '=']
        )
        ) , thousands="," ,header = 0 )
    #"證券代號","證券名稱","成交股數","成交筆數","成交金額","開盤價","最高價","最低價","收盤價","漲跌(+/-)","漲跌價差","最後揭示買價","最後揭示買量","最後揭示賣價","最後揭示賣量","本益比",
    df.columns = ['security_code' ,'code_name' ,'trade_volume' ,'transactions' ,'trade_value' ,'opening_price' ,'highest_price' ,'lowest_price' ,'closing_price' ,'dir'
     ,'change' ,'last_best_bid_price' ,'last_best_bid_volume' ,'last_best_ask_price' ,'last_best_ask_volume' ,'price_eaming_ratio' ,'data_dt']
    trans_dict  = {'+' : '1' ,'-':'-1' ,' ':'0' ,'X':'-2'} #漲跌(+/-)欄位符號說明:+/-/X表示漲/跌/不比價
    trans_table ="+- X".maketrans(trans_dict) 
    df["dir"]= df["dir"].str.translate(trans_table)  # https://vimsky.com/zh-tw/examples/usage/python-pandas-series-str-translate.html  
    df['data_dt'] = datestr
    
    print(datestr)
        
    df.to_sql(
        'daily_quotes', 
        engine,
        index=False,
        if_exists='append' 
    )
    engine.execute("update stock_calendar set stock_data = 1 where data_dt = '" + datetime.datetime.strptime(datestr ,"%Y%m%d").strftime('%Y-%m-%d') + "'")



