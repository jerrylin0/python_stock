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
    r = requests.post('http://www.twse.com.tw/exchangeReport/TWT53U?response=csv&date=' + datestr + '&type=ALL')
    #time.sleep(random.randint(90,120))
    if len(r.text) < 10 :
        print('tt')
        continue

    df = pd.read_csv(StringIO("\n".join(
        # i.translate({ord(c): None for c in ' '})
        [re.sub('"--"' ,'' ,i) for i in r.text.split('\n') if len(i.split('",')) == 11 and i[0] != '=']
        )
        ) , thousands="," ,header = 0 )
    #"證券代號","證券名稱","成交股數","成交筆數","成交金額","最後揭示買價","最後揭示買量","最後揭示賣價","最後揭示賣量",,
    df.columns = ['security_code' ,'code_name' ,'trade_volume' ,'transactions' ,'trade_value' ,'trade_price' ,'last_best_bid_price' ,'last_best_bid_volume' ,'last_best_ask_price' ,'last_best_ask_volume' ,'data_dt']
    df['data_dt'] = datestr
    
    print(datestr)
        
    df.to_sql(
        'daily_odd_lot_trade', 
        engine,
        index=False,
        if_exists='append' 
    )
    


