#coding=utf-8
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

sdt_str = '20200926'
edt_str = '20201001'

sdt = datetime.datetime.strptime(sdt_str,"%Y%m%d")
edt = datetime.datetime.strptime(edt_str,"%Y%m%d")
engine = create_engine('postgresql://apuser:apuser@10.101.60.88:5432/stock')
engine = engine.execution_options(autocommit=True)

while sdt < edt :
    datestr = sdt.strftime('%Y%m%d')
    sdt = sdt + datetime.timedelta(days = 1)
    r = requests.post('http://www.twse.com.tw/fund/T86?response=csv&date=' + datestr + '&selectType=ALL')
    time.sleep(random.randint(25 ,35))
    if r.text == '' :
        print('tt')
        continue

    regex_str = '"證券代號","證券名稱",(.*?)"三大法人買賣超股數",'
    match_obj = re.search(regex_str, r.text)
    if match_obj:
        llen = len(match_obj.group().split('",'))
        if llen == 13:
                df = pd.read_csv(StringIO("\n".join(
                [i.translate({ord(c): None for c in ' '}) for i in r.text.split('\n') if len(i.split('",')) == 13 and i[0] != '=']
                )
                ) , thousands="," ,header = 0 )
                #"證券代號","證券名稱","外資買進股數","外資賣出股數","外資買賣超股數","投信買進股數","投信賣出股數","投信買賣超股數","自營商買賣超股數","自營商買進股數","自營商賣出股數","三大法人買賣超股數",
                #自營商買賣超股數 = 自營商買進股數 + 自營商賣出股數
                df.columns = ['security_code','code_name','foreign_dealers_total_buy','foreign_dealers_total_sell','foreign_dealers_total_diff'
                ,'investment_company_buy','investment_company_sell','investment_company_diff'
                ,'dealers_diff','dealers_proprietary_buy','dealers_proprietary_sell','dealers_proprietary_diff','data_dt']
                df['data_dt'] = datestr
        elif llen == 17:
                df = pd.read_csv(StringIO("\n".join(
                [i.translate({ord(c): None for c in ' '}) for i in r.text.split('\n') if len(i.split('",')) == 17 and i[0] != '=']
                )
                ) , thousands="," ,header = 0 )
                "證券代號","證券名稱","外資買進股數","外資賣出股數","外資買賣超股數","投信買進股數","投信賣出股數","投信買賣超股數"
                #,"自營商買賣超股數","自營商買進股數","自營商賣出股數","三大法人買賣超股數",
                #自營商買賣超股數 = 自營商買賣超股數(自行買賣) + 自營商買賣超股數(避險)
                df.columns = ['security_code','code_name','foreign_dealers_total_buy','foreign_dealers_total_sell','foreign_dealers_total_diff'
                ,'investment_company_buy','investment_company_sell','investment_company_diff'
                ,'dealers_diff','dealers_proprietary_buy','dealers_proprietary_sell','dealers_proprietary_diff'
                ,'dealers_hedge_buy','dealers_hedge_sell','dealers_hedge_diff','total_diff','data_dt']
                df['data_dt'] = datestr
        elif llen == 20:
                df = pd.read_csv(StringIO("\n".join(
                [i.translate({ord(c): None for c in ' '}) for i in r.text.split('\n') if len(i.split('",')) == 20 and i[0] != '=']
                )
                ) , thousands="," ,header = 0 )
                #"證券代號","證券名稱","外陸資買進股數(不含外資自營商)","外陸資賣出股數(不含外資自營商)","外陸資買賣超股數(不含外資自營商)","外資自營商買進股數","外資自營商賣出股數","外資自營商買賣超股數"
                # ,"投信買進股數","投信賣出股數","投信買賣超股數","自營商買賣超股數","自營商買進股數(自行買賣)","自營商賣出股數(自行買賣)","自營商買賣超股數(自行買賣)"
                # ,"自營商買進股數(避險)","自營商賣出股數(避險)","自營商買賣超股數(避險)","三大法人買賣超股數",
                df.columns = ['security_code','code_name','foreign_total_buy','foreign_total_sell','foreign_total_diff','foreign_dealers_total_buy'
                ,'foreign_dealers_total_sell','foreign_dealers_total_diff','investment_company_buy','investment_company_sell'
                ,'investment_company_diff','dealers_diff','dealers_proprietary_buy','dealers_proprietary_sell','dealers_proprietary_diff'
                ,'dealers_hedge_buy','dealers_hedge_sell','dealers_hedge_diff','total_diff','data_dt']
                df['data_dt'] = datestr
        else:
            continue
    else:
        continue

    print(datestr)

   
    df.to_sql(
        'daily_trading_detail', 
        engine,
        index=False,
        if_exists='append' 
    )



