import requests
import pandas as pd
import numpy as np
import psycopg2
from io import StringIO
from sqlalchemy import create_engine


'''
1.先從r中利用\n分割每一行資料
2.如果分割後的字串經由'",'再切割後，長度為17而且字串的第一個字不為'='的情況下，就放到array中
3.放到array前，若字串有任何包含任何' '空白字串，就會用None取代掉(就是把空白鍵給取消掉) 
4.最後stringIO最後stringIO將array中加入\n  
{ord(c): None for c in ' '} : 建立{32: None} 字典
'''
datestr = '20180131'
r = requests.post('http://www.twse.com.tw/exchangeReport/MI_INDEX?response=csv&date=' + datestr + '&type=ALL')
df = pd.read_csv(StringIO("\n".join(
    # i.translate({ord(c): None for c in ' '})
    [i.translate({ord(c): None for c in ' '}) for i in r.text.split('\n') if len(i.split('",')) == 17 and i[0] != '=']
    )
    ), thousands="," ,header = 0 )
df.columns = ['security_code' ,'code_name' ,'trade_volume' ,'transactions' ,'trade_value' ,'opening_price' ,'highest_price' ,'lowest_price' ,'closing_price' ,'dir'
 ,'change' ,'last_best_bid_price' ,'last_best_bid_volume' ,'last_best_ask_price' ,'last_best_ask_volume' ,'price_eaming_ratio' ,'unname']
trans_dict  = {'+' : '1' ,'-':'-1' ,' ':'0' ,'X':'-2'}
trans_table ="+- X".maketrans(trans_dict) 
df["dir"]= df["dir"].str.translate(trans_table) # https://vimsky.com/zh-tw/examples/usage/python-pandas-series-str-translate.html
'''
df['opening_price'] = df['opening_price'].replace('--' ,'NULL')
df['highest_price'] = df['highest_price'].replace('--' ,'NULL)
df['lowest_price'] = df['lowest_price'].replace('--' ,'NULL')
df['closing_price'] = df['closing_price'].replace('--' ,'NULL')
'''
df.drop(["unname"], axis = 1, inplace = True)
df.opening_price = df.opening_price.astype(str)
print(df.dtypes)
'''
csv文件自带列标题:header=0
csv文件有列标题，但是想自己换成别的列标题：df.columns = ['A','B','C'] or pd.read_csv('Pandas_example_read.csv', names=['A', 'B','C']) 
csv文件没有列标题，从第一行就直接开始是数据的录入了：pd.read_csv('Pandas_example_read_withoutCols.csv', header=None)


在處理財金相關數據時，常常在讀進CSV檔後，會發現欄位中部分數值因為有千分號(逗號)，所以pandas讀進來後需將欄位型態從object改成float。
以往的處理方式是：
df['col']=df['col'].str.replace(',','').astype(float)
pandas的read_csv有參數可解決
df = pd.read_csv("data.csv", thousands=",")
加個thousands寫明是哪種符號作為分隔符就可以了
'''
'''
engine = create_engine('postgresql://apuser:apuser@10.101.60.88:5432/stock')
df.to_sql(
    'daily_stock_info', 
    engine,
    index=False,
    if_exists='append' 
)
'''
