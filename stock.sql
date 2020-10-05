-- daily_quotes
drop table if exists daily_quotes;
create table daily_quotes(
security_code character varying(10)
,code_name character varying(20)
,trade_volume integer
,transactions integer
,trade_value numeric(18,0)
,opening_price numeric(8,2)
,highest_price numeric(8,2)
,lowest_price numeric(8,2)
,closing_price numeric(8,2)
,dir smallint
,change numeric(8,2)
,last_best_bid_price numeric(8,2)
,last_best_bid_volume integer
,last_best_ask_price numeric(8,2)
,last_best_ask_volume integer
,price_eaming_ratio numeric(8,2)
,data_dt date
);
comment on column daily_quotes.security_code is '證券代號';
comment on column daily_quotes.code_name is '證券名稱';
comment on column daily_quotes.trade_volume is '成交股數';
comment on column daily_quotes.transactions is '成交筆數';
comment on column daily_quotes.trade_value is '成交金額';
comment on column daily_quotes.opening_price is '開盤價';
comment on column daily_quotes.highest_price is '最高價';
comment on column daily_quotes.lowest_price is '最低價';
comment on column daily_quotes.closing_price is '收盤價';
comment on column daily_quotes.dir is '+/-/x 表示 漲/跌/不比價';
comment on column daily_quotes.change is '漲跌價差';
comment on column daily_quotes.last_best_bid_price is '最後揭示買價';
comment on column daily_quotes.last_best_bid_volume is '最後揭示買量';
comment on column daily_quotes.last_best_ask_price is '最後揭示賣價';
comment on column daily_quotes.last_best_ask_volume is '最後揭示賣量';
comment on column daily_quotes.price_eaming_ratio is '本益比';

-- stock_calendar
drop table if exists stock_calendar;
create table stock_calendar(
data_dt date
,week_num smallint
,weekly_num smallint
,stock_data smallint
);
comment on column stock_calendar.week_num is 'Sunday(0) to Saturday(6)';
comment on column stock_calendar.weekly_num is '1~52 week';
comment on column stock_calendar.stock_data is 'daily_stock_info是否有資料 0:無 1:有';

insert into stock_calendar
select dates ,extract(dow from dates) as week_num ,extract(week from dates) as weekly_num ,0 as stock_data
from generate_series('2004-02-11'::date,'2030-12-31'::date,'1 days') as gs(dates);

-- daily_trading_detail
drop table if exists daily_trading_detail;
create table daily_trading_detail(
security_code character varying(10)
,code_name character varying(20)
,foreign_total_buy integer
,foreign_total_sell integer
,foreign_total_diff integer
,foreign_dealers_total_buy integer
,foreign_dealers_total_sell integer
,foreign_dealers_total_diff integer
,investment_company_buy integer
,investment_company_sell integer
,investment_company_diff integer
,dealers_diff integer
,dealers_proprietary_buy integer
,dealers_proprietary_sell integer
,dealers_proprietary_diff integer
,dealers_hedge_buy integer
,dealers_hedge_sell integer
,dealers_hedge_diff integer
,total_diff integer
,data_dt date
);

comment on column daily_trading_detail.foreign_total_buy is '外陸資買進股數(不含外資自營商)Foreign Investors include Mainland Area Investors (Foreign Dealers excluded) Buy';
comment on column daily_trading_detail.foreign_total_sell is '外陸資賣出股數(不含外資自營商)Foreign Investors include Mainland Area Investors (Foreign Dealers excluded) Sell';
comment on column daily_trading_detail.foreign_total_diff is '外陸資買賣超股數(不含外資自營商)Foreign Investors include Mainland Area Investors (Foreign Dealers excluded) Difference';
comment on column daily_trading_detail.foreign_dealers_total_buy is '外資自營商買進股數Foreign Dealers Total Buy';
comment on column daily_trading_detail.foreign_dealers_total_sell is '外資自營商賣出股數Foreign Dealers Total Sell';
comment on column daily_trading_detail.foreign_dealers_total_diff is '外資自營商買賣超股數Foreign Dealers Difference';
comment on column daily_trading_detail.investment_company_buy is '投信買進股數Securities Investment Trust Companies Total Buy';
comment on column daily_trading_detail.investment_company_sell is '投信賣出股數Securities Investment Trust Companies Total Sell';
comment on column daily_trading_detail.investment_company_diff is '投信買賣超股數Securities Investment Trust Companies Difference';
comment on column daily_trading_detail.dealers_diff is '自營商買賣超股數Dealers Difference';
comment on column daily_trading_detail.dealers_proprietary_buy is '自營商買進股數(自行買賣)Dealers (Proprietary) Total Buy';
comment on column daily_trading_detail.dealers_proprietary_sell is '自營商賣出股數(自行買賣)Dealers (Proprietary) Total Sell';
comment on column daily_trading_detail.dealers_proprietary_diff is '自營商買賣超股數(自行買賣)Dealers (Proprietary) Difference';
comment on column daily_trading_detail.dealers_hedge_buy is '自營商買進股數(避險)Dealers (Hedge) Total Buy';
comment on column daily_trading_detail.dealers_hedge_sell is '自營商賣出股數(避險)Dealers (Hedge) Total Sell';
comment on column daily_trading_detail.dealers_hedge_diff is '自營商買賣超股數(避險)Dealers (Hedge) Difference';
comment on column daily_trading_detail.total_diff is '三大法人買賣超股數 Total Difference';

-- category_list
drop table if exists category_list;
create table category_list(
id serial
,category_chi character varying(10)
,category_eng character varying(64)
);
comment on column category_list.category_chi is '上市公司產業類別(中文)';
comment on column category_list.category_eng is '上市公司產業類別(英文)';

insert into category_list(category_chi ,category_eng) values('水泥工業','Cement');
insert into category_list(category_chi ,category_eng) values('食品工業','Food');
insert into category_list(category_chi ,category_eng) values('塑膠工業','Plastic');
insert into category_list(category_chi ,category_eng) values('紡織纖維','Textile');
insert into category_list(category_chi ,category_eng) values('電機機械','Electric,Machinery');
insert into category_list(category_chi ,category_eng) values('電器電纜','Electrical and Cable');
insert into category_list(category_chi ,category_eng) values('化學工業','Chemical Industry');
insert into category_list(category_chi ,category_eng) values('生技醫療業','Biotechnology and Medical Care Industry');
insert into category_list(category_chi ,category_eng) values('玻璃陶瓷','Glass and Ceramic');
insert into category_list(category_chi ,category_eng) values('造紙工業','Paper and Pulp');
insert into category_list(category_chi ,category_eng) values('鋼鐵工業','Iron and Steel');
insert into category_list(category_chi ,category_eng) values('橡膠工業','Rubber');
insert into category_list(category_chi ,category_eng) values('汽車工業','Automobile');
insert into category_list(category_chi ,category_eng) values('半導體業','Semiconductor Industry');
insert into category_list(category_chi ,category_eng) values('電腦及週邊設備業','Computer and Peripheral Equipment Industry');
insert into category_list(category_chi ,category_eng) values('光電業','Optoelectronic Industry');
insert into category_list(category_chi ,category_eng) values('通信網路業','Communications and Internet Industry');
insert into category_list(category_chi ,category_eng) values('電子零組件業','Electronic Parts/Components Industry');
insert into category_list(category_chi ,category_eng) values('電子通路業','Electronic Products Distribution Industry');
insert into category_list(category_chi ,category_eng) values('資訊服務業','Information Service Industry');
insert into category_list(category_chi ,category_eng) values('其他電子業','Other Electronic Industry');
insert into category_list(category_chi ,category_eng) values('建材營造','Building Material and Construction');
insert into category_list(category_chi ,category_eng) values('航運業','Shipping and Transportation');
insert into category_list(category_chi ,category_eng) values('觀光事業','Tourism');
insert into category_list(category_chi ,category_eng) values('金融保險','Financial and Insurance');
insert into category_list(category_chi ,category_eng) values('貿易百貨',"Trading and Consumers' Goods Industry");
insert into category_list(category_chi ,category_eng) values('油電燃氣業','Oil, Gas and Electricity Industry');
insert into category_list(category_chi ,category_eng) values('綜合企業','General');
insert into category_list(category_chi ,category_eng) values('其他','Other Industry');

-- stock_list
drop table if exists stock_list;
create table stock_list(
security_code character varying(10)
,code_name character varying(20)
);
comment on column stock_list.security_code is '證券代號';
comment on column stock_list.code_name is '證券名稱';

-- stock_category
drop table if exists stock_category;
create table stock_category(
id serial
,security_code character varying(10)
,category_id integer
);

-- daily_quotes_statistics
drop table if exists daily_quotes_statistics;
create table daily_quotes_statistics(
data_dt date
,security_code character varying(10)
,yester_closing_price numeric(8,2)
,closing_price_avg_5 numeric(8,2)
,closing_price_avg_10 numeric(8,2)
,closing_price_avg_20 numeric(8,2)
,closing_price_avg_60 numeric(8,2)
,trade_volume_avg_5 integer
,trade_volume_avg_10 integer
,trade_volume_avg_20 integer
,trade_volume_avg_60 integer
,trx_avg_5 integer
,trx_avg_10 integer
,trx_avg_20 integer
,trx_avg_60 integer
,closing_price_20_sigma numeric(8,2)
);
comment on column daily_quotes_statistics.data_dt is '統計日期';
comment on column daily_quotes_statistics.security_code is '證券代號';
comment on column daily_quotes_statistics.yester_closing_price is '昨日收盤價';
comment on column daily_quotes_statistics.closing_price_avg_5 is '5日平均收盤價';
comment on column daily_quotes_statistics.closing_price_avg_10 is '10日平均收盤價';
comment on column daily_quotes_statistics.closing_price_avg_20 is '20日平均收盤價';
comment on column daily_quotes_statistics.closing_price_avg_60 is '60日平均收盤價';
comment on column daily_quotes_statistics.trade_volume_avg_5 is '5日平均成交股數';
comment on column daily_quotes_statistics.trade_volume_avg_10 is '10日平均成交股數';
comment on column daily_quotes_statistics.trade_volume_avg_20 is '20日平均成交股數';
comment on column daily_quotes_statistics.trade_volume_avg_60 is '60日平均成交股數';
comment on column daily_quotes_statistics.trx_avg_5 is '5日平均成交筆數';
comment on column daily_quotes_statistics.trx_avg_10 is '10日平均成交筆數';
comment on column daily_quotes_statistics.trx_avg_20 is '20日平均成交筆數';
comment on column daily_quotes_statistics.trx_avg_60 is '60日平均成交筆數';
comment on column daily_quotes_statistics.closing_price_20_sigma is '20日的價格標準差';

insert into daily_quotes_statistics
select data_dt ,security_code
 ,lag(closing_price ,1) over(partition by security_code order by data_dt) as "yester_closing_price"
 ,round(avg(closing_price) over (partition by security_code order by data_dt rows between 4 preceding and current row) ,2) as "closing_price_avg_5"
 ,round(avg(closing_price) over (partition by security_code order by data_dt rows between 9 preceding and current row) ,2) as "closing_price_avg_10"
 ,round(avg(closing_price) over (partition by security_code order by data_dt rows between 19 preceding and current row) ,2) as "closing_price_avg_20"
 ,round(avg(closing_price) over (partition by security_code order by data_dt rows between 59 preceding and current row) ,2) as "closing_price_avg_60"
 ,round(avg(trade_volume) over (partition by security_code order by data_dt rows between 4 preceding and current row) ,0) as "trade_volume_5"
 ,round(avg(trade_volume) over (partition by security_code order by data_dt rows between 9 preceding and current row) ,0) as "trade_volume_10"
 ,round(avg(trade_volume) over (partition by security_code order by data_dt rows between 19 preceding and current row) ,0) as "trade_volume_20"
 ,round(avg(trade_volume) over (partition by security_code order by data_dt rows between 59 preceding and current row) ,0) as "trade_volume_60"
 ,round(avg(transactions) over (partition by security_code order by data_dt rows between 4 preceding and current row) ,0) as "trx_5"
 ,round(avg(transactions) over (partition by security_code order by data_dt rows between 9 preceding and current row) ,0) as "trx_10"
 ,round(avg(transactions) over (partition by security_code order by data_dt rows between 19 preceding and current row) ,0) as "trx_20"
 ,round(avg(transactions) over (partition by security_code order by data_dt rows between 59 preceding and current row) ,0) as "trx_60"
 ,round(stddev(closing_price) over (partition by security_code order by data_dt rows between 19 preceding and current row) ,2) as "closing_price_sigma_20"
from daily_quotes;

-- daily_kd
drop table if exists daily_kd;
create table daily_kd(
data_dt date
,security_code character varying(10)
,rsv_value numeric(5,4)
,k_value numeric(5,4)
,d_value numeric(5,4)
);

-- stock_list
drop table if exists stock_list;
create table stock_list(
security_code character varying(10)
,code_name character varying(20)
,first_quote date
);
comment on column stock_list.first_quote is '第一筆交易資料出現的時間';

insert into stock_list
select security_code ,code_name ,min(data_dt)
from daily_quotes
group by security_code ,code_name ;