select data_dt ,code_name ,transactions
      ,lag(transactions ,1) over (partition by code_name order by data_dt) as "1_day"
	  ,lag(transactions ,5) over (partition by code_name order by data_dt) as "5_day"
	  ,lag(transactions ,20) over (partition by code_name order by data_dt) as "20_day"
	  ,max(transactions) over (partition by code_name order by data_dt ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) as tt
from daily_quotes
where data_dt >= '2020-08-01'
 and data_dt < '2020-09-01'
 and (security_code = '3006' or security_code = '2002')

 -- move average
select data_dt ,security_code
 ,lag(closing_price ,1) over(partition by security_code order by data_dt) as "yester_closing_price"
 ,round(avg(closing_price) over (partition by security_code order by data_dt rows between 4 preceding and current row) ,2) as "closing_price_avg_5"
 ,round(avg(closing_price) over (partition by security_code order by data_dt rows between 9 preceding and current row) ,2) as "closing_price_avg_10"
 ,round(avg(closing_price) over (partition by security_code order by data_dt rows between 19 preceding and current row) ,2) as "closing_price_avg_20"
 ,round(avg(closing_price) over (partition by security_code order by data_dt rows between 59 preceding and current row) ,2) as "closing_price_avg_60"
 ,round(avg(closing_price) over (partition by security_code order by data_dt rows between 119 preceding and current row) ,2) as "closing_price_avg_120"
from daily_quotes ;

-- Bband
with bband as (
	select data_dt ,security_code
	 ,closing_price
	 ,round(avg(closing_price) over (partition by security_code order by data_dt rows between 19 preceding and current row) ,2) as "closing_price_avg_20"
	 ,round(stddev(closing_price) over (partition by security_code order by data_dt rows between 19 preceding and current row) ,2) as "closing_price_sigma_20"
	from daily_quotes
	),
	bband_wide as (
	select data_dt ,security_code
	 ,(closing_price_avg_20 + 2 * closing_price_sigma_20) as bband_top
	 ,(closing_price_avg_20 - 2 * closing_price_sigma_20) as bband_bot
	from bband
	)
select x.data_dt ,x.security_code
 ,y.bband_top 
 ,x.closing_price_avg_20 as bband_mid
 ,y.bband_bot
 ,round(case when (y.bband_top - y.bband_bot) = 0 then 0 else (x.closing_price - y.bband_bot) / (y.bband_top - y.bband_bot) end ,2) as bband_percent
 ,round((y.bband_top - y.bband_bot) / x.closing_price ,2) as bband_width
from bband x join bband_wide y
 on x.data_dt = y.data_dt and x.security_code = y.security_code ;

-- KD
create procedure sp_batch_kd() as 
$$
declare
	cur_code cursor for select distinct security_code from daily_quotes ;
	c_code varchar(10);
begin
	open cur_code ;
	loop fetch cur_code into c_code;
		exit when not found;

		with recursive day_9_value as (
			select data_dt ,security_code ,closing_price 
			 ,min(lowest_price) over (partition by security_code order by data_dt rows between 8 preceding and current row) as "lowest_price_9"
			 ,max(highest_price) over (partition by security_code order by data_dt rows between 8 preceding and current row) as "highest_price_9"
			 ,row_number() over(partition by security_code order by data_dt) as rn
			from daily_quotes
			where security_code = c_code
			 and closing_price > 0
			)
			,rsv as (
			select data_dt ,security_code ,rn ,highest_price_9 ,lowest_price_9 ,closing_price
			 ,case when highest_price_9 - lowest_price_9 = 0 then 0
			  else round((closing_price - lowest_price_9) / (highest_price_9 - lowest_price_9) ,4)
			  end as rsv_value
			from day_9_value
			)
			,kd (rn ,data_dt ,security_code ,rsv_value ,k_value ,d_value ) as (
			select rn ,data_dt ,security_code ,rsv_value ,0.5::float ,0.5::float 
			from rsv
			where rn = 9
			union all
			select b.rn ,b.data_dt, b.security_code ,b.rsv_value 
			 ,round((2::numeric/3::numeric) * a.k_value::numeric + (1::numeric/3::numeric) * b.rsv_value::numeric ,4)
			 ,round((2::numeric/3::numeric) * a.d_value::numeric + (1::numeric/3::numeric) * ( (2::numeric/3::numeric) * a.k_value::numeric + (1::numeric/3::numeric) * b.rsv_value::numeric ) ,4)
			from kd a, rsv b
			where a.rn + 1 = b.rn 
		)
		insert into daily_kd
		select data_dt ,security_code ,rsv_value ,k_value ,d_value
		from kd ;
	end loop;
end
$$
language 'plpgsql';


RSV計算方式：
(今日收盤價 - 最近九天的最低價)/(最近九天的最高價 - 最近九天最低價)

K值 是RSV 和前一日的 K的加權平均
K = 2/3 * (昨日K值) + 1/3 * (今日RSV)

D值 是K 和前一日的 D 的加權平均
D = 2/3 * (昨日D值) + 1/3 * (今日K值)

create or replace procedure sp_daily_kd(calc_date date) as 
$$
declare
	last_quote_date date ;
	last_kd_date date ;
begin
	select max(data_dt) into last_quote_date from stock_calendar where data_dt < calc_date and stock_data  = 1;
	select max(data_dt) into last_kd_date from daily_kd where data_dt < calc_date;
	
	if last_quote_date <> last_kd_date then
		return ;
	end if;
	
	with day_9_value as (
		select data_dt ,security_code ,closing_price 
		 ,min(lowest_price) over (partition by security_code order by data_dt rows between 8 preceding and current row) as "lowest_price_9"
		 ,max(highest_price) over (partition by security_code order by data_dt rows between 8 preceding and current row) as "highest_price_9"
		from daily_quotes
		where data_dt >= calc_date + interval '-30 day'
		 and data_dt <= calc_date
		 and closing_price > 0
		)
		,rsv as (
		select data_dt ,security_code ,highest_price_9 ,lowest_price_9 ,closing_price
		 ,case when highest_price_9 - lowest_price_9 = 0 then 0
		  else round((closing_price - lowest_price_9) / (highest_price_9 - lowest_price_9) ,4)
		  end as rsv_value
		from day_9_value
		where data_dt = calc_date
		)
		insert into daily_kd(data_dt ,security_code ,rsv_value ,k_value ,d_value)
		select x.data_dt ,x.security_code ,x.rsv_value 
		 ,round((2::numeric/3::numeric) * y.k_value + (1::numeric/3::numeric) * x.rsv_value ,4)
		 ,round((2::numeric/3::numeric) * y.d_value + (1::numeric/3::numeric) * ( (2::numeric/3::numeric) * y.k_value + (1::numeric/3::numeric) * x.rsv_value ) ,4)
		from rsv x join daily_kd y
		 on x.security_code = y.security_code
		where y.data_dt = last_kd_date ;
end
$$
language 'plpgsql';


