-- sp_daily_quotes_statistics
create or replace procedure sp_daily_quotes_statistics(calc_date date) as 
$$
declare
	calendar smallint ;
	exec_target smallint ;
	executed smallint ;
begin
	select stock_data ,ma into calendar ,exec_target
	from stock_calendar 
	where data_dt = calc_date ;
	
	select 1 into executed 
	from daily_quotes_statistics
	where data_dt = calc_date
	limit 1 ;
	if not found then
		executed := 0;
	end if;
	
	if calendar = 1 and exec_target = 0 and executed = 0 then
		insert into daily_quotes_statistics
		select *
        from (
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
            from daily_quotes
            where data_dt >= calc_date + interval '-90 day'
            and data_dt <= calc_date
            and closing_price > 0
        ) x
        where data_dt = calc_date ;
		
		update stock_calendar set ma = 1 where data_dt = calc_date ;
	end if;
end
$$
language 'plpgsql';

-- sp_daily_kd
create or replace procedure sp_daily_kd(calc_date date) as 
$$
declare
	calendar smallint ;
	exec_target smallint ;
	executed smallint ;
	last_kd_date date ;
begin
	select max(data_dt) into last_kd_date from daily_kd where data_dt < calc_date;
	
	select stock_data ,kd into calendar ,exec_target
	from stock_calendar 
	where data_dt = calc_date ;
	
	select 1 into executed 
	from daily_kd
	where data_dt = calc_date
	limit 1 ;
	if not found then
		executed := 0;
	end if;

	if calendar = 1 and exec_target = 0 and executed = 0 then
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
		
		update stock_calendar set kd = 1 where data_dt = calc_date ;
	end if;
end
$$
language 'plpgsql';

-- sp_daily_bband
create or replace procedure sp_daily_bband(calc_date date) as 
$$
declare
	calendar smallint ;
	exec_target smallint ;
	executed smallint ;
begin
	select stock_data ,bband into calendar ,exec_target
	from stock_calendar 
	where data_dt = calc_date ;
	
	select 1 into executed 
	from daily_bband
	where data_dt = calc_date
	limit 1 ;
	if not found then
		executed := 0;
	end if;
	if calendar = 1 and exec_target = 0 and executed = 0 then
		with bband as (
			select data_dt ,security_code
			,closing_price
			,round(avg(closing_price) over (partition by security_code order by data_dt rows between 19 preceding and current row) ,2) as "closing_price_avg_20"
			,round(stddev(closing_price) over (partition by security_code order by data_dt rows between 19 preceding and current row) ,2) as "closing_price_sigma_20"
			from daily_quotes
			where data_dt >= calc_date + interval '-40 day'
			and data_dt <= calc_date
			and closing_price > 0
			),
			bband_wide as (
			select data_dt ,security_code
			,(closing_price_avg_20 + 2 * closing_price_sigma_20) as bband_top
			,(closing_price_avg_20 - 2 * closing_price_sigma_20) as bband_bot
			from bband
			where data_dt = calc_date
			)
		insert into daily_bband
		select x.data_dt ,x.security_code
		,y.bband_top 
		,x.closing_price_avg_20 as bband_mid
		,y.bband_bot
		,round(case when (y.bband_top - y.bband_bot) = 0 then 0 else (x.closing_price - y.bband_bot) / (y.bband_top - y.bband_bot) end ,2) as bband_percent
		,round((y.bband_top - y.bband_bot) / x.closing_price ,2) as bband_width
		from bband x join bband_wide y
		on x.data_dt = y.data_dt and x.security_code = y.security_code ;
		
		update stock_calendar set bband = 1 where data_dt = calc_date ;
	end if;
end
$$
language 'plpgsql';


