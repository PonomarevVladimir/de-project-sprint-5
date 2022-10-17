with t as (select distinct order_ts::timestamp as ts from stg.apisystem_deliveries) 
insert into dds.dm_timestamps (ts, year, month, day, time, date) 
select t.ts, extract(year from t.ts), extract(month from t.ts), extract(day from t.ts), t.ts::time, t.ts::date from t;