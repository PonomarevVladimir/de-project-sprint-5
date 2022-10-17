with a as (select order_id, order_ts::timestamp, courier_id, cast(rate as int), cast(sum as numeric(14,2)), cast(tip_sum as numeric(14,2)) from stg.apisystem_deliveries) 
insert into dds.dm_deliveries (order_id, courier_id, time_id, rate, sum, tip) 
select a.order_id, c.id, t.id, a.rate, a.sum, a.tip_sum from a 
inner join (select id, courier_id from dds.dm_couriers) c on a.courier_id = c.courier_id 
inner join (select id, ts from dds.dm_timestamps dt) t on a.order_ts = t.ts;