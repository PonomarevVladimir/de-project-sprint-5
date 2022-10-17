insert into dds.dm_couriers (courier_id, courier_name) 
select distinct on (_id) * from stg.apisystem_couriers;