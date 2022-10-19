with a as (select row_number() over (partition by load_table order by current_offset desc), current_offset as co  from stg.loads_check where load_table = 'apisystem_couriers'),
b as (select a.co from a where a.row_number = 2),
c as (select row_number() over (), _id, name from stg.apisystem_couriers)
insert into dds.dm_couriers (courier_id, courier_name) 
select c._id, c.name from c, b where c.row_number > b.co;
