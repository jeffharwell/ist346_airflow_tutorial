/*
 * Transform statement for dim_staff
 */

insert into [stage].[dim_staff]
select [last_update] as staff_last_update,
       [staff_id] as staff_id,
       [first_name] as staff_first_name,
       [last_name] as staff_last_name,
       [store_id] as staff_store_id,
       -- no history in the source table
       -- set up the SCD fields appropriately
       1 as staff_version_number,
       DATEFROMPARTS(2000,1,1) as staff_valid_from,
       DATEFROMPARTS(2999,1,1) as staff_valid_through,
       [active] as staff_active
  from [stage].[staff];