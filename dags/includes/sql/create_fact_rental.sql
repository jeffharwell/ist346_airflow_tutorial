/*
 * SQL to creates the fact Rental fact table
 */

-- Common Table Expression to Fetch the Film Key
with inventory_to_film (inventory_id, film_key) as (
    select inventory_id, film_key
      from [stage].[inventory] inv join [sakila_dwh].[dim_film] df on inv.film_id = df.film_id 
),
-- Get the store key from the staff id
staff_and_store_keys (staff_id, staff_key, store_key) as (
    select s_staff.staff_id, dw_staff.staff_key, dw_store.store_key
      from [stage].[staff] s_staff join [sakila_dwh].[dim_staff] dw_staff on s_staff.staff_id = dw_staff.staff_id
                                   join [sakila_dwh].[dim_store] dw_store on s_staff.store_id = dw_store.store_id
)
insert into [stage].[fact_rental]
select
  -- Get the customer ID from dim_customer
  (select customer_key
     from [sakila_dwh].[dim_customer] dwdc 
    where dwdc.[customer_id] = [rental].[customer_id]) as customer_key,
  staff_key, -- from CTA
  film_key,  -- from CTA
  store_key, -- from CTA
  -- get the rental date key from dim_date using the date portion of rental_date
  (select date_key
     from [sakila_dwh].[dim_date]
    where [dim_date].[date_value] = CAST([rental].[rental_date] as date)) as rental_date_key,
  -- get the return date key from dim_date
  -- if it is null they have not returned the rental, so use -1
  -- which is the date_key for a unknown date
  CASE WHEN [rental].[return_date] is null
       THEN -1
       ELSE
        (select date_key
            from [sakila_dwh].[dim_date]
            where [dim_date].[date_value] = CAST([rental].[return_date] as date))
  END as return_date_key,
  -- get the rental time key from dim_time using the time portion of rental_time
  -- you need the max() because 00:00:00 is also the time for the invalid
  -- time row that we use for a unknown time.
  (select max(time_key)
     from [sakila_dwh].[dim_time]
    where [dim_time].[time_value] = CAST([rental].[rental_date] as time(0))) as rental_time_key,
  -- If there is no return, this is 0, but if it has been returned this is 1
  CASE
    WHEN [rental].[return_date] is null
    THEN 0 else 1
  END as count_return,
  -- this is a kind of factless fact table, we are just reporting that a rental happened
  1 as count_rentals,
  -- Similarly, if there is no return this is null, otherwise it is the 
  -- duration of the rental in seconds as calculated by DATEDIFF
  CASE
    WHEN [rental].[return_date] is NULL
    THEN null
    ELSE DATEDIFF(ss, [rental].[rental_date], [rental].[return_date])
  END as rental_duration,
  last_update as rental_last_update, -- get this from our source table
  rental_id
  -- rental is a our base table, then join in our two common table expressions
  from [stage].[rental] join inventory_to_film itf on itf.[inventory_id] = [rental].[inventory_id]
                        join staff_and_store_keys sask on sask.[staff_id] = [rental].[staff_id]