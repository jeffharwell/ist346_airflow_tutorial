/*
 * Transform for the store dimension
 */

insert into [stage].[dim_store]
select [store].[last_update] as store_last_update,
       [store].[store_id] as store_id,
       -- collapse the address hierarchy into the customer dimension
       store_address = CASE
                    WHEN [address2] is NULL THEN [address]
                    ELSE [address] + ' ' + [address2]
                    END,
       [district] as store_distict,
       -- Business Logic allows postal code to be Null, replace that with N/A for Not Applicable
       COALESCE([postal_code], 'N/A') as store_postal_code,
       [phone] as store_phone_number,
       [city] as store_city,
       [country] as store_country,
       [staff].[staff_id] as store_manager_staff_id,
       [staff].[first_name] as store_manager_first_name,
       [staff].[last_name] as store_manager_last_name,
       -- since there is no historical data, set up the SCD for the current version
       1 as store_version_number,
       DATEFROMPARTS(2000,1,1) as store_valid_from,
       DATEFROMPARTS(2999,1,1) as store_valid_through
  from [stage].[store] -- join in the address hierachy so we can collapse it into the dimension
                    join [stage].[address] on [store].[address_id] = [address].[address_id]
                    join [stage].[city] on [address].[city_id] = [city].[city_id]
                    join [stage].[country] on [city].[country_id] = [country].[country_id]
                    -- join in the staff hierarchy
                    join [stage].[staff] on [store].[manager_staff_id] = [staff].[staff_id]