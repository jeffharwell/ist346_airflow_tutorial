/*
 * Transform for dim_customer
 *
 * Collapses the address hierarchy into the dimension and sets up the type II SCD
 * fields.
 * This transform is designed as an initial load when there is no history in the 
 * source table.
 */
insert into [stage].[dim_customer]
select [customer].[last_update] as customer_last_update,
       [customer_id] as customer_id,
       [first_name] as customer_first_name,
       [last_name] as customer_last_name,
       [email] as customer_email,
       [active] as customer_active,
       [create_date] as customer_created,
       -- collapse the address hierarchy into the customer dimension
       customer_address = CASE
                    WHEN [address2] is NULL THEN [address]
                    ELSE [address] + ' ' + [address2]
                    END,
       [district] as customer_distict,
       [postal_code] as customer_postal_code,
       [phone] as customer_phone_number,
       [city] as customer_city,
       [country] as customer_country,
       -- for our initial load, since the source data has no previous versions
       1 as customer_version_number,
       [customer].[create_date] as customer_valid_from,
       DATEFROMPARTS(2999,1,1) as customer_valid_through
from [stage].[customer] -- join in the address hierachy so we can collapse it into the dimension
                        join [stage].[address] on [customer].[address_id] = [address].[address_id]
                        join [stage].[city] on [address].[city_id] = [city].[city_id]
                        join [stage].[country] on [city].[country_id] = [country].[country_id]