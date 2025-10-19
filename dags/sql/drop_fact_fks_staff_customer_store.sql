alter table [sakila_dwh].[fact_rental] drop constraint [fk_fact_rental_staff_key];
alter table [sakila_dwh].[fact_rental] drop constraint [fk_fact_rental_customer_key];
alter table [sakila_dwh].[fact_rental] drop constraint [fk_fact_rental_store_key];
alter table [sakila_dwh].[fact_rental] drop constraint [fk_fact_rental_rental_date_key];
alter table [sakila_dwh].[fact_rental] drop constraint [fk_fact_rental_rental_time_key];
alter table [sakila_dwh].[fact_rental] drop constraint [fk_fact_rental_return_date_key];