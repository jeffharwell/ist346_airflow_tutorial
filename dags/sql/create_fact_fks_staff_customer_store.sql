--
-- (Re)create the constraints for the Staff, Customer, and Store dimensions.
-- 
ALTER TABLE [sakila_dwh].[fact_rental]  WITH CHECK ADD CONSTRAINT [fk_fact_rental_staff_key] FOREIGN KEY([staff_key])
REFERENCES [sakila_dwh].[dim_staff] ([staff_key]);
ALTER TABLE [sakila_dwh].[fact_rental]  WITH CHECK ADD CONSTRAINT [fk_fact_rental_customer_key] FOREIGN KEY([customer_key])
REFERENCES [sakila_dwh].[dim_customer] ([customer_key]);
ALTER TABLE [sakila_dwh].[fact_rental]  WITH CHECK ADD CONSTRAINT [fk_fact_rental_store_key] FOREIGN KEY([store_key])
REFERENCES [sakila_dwh].[dim_store] ([store_key]);

--
-- Re(create) the constrainst for the date and time dimensions.
-- 
ALTER TABLE [sakila_dwh].[fact_rental]  WITH CHECK ADD CONSTRAINT [fk_fact_rental_rental_date_key] FOREIGN KEY ([rental_date_key]) 
REFERENCES [sakila_dwh].[dim_date] ([date_key]);
ALTER TABLE [sakila_dwh].[fact_rental]  WITH CHECK ADD CONSTRAINT [fk_fact_rental_rental_time_key] FOREIGN KEY ([rental_time_key]) 
REFERENCES [sakila_dwh].[dim_time] ([time_key]);
ALTER TABLE [sakila_dwh].[fact_rental]  WITH CHECK ADD CONSTRAINT [fk_fact_rental_return_date_key] FOREIGN KEY ([return_date_key]) 
REFERENCES [sakila_dwh].[dim_date] ([date_key]);