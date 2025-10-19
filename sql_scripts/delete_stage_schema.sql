
--
-- Drop the Staging Tables for the Transactional Database
--

DROP TABLE [stage].[actor];
DROP TABLE [stage].[address];
DROP TABLE [stage].[category];
DROP TABLE [stage].[city];
DROP TABLE [stage].[country];
DROP TABLE [stage].[customer];
DROP TABLE [stage].[film];
DROP TABLE [stage].[film_actor];
DROP TABLE [stage].[film_category];
DROP TABLE [stage].[film_text];
DROP TABLE [stage].[inventory];
DROP TABLE [stage].[language];
DROP TABLE [stage].[payment];
DROP TABLE [stage].[rental];
DROP TABLE [stage].[staff];
DROP TABLE [stage].[store];

--
-- Drop The Staging Table For the Dimensional Model
--

DROP TABLE [stage].[dim_film];
DROP TABLE [stage].[dim_actor];
DROP TABLE [stage].[dim_customer];
DROP TABLE [stage].[dim_film_actor_bridge];
DROP TABLE [stage].[dim_staff];
DROP TABLE [stage].[dim_store];
DROP TABLE [stage].[fact_rental];

--
-- Finally drop the schema
--

drop schema [stage];