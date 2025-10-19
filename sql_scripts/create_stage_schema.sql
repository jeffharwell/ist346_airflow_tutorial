
--
-- Create the schema
--

create schema [stage];

--
-- Create the Staging Tables
--

CREATE TABLE [stage].[actor](
	[actor_id] [int] NOT NULL PRIMARY KEY,
	[first_name] [varchar](45) NOT NULL,
	[last_name] [varchar](45) NOT NULL,
	[last_update] [datetime] NOT NULL
);

CREATE TABLE [stage].[address](
	[address_id] [int] NOT NULL PRIMARY KEY,
	[address] [varchar](50) NOT NULL,
	[address2] [varchar](50) NULL,
	[district] [varchar](20) NOT NULL,
	[city_id] [int] NOT NULL,
	[postal_code] [varchar](10) NULL,
	[phone] [varchar](20) NOT NULL,
	[last_update] [datetime] NOT NULL
);

CREATE TABLE [stage].[category](
	[category_id] [tinyint] NOT NULL PRIMARY KEY,
	[name] [varchar](25) NOT NULL,
	[last_update] [datetime] NOT NULL
);

CREATE TABLE [stage].[city](
	[city_id] [int] NOT NULL PRIMARY KEY,
	[city] [varchar](50) NOT NULL,
	[country_id] [smallint] NOT NULL,
	[last_update] [datetime] NOT NULL
);

CREATE TABLE [stage].[country](
	[country_id] [smallint] NOT NULL PRIMARY KEY,
	[country] [varchar](50) NOT NULL,
	[last_update] [datetime] NULL
);

CREATE TABLE [stage].[customer](
	[customer_id] [int] NOT NULL PRIMARY KEY,
	[store_id] [int] NOT NULL,
	[first_name] [varchar](45) NOT NULL,
	[last_name] [varchar](45) NOT NULL,
	[email] [varchar](50) NULL,
	[address_id] [int] NOT NULL,
	[active] [char](1) NOT NULL,
	[create_date] [datetime] NOT NULL,
	[last_update] [datetime] NOT NULL
);

CREATE TABLE [stage].[film](
	[film_id] [int] NOT NULL PRIMARY KEY,
	[title] [varchar](255) NOT NULL,
	[description] [text] NULL,
	[release_year] [varchar](4) NULL,
	[language_id] [tinyint] NOT NULL,
	[original_language_id] [tinyint] NULL,
	[rental_duration] [tinyint] NOT NULL,
	[rental_rate] [decimal](4, 2) NOT NULL,
	[length] [smallint] NULL,
	[replacement_cost] [decimal](5, 2) NOT NULL,
	[rating] [varchar](10) NULL,
	[special_features] [varchar](255) NULL,
	[last_update] [datetime] NOT NULL
);

CREATE TABLE [stage].[film_actor](
	[actor_id] [int] NOT NULL PRIMARY KEY,
	[film_id] [int] NOT NULL,
	[last_update] [datetime] NOT NULL
);

CREATE TABLE [stage].[film_category](
	[film_id] [int] NOT NULL PRIMARY KEY,
	[category_id] [tinyint] NOT NULL,
	[last_update] [datetime] NOT NULL
);

CREATE TABLE [stage].[film_text](
	[film_id] [smallint] NOT NULL PRIMARY KEY,
	[title] [varchar](255) NOT NULL,
	[description] [text] NULL
);

CREATE TABLE [stage].[inventory](
	[inventory_id] [int] NOT NULL PRIMARY KEY,
	[film_id] [int] NOT NULL,
	[store_id] [int] NOT NULL,
	[last_update] [datetime] NOT NULL
);

CREATE TABLE [stage].[language](
	[language_id] [tinyint] NOT NULL PRIMARY KEY,
	[name] [char](20) NOT NULL,
	[last_update] [datetime] NOT NULL
);

CREATE TABLE [stage].[payment](
	[payment_id] [int] NOT NULL PRIMARY KEY,
	[customer_id] [int] NOT NULL,
	[staff_id] [tinyint] NOT NULL,
	[rental_id] [int] NULL,
	[amount] [decimal](5, 2) NOT NULL,
	[payment_date] [datetime] NOT NULL,
	[last_update] [datetime] NOT NULL
);

CREATE TABLE [stage].[rental](
	[rental_id] [int] IDENTITY(1,1) NOT NULL,
	[rental_date] [datetime] NOT NULL,
	[inventory_id] [int] NOT NULL,
	[customer_id] [int] NOT NULL,
	[return_date] [datetime] NULL,
	[staff_id] [tinyint] NOT NULL,
	[last_update] [datetime] NOT NULL
);

CREATE TABLE [stage].[staff](
	[staff_id] [tinyint] IDENTITY(1,1) NOT NULL,
	[first_name] [varchar](45) NOT NULL,
	[last_name] [varchar](45) NOT NULL,
	[address_id] [int] NOT NULL,
	[picture] [image] NULL,
	[email] [varchar](50) NULL,
	[store_id] [int] NOT NULL,
	[active] [bit] NOT NULL,
	[username] [varchar](16) NOT NULL,
	[password] [varchar](40) NULL,
	[last_update] [datetime] NOT NULL
);

CREATE TABLE [stage].[store](
	[store_id] [int] IDENTITY(1,1) NOT NULL,
	[manager_staff_id] [tinyint] NOT NULL,
	[address_id] [int] NOT NULL,
	[last_update] [datetime] NOT NULL
);

--
-- Create the Staging For the Dimensional Model
--
-- Note that we don't create the surrogate key here, that happens on insert 
-- into the [sakila_dwh] schema because of the IDENTITY() column on those tables

CREATE TABLE [stage].[dim_film] (
    -- note that we don't create the key, will will let the 
	-- identity column in the data warehouse do that
	[film_last_update] [datetime],
	[film_id] [int] NOT NULL,
	[film_title] [varchar](64) NOT NULL,
	[film_description] [text] NOT NULL,
	[film_release_year] [smallint] NOT NULL,
	[film_language] [varchar](20) NOT NULL,
	[film_original_language] [varchar](20) NOT NULL,
	[film_rental_duration] [tinyint] NOT NULL,
	[film_rental_rate] [decimal](4, 2) NOT NULL,
	[film_duration] [int] NOT NULL,
	[film_replacement_cost] [decimal](5, 2) NOT NULL,
	[film_rating_code] [char](5) NOT NULL,
	[film_rating_text] [varchar](30) NOT NULL,
	[film_has_trailers] [char](4) NOT NULL,
	[Film_has_commentaries] [char](4) NOT NULL,
	[Film_has_deleted_scenes] [char](4) NOT NULL,
	[Film_has_behind_the_scenes] [char](4) NOT NULL,
	[Film_in_category_action] [char](4) NOT NULL,
	[Film_in_category_animation] [char](4) NOT NULL,
	[Film_in_category_children] [char](4) NOT NULL,
	[Film_in_category_classics] [char](4) NOT NULL,
	[Film_in_category_comedy] [char](4) NOT NULL,
	[Film_in_category_documentary] [char](4) NOT NULL,
	[Film_in_category_drama] [char](4) NOT NULL,
	[Film_in_category_family] [char](4) NOT NULL,
	[Film_in_category_foreign] [char](4) NOT NULL,
	[Film_in_category_games] [char](4) NOT NULL,
	[Film_in_category_horror] [char](4) NOT NULL,
	[Film_in_category_music] [char](4) NOT NULL,
	[Film_in_category_new] [char](4) NOT NULL,
	[Film_in_category_scifi] [char](4) NOT NULL,
	[Film_in_category_sports] [char](4) NOT NULL,
	[Film_in_category_travel] [char](4) NOT NULL
);

CREATE TABLE [stage].[dim_actor] (
	[actor_last_update] [datetime],
	[actor_id] [int] NOT NULL,
	[actor_last_name] [varchar](45) NOT NULL,
	[actor_first_name] [varchar](45) NOT NULL
);

CREATE TABLE [stage].[dim_customer] (
	[customer_last_update] [datetime],
	[customer_id] [int] NOT NULL,
	[customer_first_name] [varchar](45) NOT NULL,
	[customer_last_name] [varchar](45) NOT NULL,
	[customer_email] [varchar](50) NOT NULL,
	[customer_active] [char](3) NOT NULL,
	[customer_created] [date] NOT NULL,
	[customer_address] [varchar](64) NOT NULL,
	[customer_district] [varchar](20) NOT NULL,
	[customer_postal_code] [varchar](10) NOT NULL,
	[customer_phone_number] [varchar](20) NOT NULL,
	[customer_city] [varchar](50) NOT NULL,
	[customer_country] [varchar](50) NOT NULL,
	[customer_version_number] [smallint] NOT NULL,
	[customer_valid_from] [date] NOT NULL,
	[customer_valid_through] [date] NOT NULL
);

CREATE TABLE [stage].[dim_film_actor_bridge](
	[film_key] [int] NOT NULL,
	[actor_key] [int] NOT NULL,
	[actor_weighting_factor] [decimal](3, 0) NOT NULL
);

CREATE TABLE [stage].[dim_staff](
	[staff_last_update] [datetime],
	[staff_id] [int] NOT NULL,
	[staff_first_name] [varchar](45) NOT NULL,
	[staff_last_name] [varchar](45) NOT NULL,
	[staff_store_id] [int] NOT NULL,
	[staff_version_number] [smallint] NOT NULL,
	[staff_valid_from] [date] NOT NULL,
	[staff_valid_through] [date] NOT NULL,
	[staff_active] [char](3) NOT NULL
);

CREATE TABLE [stage].[dim_store](
	[store_last_update] [datetime],
	[store_id] [int] NOT NULL,
	[store_address] [varchar](64) NOT NULL,
	[store_district] [varchar](20) NOT NULL,
	[store_postal_code] [varchar](10) NOT NULL,
	[store_phone_number] [varchar](20) NOT NULL,
	[store_city] [varchar](50) NOT NULL,
	[store_country] [varchar](50) NOT NULL,
	[store_manager_staff_id] [int] NOT NULL,
	[store_manager_first_name] [varchar](45) NOT NULL,
	[store_manager_last_name] [varchar](45) NOT NULL,
	[store_version_numbers] [smallint] NOT NULL,
	[store_valid_from] [date] NOT NULL,
	[store_valid_to] [date] NOT NULL
);

CREATE TABLE [stage].[fact_rental] (
	[customer_key] [int] NOT NULL,
	[staff_key] [int] NOT NULL,
	[film_key] [int] NOT NULL,
	[store_key] [int] NOT NULL,
	[rental_date_key] [int] NOT NULL,
	[return_date_key] [int] NOT NULL,
	[return_time_key] [int] NOT NULL,
	[count_returns] [int] NOT NULL,
	[count_rentals] [int] NOT NULL,
	[rental_duration] [int],
	[rental_last_update] [datetime],
	[rental_id] INT NOT NULL
);