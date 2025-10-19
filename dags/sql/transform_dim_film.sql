/*
 * SQL statement that transforms our stage data from the 'film_category', 'category',
 * 'language', and 'film' tables. The transformed data is then staged in [stage].[dim_film].
 */

-- Collapse our our film category -> category hierarchy using a common table expression
with film_category (film_id, category_name) as (
select [film_id], [name]
  from [stage].[film_category] join [stage].[category] 
                                on [film_category].[category_id] = [category].[category_id]
)
-- insert into dim_film
insert into [stage].[dim_film]
-- use this select statement to do the transform
select [film].[last_update] as film_last_update,
       [film].[film_id],
       -- use the title and description from the [film_text] table although it seems to be
       -- a perfect duplicate of the values in the [film] table.
       [film_text].[title] as film_title,
       [film_text].[description] as film_description,
       [release_year] as film_release_year,
       f_language.name as film_language,
       -- In the source data original_language_id can be NULL, replace with 'Not Applicable'
       COALESCE(o_language.name, 'Not Applicable') as film_original_language,
       [rental_duration] as film_rental_duration,
       [rental_rate] as film_rental_rate,
       [length] as film_duration,
       [replacement_cost] as film_replacement_cost,
       [rating] as film_rating_code,
       -- The case statement adding the rating text from the film rating
       film_rating_text = CASE
            WHEN [rating] = 'NC-17' THEN 'Adults Only'
            WHEN [rating] = 'PG' THEN 'Parental Guidance Suggested'
            WHEN [rating] = 'PG-13' THEN 'Parents Strongly Cautioned'
            WHEN [rating] = 'G' THEN 'General Audience'
            WHEN [rating] = 'R' THEN 'Restricted'
            ELSE 'Unknown Rating'
        END,
       -- use CASE statements to look for the strings representing the special features
       -- in the special_features column and populate our film_has_X columns
       film_has_trailers = CASE WHEN special_features like '%Trailers%' THEN 'YES' ELSE 'NO' END,
       film_has_commentaries = CASE WHEN special_features like '%Commentaries%' THEN 'YES' ELSE 'NO' END,
       film_has_deleted_scenes = CASE WHEN special_features like '%Deleted Scenes%' THEN 'YES' ELSE 'NO' END,
       film_has_behind_the_scenes = CASE WHEN special_features like '%Behind the Scenes%' THEN 'YES' ELSE 'NO' END,
       -- Populate our film_in_category_X using case statements to evaluate the category_name string
       -- from the film_category common table expression we defined to collapse the category hierarchy
       film_in_category_action = CASE WHEN [category_name] = 'Action' THEN 'YES' ELSE 'NO' END,
       film_in_category_animation = CASE WHEN [category_name] = 'Animation' THEN 'YES' ELSE 'NO' END,
       film_in_category_childen = CASE WHEN [category_name] = 'Children' THEN 'YES' ELSE 'NO' END,
       film_in_category_classics = CASE WHEN [category_name] = 'Classics' THEN 'YES' ELSE 'NO' END,
       film_in_category_comedy = CASE WHEN [category_name] = 'Comedy' THEN 'YES' ELSE 'NO' END,
       film_in_category_documentary = CASE WHEN [category_name] = 'Documentary' THEN 'YES' ELSE 'NO' END,
       film_in_category_drama = CASE WHEN [category_name] = 'Drama' THEN 'YES' ELSE 'NO' END,
       film_in_category_family = CASE WHEN [category_name] = 'Family' THEN 'YES' ELSE 'NO' END,
       film_in_category_foreign = CASE WHEN [category_name] = 'Foreign' THEN 'YES' ELSE 'NO' END,
       film_in_category_games = CASE WHEN [category_name] = 'Games' THEN 'YES' ELSE 'NO' END,
       film_in_category_horror = CASE WHEN [category_name] = 'Horror' THEN 'YES' ELSE 'NO' END,
       film_in_category_music = CASE WHEN [category_name] = 'Music' THEN 'YES' ELSE 'NO' END,
       film_in_category_new = CASE WHEN [category_name] = 'New' THEN 'YES' ELSE 'NO' END,
       film_in_category_scifi = CASE WHEN [category_name] = 'Sci-Fi' THEN 'YES' ELSE 'NO' END,
       film_in_category_sports = CASE WHEN [category_name] = 'Sports' THEN 'YES' ELSE 'NO' END,
       film_in_category_travel = CASE WHEN [category_name] = 'Travel' THEN 'YES' ELSE 'NO' END
  from [stage].[film] -- there is some duplicated data here, but let's pull from the normalized tables
                      -- in real life we would call the DBA and ask what is going on here
                      join [stage].[film_text] on [film_text].[film_id] = [film].[film_id]
                      -- join the film_category common table expression we declared earlier using 'with'
                      join film_category on [film].[film_id] = film_category.[film_id]
                      -- language_id can't be null, so we can use a regular inner join
                      join [stage].[language] as f_language on [film].[language_id] = f_language.[language_id]
                      -- original_language_id can be null, so we must use a left outer join
                      left outer join [stage].[language] as o_language on [film].[original_language_id] = o_language.[language_id]