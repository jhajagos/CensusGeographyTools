--create schema acs_computed;

/* Build tables with basic demographic parameters */

/* NY Zip Codes Language */

/* By Zip */

drop table if exists acs_computed.ny_zip_language_2013;
create table acs_computed.ny_zip_language_2013 as
  select  distinct t.geo_name, t.table_id, 
    t.table_name, t.subject_area,
    t.geoid as full_geoid,
    t.geoid_tiger,
    t.geoid_tiger as zip5,
    sezc.city, 
    t.numeric_value as total_households, 
    t.spanish_language_households, 
    t.fraction_spanish_language_households, 
    t.english_language_households, 
    t.fraction_english_language_households
    from spatial.tl_2013_us_zcta510 nbt join 
    (select e1.*, e2.numeric_value as spanish_language_households, 
      case when e1.numeric_value > 0 then e2.numeric_value / e1.numeric_value else null end as fraction_spanish_language_households,
      e3.numeric_value as english_language_households,
      case when e1.numeric_value > 0 then e3.numeric_value / e1.numeric_value else null end fraction_english_language_households
      from acs_summary_us."e20135usB16002" e1 join acs_summary_us."e20135usB16002" e2 on e1.geoid = e2.geoid and e1.relative_position = 1
        and e2.relative_position = 3
        join acs_summary_us."e20135usB16002" e3 on e3.relative_position = 2 and e1.geoid = e3.geoid and e1.relative_position = 1) t
        on t.geoid_tiger = nbt.geoid10 and t.geo_name like 'ZCTA5%'
        join spatial.us_counties uc on /*uc.name = */ uc.statefp = '36' and public.St_Intersects(uc.geom, nbt.geom)
        join spatial.sparcs_empirical_zip_city sezc on sezc.zip5 = t.geoid_tiger;
      
/* By Tract and Block Group */

drop table if exists  acs_computed.ny_bg_tract_language_2013;
create table acs_computed.ny_bg_tract_language_2013 as
  select  distinct t.geo_name, t.table_id, 
    t.table_name, t.subject_area,
    t.geoid as full_geoid,
    t.geoid_tiger, 
    t.numeric_value as total_households, 
    t.spanish_language_households, 
    t.fraction_spanish_language_households, 
    t.english_language_households, 
    t.fraction_english_language_households from (
  select e1.*, e2.numeric_value as spanish_language_households, 
      case when e1.numeric_value > 0 then e2.numeric_value / e1.numeric_value else null end as fraction_spanish_language_households,
      e3.numeric_value as english_language_households,
      case when e1.numeric_value > 0 then e3.numeric_value / e1.numeric_value else null end fraction_english_language_households
      from acs_summary_ny."e20135nyB16002" e1 join acs_summary_ny."e20135nyB16002" e2 on e1.geoid = e2.geoid and e1.relative_position = 1
        and e2.relative_position = 3
        join acs_summary_ny."e20135nyB16002" e3 on e3.relative_position = 2 and e1.geoid = e3.geoid and e1.relative_position = 1) t;

/* By Tract */
drop table if exists  acs_computed.ny_tract_language_2013 ;
create table  acs_computed.ny_tract_language_2013 as
  select t.* from acs_computed.ny_bg_tract_language_2013 t join spatial.tl_2013_36_tract bg on t.geoid_tiger = bg.geoid;

/* By Block Group */

drop table if exists  acs_computed.ny_bg_language_2013;
create table  acs_computed.ny_bg_language_2013 as
  select t.* from  acs_computed.ny_bg_tract_language_2013 t join spatial.tl_2013_36_bg bg on t.geoid_tiger = bg.geoid;


/* Population Density */


drop table if exists  acs_computed.ny_zip_density_2013;
create table acs_computed.ny_zip_density_2013 as
  select distinct t.*,t.geoid_tiger as zip5, sezc.city,
     nbt.aland10 as land_area_square_meters,
     case when nbt.aland10 > 0 then 1e6 * (total_population / nbt.aland10)  
     else null end as population_density_per_square_kilometer
  from (
    select  geo_name, table_id, table_name, subject_area, geoid as full_geoid,
      geoid_tiger, numeric_value as total_population    from acs_summary_us."e20135usB01003") t
      join  spatial.tl_2013_us_zcta510 nbt on t.geoid_tiger = nbt.geoid10 and t.geo_name like 'ZCTA5%'
      join spatial.us_counties uc on  uc.statefp = '36' and public.St_Intersects(uc.geom, nbt.geom) 
      join spatial.sparcs_empirical_zip_city sezc on sezc.zip5 = t.geoid_tiger;
      


/* By Block Group */
drop table if exists  acs_computed.ny_bg_density_2013;
create table acs_computed.ny_bg_density_2013 as
  select  geo_name, table_id, table_name, subject_area, p.geoid as full_geoid,
    geoid_tiger, numeric_value as total_population, tb.aland as land_area_square_meters,
    case when tb.aland > 0 then 1e6 * (numeric_value / tb.aland)  else null end as population_density_per_square_kilometer
    from acs_summary_ny."e20135nyB01003" p
    join spatial.tl_2013_36_bg tb on tb.geoid = p.geoid_tiger 
  ;

/* By Tract */
drop table if exists  acs_computed.ny_tract_density_2013;
create table acs_computed.ny_tract_density_2013 as
  select  geo_name, table_id, table_name, subject_area, p.geoid as full_geoid,
    geoid_tiger, numeric_value as total_population, tb.aland as land_area_square_meters,
    case when tb.aland > 0 then 1e6 * (numeric_value / tb.aland)  else null end as population_density_per_square_kilometer
    from acs_summary_ny."e20135nyB01003" p
    join spatial.tl_2013_36_tract tb on tb.geoid = p.geoid_tiger ;
    
/* Median Age */

/* Zip */

drop table if exists acs_computed.ny_zip_median_age_2013;
create table acs_computed.ny_zip_median_age_2013 as
select distinct geo_name, table_id, table_name, subject_area, t.geoid as full_geoid,
      geoid_tiger, numeric_value as median_age, t.geoid_tiger as zip5, sezc.city from acs_summary_us."e20135usB01002" t
 join  spatial.tl_2013_us_zcta510 nbt on t.geoid_tiger = nbt.geoid10 and t.geo_name like 'ZCTA5%'
 join spatial.us_counties uc on  uc.statefp = '36' and public.St_Intersects(uc.geom, nbt.geom) 
 join spatial.sparcs_empirical_zip_city sezc on sezc.zip5 = t.geoid_tiger
  where relative_position = 1;
      
      ;
/* Tract */

drop table if exists acs_computed.ny_tract_median_age_2013;
create table acs_computed.ny_tract_median_age_2013 as
  select geo_name, table_id, table_name, subject_area, t.geoid as full_geoid,
      geoid_tiger, numeric_value as median_age from acs_summary_ny."e20135nyB01002" t
      join spatial.tl_2013_36_tract tr on t.geoid_tiger = tr.geoid
      where relative_position = 1;

/* Block Group */       
drop table if exists acs_computed.ny_bg_median_age_2013;
create table acs_computed.ny_bg_median_age_2013 as
  select geo_name, table_id, table_name, subject_area, t.geoid as full_geoid,
      geoid_tiger, numeric_value as median_age from acs_summary_ny."e20135nyB01002" t
      join spatial.tl_2013_36_bg tr on t.geoid_tiger = tr.geoid
      where relative_position = 1;
      
/* Median Household Income */

/* Zip */

drop table if exists acs_computed.ny_zip_median_household_income_2013;
create table acs_computed.ny_zip_median_household_income_2013 as
select distinct geo_name, table_id, table_name, subject_area, t.geoid as full_geoid,
      geoid_tiger, numeric_value as median_household_income_2013, t.geoid_tiger as zip5, sezc.city 
      from acs_summary_us."e20135usB19013" t
 join  spatial.tl_2013_us_zcta510 nbt on t.geoid_tiger = nbt.geoid10 and t.geo_name like 'ZCTA5%'
 join spatial.us_counties uc on  uc.statefp = '36' and public.St_Intersects(uc.geom, nbt.geom) 
 join spatial.sparcs_empirical_zip_city sezc on sezc.zip5 = t.geoid_tiger
  where relative_position = 1;
      
 
/* Tract */
drop table if exists acs_computed.ny_tract_median_household_income_2013;
create table acs_computed.ny_tract_median_household_income_2013 as
  select geo_name, table_id, table_name, subject_area, t.geoid as full_geoid,
      geoid_tiger, numeric_value as median_household_income_2013  from acs_summary_ny."e20135nyB19013" t
      join spatial.tl_2013_36_tract tr on t.geoid_tiger = tr.geoid
      where relative_position = 1;

/* Block Group */       

drop table if exists acs_computed.ny_bg_median_household_income_2013;
create table acs_computed.ny_bg_median_household_income_2013 as
  select geo_name, table_id, table_name, subject_area, t.geoid as full_geoid,
      geoid_tiger, numeric_value as median_household_income_2013 from acs_summary_ny."e20135nyB19013" t
      join spatial.tl_2013_36_bg tr on t.geoid_tiger = tr.geoid
      where relative_position = 1;
      
      
/* Per-Capita Income */

/* Zip */

drop table if exists acs_computed.ny_zip_per_capita_income_2013;
create table acs_computed.ny_zip_per_capita_income_2013 as
select distinct geo_name, table_id, table_name, subject_area, t.geoid as full_geoid,
      geoid_tiger, numeric_value as per_capita_income_2013, t.geoid_tiger as zip5, sezc.city 
      from acs_summary_us."e20135usB19301" t
 join  spatial.tl_2013_us_zcta510 nbt on t.geoid_tiger = nbt.geoid10 and t.geo_name like 'ZCTA5%'
 join spatial.us_counties uc on  uc.statefp = '36' and public.St_Intersects(uc.geom, nbt.geom) 
 join spatial.sparcs_empirical_zip_city sezc on sezc.zip5 = t.geoid_tiger
  where relative_position = 1;
      
 
/* Tract */
drop table if exists acs_computed.ny_tract_per_capita_income_2013;
create table acs_computed.ny_tract_per_capita_income_2013 as
  select geo_name, table_id, table_name, subject_area, t.geoid as full_geoid,
      geoid_tiger, numeric_value as per_capita_income_2013  from acs_summary_ny."e20135nyB19301" t
      join spatial.tl_2013_36_tract tr on t.geoid_tiger = tr.geoid
      where relative_position = 1;

/* Block Group */       

drop table if exists acs_computed.ny_bg_per_capita_income_2013;
create table acs_computed.ny_bg_per_capita_income_2013 as
  select geo_name, table_id, table_name, subject_area, t.geoid as full_geoid,
      geoid_tiger, numeric_value as per_capita_income_2013 from acs_summary_ny."e20135nyB19301" t
      join spatial.tl_2013_36_bg tr on t.geoid_tiger = tr.geoid
      where relative_position = 1;

/* Household SNAP and Disabilities */

select distinct relative_position, field_name, context_path from acs_summary_ny."e20135nyB22010" t1;
/*

1	Total	
2	Household received Food Stamps/SNAP in the past 12 months	Total
3	Households with 1 or more persons with a disability	Total|Household received Food Stamps/SNAP in the past 12 months
4	Households with no persons with a disability	Total|Household received Food Stamps/SNAP in the past 12 months
5	Household did not receive Food Stamps/SNAP in the past 12 months	Total|Household received Food Stamps/SNAP in the past 12 months
6	Households with 1 or more persons with a disability	Total|Household did not receive Food Stamps/SNAP in the past 12 months
7	Households with no persons with a disability	Total|Household did not receive Food Stamps/SNAP in the past 12 months
*/

/* Zip */
drop table if exists acs_computed.ny_zip_snap_2013 ;
create table acs_computed.ny_zip_snap_2013 as
  select distinct tt.*, sezc.city from (
    select t.*, 
        case when t.total_households > 0 then total_households_receiving_snap / total_households end as fraction_receiving_snap,
        case when t.total_households > 0 then total_households_with_one_disability / total_households end as fraction_with_one_disability
        from (
        select t1.geoid as full_geoid, t1.geo_name, t1.table_id, t1.table_name, t1.subject_area, t1.geoid_tiger,
          t1.numeric_value as total_households, t2.numeric_value as total_households_receiving_snap, 
          t3.numeric_value as total_households_with_one_disability
          from acs_summary_us."e20135usB22010" t1 
          join acs_summary_us."e20135usB22010" t2 on t1.geoid = t2.geoid
          join acs_summary_us."e20135usB22010" t3 on t1.geoid = t3.geoid
        where t1.relative_position = 1 and t2.relative_position = 2 and t3.relative_position = 3) t) tt
        join  spatial.tl_2013_us_zcta510 nbt on tt.geoid_tiger = nbt.geoid10 and tt.geo_name like 'ZCTA5%'
        join spatial.us_counties uc on  uc.statefp = '36' and public.St_Intersects(uc.geom, nbt.geom) 
        join spatial.sparcs_empirical_zip_city sezc on sezc.zip5 = tt.geoid_tiger
        ; 


drop table if exists acs_computed.ny_bg_tract_snap_2013 ;
create table acs_computed.ny_bg_tract_snap_2013 as
  select *, 
    case when t.total_households > 0 then total_households_receiving_snap / total_households end as fraction_receiving_snap,
    case when t.total_households > 0 then total_households_with_one_disability / total_households end as fraction_with_one_disability
    from (
    select t1.geoid as full_geoid, t1.geo_name, t1.table_id, t1.table_name, t1.subject_area, t1.geoid_tiger,
      t1.numeric_value as total_households, t2.numeric_value as total_households_receiving_snap, 
      t3.numeric_value as total_households_with_one_disability
      from acs_summary_ny."e20135nyB22010" t1 
      join acs_summary_ny."e20135nyB22010" t2 on t1.geoid = t2.geoid
      join acs_summary_ny."e20135nyB22010" t3 on t1.geoid = t3.geoid
    where t1.relative_position = 1 and t2.relative_position = 2 and t3.relative_position = 3) t; 

/* By Tract */
drop table if exists  acs_computed.ny_tract_snap_2013 ;
create table  acs_computed.ny_tract_snap_2013 as
  select t.* from acs_computed.ny_bg_tract_snap_2013 t join spatial.tl_2013_36_tract bg on t.geoid_tiger = bg.geoid;

/* By Block Group */

drop table if exists  acs_computed.ny_bg_snap_2013 ;
create table acs_computed.ny_bg_snap_2013  as
  select t.* from  acs_computed.ny_bg_tract_snap_2013 t join spatial.tl_2013_36_bg bg on t.geoid_tiger = bg.geoid;
  
select distinct relative_position, field_name, context_path from  acs_summary_ny."e20135nyB02001"
order by relative_position
;
/*
1	Total	
2	White alone	Total
3	Black or African American alone	Total
4	American Indian and Alaska Native alone	Total
5	Asian alone	Total
6	Native Hawaiian and Other Pacific Islander alone	Total
7	Some other race alone	Total
8	Two or more races	Total
9	Two races including Some other race	Total|Two or more races
10	Two races excluding Some other race, and three or more races	Total|Two or more races
*/

/* Zip */

drop table if exists acs_computed.ny_zip_race_2013;
create table acs_computed.ny_zip_race_2013 as
  select distinct tt.*, sezc.city from
   (select *,
          case when total > 0 then total_white / total end as fraction_population_white,
          case when total > 0 then total_african_american / total end as fraction_population_african_american,
          case when total > 0 then total_native_american / total end as fraction_population_native_american,
          case when total > 0 then total_asian_american / total end as fraction_population_asian_american,
          case when total > 0 then total_two_or_more_races / total end as fraction_population_two_or_more_races
        from (
        select t1.geoid as full_geoid, t1.geo_name, t1.table_id, t1.table_name, t1.subject_area, t1.geoid_tiger,
          t1.numeric_value as total, 
          t2.numeric_value as total_white,
          t3.numeric_value as total_african_american,
          t4.numeric_value as total_native_american,
          t5.numeric_value as total_asian_american,
          t8.numeric_value as total_two_or_more_races
          from acs_summary_us."e20135usB02001" t1 
          join acs_summary_us."e20135usB02001" t2 on t1.geoid = t2.geoid
          join acs_summary_us."e20135usB02001" t3 on t1.geoid = t3.geoid
          join acs_summary_us."e20135usB02001" t4 on t1.geoid = t4.geoid
          join acs_summary_us."e20135usB02001" t5 on t1.geoid = t5.geoid
          join acs_summary_us."e20135usB02001" t8 on t1.geoid = t8.geoid
        where t1.relative_position = 1 and t2.relative_position = 2 and t3.relative_position = 3
        and t4.relative_position = 4 and t5.relative_position = 5 and t8.relative_position = 8
        ) t) tt
        join  spatial.tl_2013_us_zcta510 nbt on tt.geoid_tiger = nbt.geoid10 and tt.geo_name like 'ZCTA5%'
        join spatial.us_counties uc on  uc.statefp = '36' and public.St_Intersects(uc.geom, nbt.geom) 
        join spatial.sparcs_empirical_zip_city sezc on sezc.zip5 = tt.geoid_tiger
        ; 




drop table if exists acs_computed.ny_bg_tract_race_2013;
create table acs_computed.ny_bg_tract_race_2013 as
  select *,
        case when total > 0 then total_white / total end as fraction_population_white,
        case when total > 0 then total_african_american / total end as fraction_population_african_american,
        case when total > 0 then total_native_american / total end as fraction_population_native_american,
        case when total > 0 then total_asian_american / total end as fraction_population_asian_american,
        case when total > 0 then total_two_or_more_races / total end as fraction_population_two_or_more_races
      from (
      select t1.geoid as full_geoid, t1.geo_name, t1.table_id, t1.table_name, t1.subject_area, t1.geoid_tiger,
        t1.numeric_value as total, 
        t2.numeric_value as total_white,
        t3.numeric_value as total_african_american,
        t4.numeric_value as total_native_american,
        t5.numeric_value as total_asian_american,
        t8.numeric_value as total_two_or_more_races
        from acs_summary_ny."e20135nyB02001" t1 
        join acs_summary_ny."e20135nyB02001" t2 on t1.geoid = t2.geoid
        join acs_summary_ny."e20135nyB02001" t3 on t1.geoid = t3.geoid
        join acs_summary_ny."e20135nyB02001" t4 on t1.geoid = t4.geoid
        join acs_summary_ny."e20135nyB02001" t5 on t1.geoid = t5.geoid
        join acs_summary_ny."e20135nyB02001" t8 on t1.geoid = t8.geoid
      where t1.relative_position = 1 and t2.relative_position = 2 and t3.relative_position = 3
      and t4.relative_position = 4 and t5.relative_position = 5 and t8.relative_position = 8
      ) t; 
  
/* By Tract */
drop table if exists  acs_computed.ny_tract_race_2013 ;
create table  acs_computed.ny_tract_race_2013 as
  select t.* from acs_computed.ny_bg_tract_race_2013 t join spatial.tl_2013_36_tract bg on t.geoid_tiger = bg.geoid;

/* By Block Group */
drop table if exists  acs_computed.ny_bg_race_2013 ;
create table acs_computed.ny_bg_race_2013  as
  select t.* from  acs_computed.ny_bg_tract_race_2013 t join spatial.tl_2013_36_bg bg on t.geoid_tiger = bg.geoid;
    
  
select distinct relative_position, field_name, context_path from acs_summary_ny."e20135nyB03003" order by relative_position;
/*
1	Total	
2	Not Hispanic or Latino	Total
3	Hispanic or Latino	Total
*/

drop table if exists acs_computed.ny_zip_hispanic_2013;
create table acs_computed.ny_zip_hispanic_2013 as
  select distinct tt.*, sezc.city from
   (  select *, 
      case when t.total_population > 0 then total_population_non_hispanic / total_population end as fraction_population_non_hispanic,
      case when t.total_population > 0 then total_population_hispanic / total_population end as fraction_population_hispanic
      from (
      select t1.geoid as full_geoid, t1.geo_name, t1.table_id, t1.table_name, t1.subject_area, t1.geoid_tiger,
        t1.numeric_value as total_population, t2.numeric_value as total_population_non_hispanic,
        t3.numeric_value as total_population_hispanic
        from acs_summary_us."e20135usB03003" t1 
        join acs_summary_us."e20135usB03003" t2 on t1.geoid = t2.geoid
        join acs_summary_us."e20135usB03003" t3 on t1.geoid = t3.geoid
      where t1.relative_position = 1 and t2.relative_position = 2 and t3.relative_position = 3) t) tt
        join  spatial.tl_2013_us_zcta510 nbt on tt.geoid_tiger = nbt.geoid10 and tt.geo_name like 'ZCTA5%'
        join spatial.us_counties uc on  uc.statefp = '36' and public.St_Intersects(uc.geom, nbt.geom) 
        join spatial.sparcs_empirical_zip_city sezc on sezc.zip5 = tt.geoid_tiger
        ; 



drop table if exists acs_computed.ny_bg_tract_hispanic_2013;
create table acs_computed.ny_bg_tract_hispanic_2013 as
  select *, 
      case when t.total_population > 0 then total_population_non_hispanic / total_population end as fraction_population_non_hispanic,
      case when t.total_population > 0 then total_population_hispanic / total_population end as fraction_population_hispanic
      from (
      select t1.geoid as full_geoid, t1.geo_name, t1.table_id, t1.table_name, t1.subject_area, t1.geoid_tiger,
        t1.numeric_value as total_population, t2.numeric_value as total_population_non_hispanic,
        t3.numeric_value as total_population_hispanic
        from acs_summary_ny."e20135nyB03003" t1 
        join acs_summary_ny."e20135nyB03003" t2 on t1.geoid = t2.geoid
        join acs_summary_ny."e20135nyB03003" t3 on t1.geoid = t3.geoid
      where t1.relative_position = 1 and t2.relative_position = 2 and t3.relative_position = 3) t; 


/* By Tract */
drop table if exists  acs_computed.ny_tract_hispanic_2013 ;
create table  acs_computed.ny_tract_hispanic_2013 as
  select t.* from acs_computed.ny_bg_tract_hispanic_2013 t join spatial.tl_2013_36_tract bg on t.geoid_tiger = bg.geoid;

/* By Block Group */
drop table if exists  acs_computed.ny_bg_hispanic_2013 ;
create table acs_computed.ny_bg_hispanic_2013  as
  select t.* from  acs_computed.ny_bg_tract_hispanic_2013 t join spatial.tl_2013_36_bg bg on t.geoid_tiger = bg.geoid;
  


select distinct relative_position, field_name, context_path from acs_summary_ny."e20135nyB25003" order by relative_position;
/* 
1	Total	
2	Owner occupied	Total
3	Renter occupied	Total
*/


/* Zip */

drop table if exists acs_computed.ny_zip_own_rent_2013;
create table acs_computed.ny_zip_own_rent_2013 as
  select distinct tt.*, sezc.city from
   (  select *, 
    case when t.total_households > 0 then total_households_owner_occupied / total_households end as fraction_households_owner_occupied,
    case when t.total_households > 0 then total_households_renter_occupied / total_households end as fraction_households_renter_occupied
    from (
      select t1.geoid as full_geoid, t1.geo_name, t1.table_id, t1.table_name, t1.subject_area, t1.geoid_tiger,
        t1.numeric_value as total_households, t2.numeric_value as total_households_owner_occupied, 
        t3.numeric_value as total_households_renter_occupied
        from acs_summary_us."e20135usB25003" t1 
        join acs_summary_us."e20135usB25003" t2 on t1.geoid = t2.geoid
        join acs_summary_us."e20135usB25003" t3 on t1.geoid = t3.geoid
    where t1.relative_position = 1 and t2.relative_position = 2 and t3.relative_position = 3) t
   
   ) tt
        join  spatial.tl_2013_us_zcta510 nbt on tt.geoid_tiger = nbt.geoid10 and tt.geo_name like 'ZCTA5%'
        join spatial.us_counties uc on  uc.statefp = '36' and public.St_Intersects(uc.geom, nbt.geom) 
        join spatial.sparcs_empirical_zip_city sezc on sezc.zip5 = tt.geoid_tiger
        ; 

drop table if exists acs_computed.ny_bg_tract_own_rent_2013;
create table acs_computed.ny_bg_tract_own_rent_2013 as 
  select *, 
    case when t.total_households > 0 then total_households_owner_occupied / total_households end as fraction_households_owner_occupied,
    case when t.total_households > 0 then total_households_renter_occupied / total_households end as fraction_households_renter_occupied
    from (
      select t1.geoid as full_geoid, t1.geo_name, t1.table_id, t1.table_name, t1.subject_area, t1.geoid_tiger,
        t1.numeric_value as total_households, t2.numeric_value as total_households_owner_occupied, 
        t3.numeric_value as total_households_renter_occupied
        from acs_summary_ny."e20135nyB25003" t1 
        join acs_summary_ny."e20135nyB25003" t2 on t1.geoid = t2.geoid
        join acs_summary_ny."e20135nyB25003" t3 on t1.geoid = t3.geoid
    where t1.relative_position = 1 and t2.relative_position = 2 and t3.relative_position = 3) t; 

/* By Tract */
drop table if exists  acs_computed.ny_tract_own_rent_2013 ;
create table  acs_computed.ny_tract_own_rent_2013 as
  select t.* from acs_computed.ny_bg_tract_own_rent_2013 t join spatial.tl_2013_36_tract bg on t.geoid_tiger = bg.geoid;

/* By Block Group */
drop table if exists  acs_computed.ny_bg_own_rent_2013 ;
create table acs_computed.ny_bg_own_rent_2013  as
  select t.* from  acs_computed.ny_bg_tract_own_rent_2013 t join spatial.tl_2013_36_bg bg on t.geoid_tiger = bg.geoid;


/* TODO: Insurance Coverage */

select distinct relative_position, field_name, context_path from acs_summary_ny."e20135nyB27010" order by relative_position;
/*
1	Total	
2	Under 18 years	Total
3	With one type of health insurance coverage	Total|Under 18 years
4	With employer-based health insurance only	Total|With one type of health insurance coverage
5	With direct-purchase health insurance only	Total|With one type of health insurance coverage
6	With Medicare coverage only	Total|With one type of health insurance coverage
7	With Medicaid/means-tested public coverage only	Total|With one type of health insurance coverage
8	With TRICARE/military health coverage only	Total|With one type of health insurance coverage
9	With VA Health Care only	Total|With one type of health insurance coverage
10	With two or more types of health insurance coverage	Total|With one type of health insurance coverage
11	With employer-based and direct-purchase coverage	Total|With two or more types of health insurance coverage
12	With employer-based and Medicare coverage	Total|With two or more types of health insurance coverage
13	With Medicare and Medicaid/means-tested public coverage	Total|With two or more types of health insurance coverage
14	Other private only combinations	Total|With two or more types of health insurance coverage
15	Other public only combinations	Total|With two or more types of health insurance coverage
16	Other coverage combinations	Total|With two or more types of health insurance coverage
17	No health insurance coverage	Total|With two or more types of health insurance coverage
18	18 to 34 years	Total|With two or more types of health insurance coverage
19	With one type of health insurance coverage	Total|18 to 34 years
20	With employer-based health insurance only	Total|With one type of health insurance coverage
21	With direct-purchase health insurance only	Total|With one type of health insurance coverage
22	With Medicare coverage only	Total|With one type of health insurance coverage
23	With Medicaid/means-tested public coverage only	Total|With one type of health insurance coverage
24	With TRICARE/military health coverage only	Total|With one type of health insurance coverage
25	With VA Health Care only	Total|With one type of health insurance coverage
26	With two or more types of health insurance coverage	Total|With one type of health insurance coverage
27	With employer-based and direct-purchase coverage	Total|With two or more types of health insurance coverage
28	With employer-based and Medicare coverage	Total|With two or more types of health insurance coverage
29	With Medicare and Medicaid/means-tested public coverage	Total|With two or more types of health insurance coverage
30	Other private only combinations	Total|With two or more types of health insurance coverage
31	Other public only combinations	Total|With two or more types of health insurance coverage
32	Other coverage combinations	Total|With two or more types of health insurance coverage
33	No health insurance coverage	Total|With two or more types of health insurance coverage
34	35 to 64 years	Total|With two or more types of health insurance coverage
35	With one type of health insurance coverage	Total|35 to 64 years
36	With employer-based health insurance only	Total|With one type of health insurance coverage
37	With direct-purchase health insurance only	Total|With one type of health insurance coverage
38	With Medicare coverage only	Total|With one type of health insurance coverage
39	With Medicaid/means-tested public coverage only	Total|With one type of health insurance coverage
40	With TRICARE/military health coverage only	Total|With one type of health insurance coverage
41	With VA Health Care only	Total|With one type of health insurance coverage
42	With two or more types of health insurance coverage	Total|With one type of health insurance coverage
43	With employer-based and direct-purchase coverage	Total|With two or more types of health insurance coverage
44	With employer-based and Medicare coverage	Total|With two or more types of health insurance coverage
45	With direct-purchase and Medicare coverage	Total|With two or more types of health insurance coverage
46	With Medicare and Medicaid/means-tested public coverage	Total|With two or more types of health insurance coverage
47	Other private only combinations	Total|With two or more types of health insurance coverage
48	Other public only combinations	Total|With two or more types of health insurance coverage
49	Other coverage combinations	Total|With two or more types of health insurance coverage
50	No health insurance coverage	Total|With two or more types of health insurance coverage
51	65 years and over	Total|With two or more types of health insurance coverage
52	With one type of health insurance coverage	Total|65 years and over
53	With employer-based health insurance only	Total|With one type of health insurance coverage
54	With direct-purchase health insurance only	Total|With one type of health insurance coverage
55	With Medicare coverage only	Total|With one type of health insurance coverage
56	With TRICARE/military health coverage only	Total|With one type of health insurance coverage
57	With VA Health Care only	Total|With one type of health insurance coverage
58	With two or more types of health insurance coverage	Total|With one type of health insurance coverage
59	With employer-based and direct-purchase coverage	Total|With one type of health insurance coverage
60	With employer-based and Medicare coverage	Total|With one type of health insurance coverage
61	With direct-purchase and Medicare coverage	Total|With one type of health insurance coverage
62	With Medicare and Medicaid/means-tested public coverage	Total|With one type of health insurance coverage
63	Other private only combinations	Total|With one type of health insurance coverage
64	Other public only combinations	Total|With one type of health insurance coverage
65	Other coverage combinations	Total|With one type of health insurance coverage
66	No health insurance coverage	Total|With one type of health insurance coverage
*/

drop table if exists acs_computed.ny_zip_variables_table;
create table acs_computed.ny_zip_variables_table as 
select  t1.geo_name, t1.full_geoid, t1.geoid_tiger, t1.total_population, t1.zip5, t1.city, t1.land_area_square_meters, 
  t1.population_density_per_square_kilometer, 
  t2.total_population_non_hispanic, t2.total_population_hispanic, t2.fraction_population_non_hispanic, 
  t2.fraction_population_hispanic , t3.total_households, t3.spanish_language_households, t3.fraction_spanish_language_households, 
  t3.english_language_households, t3.fraction_english_language_households, t4.median_age, t5.median_household_income_2013,
  t6.per_capita_income_2013, t7.total_households_receiving_snap, t7.total_households_with_one_disability, 
  t7.fraction_receiving_snap, t7.fraction_with_one_disability, t8.total_households_owner_occupied, t8.total_households_renter_occupied, 
  t8.fraction_households_owner_occupied, t8.fraction_households_renter_occupied, 
  t9.total, t9.total_white, t9.total_african_american, t9.total_native_american, t9.total_asian_american, 
  t9.total_two_or_more_races, t9.fraction_population_white, 
  t9.fraction_population_african_american, t9.fraction_population_native_american, 
  t9.fraction_population_asian_american, t9.fraction_population_two_or_more_races,
  shp.geom, st_asgeojson(shp.geom) as geom_geojson
  from acs_computed.ny_zip_density_2013 t1
  join acs_computed.ny_zip_hispanic_2013 t2 on t1.geoid_tiger = t2.geoid_tiger
  join acs_computed.ny_zip_language_2013 t3 on t1.geoid_tiger = t3.geoid_tiger
  join acs_computed.ny_zip_median_age_2013 t4 on t1.geoid_tiger = t4.geoid_tiger
  join acs_computed.ny_zip_median_household_income_2013 t5 on t1.geoid_tiger = t5.geoid_tiger
  join acs_computed.ny_zip_per_capita_income_2013 t6 on t1.geoid_tiger = t6.geoid_tiger
  join acs_computed.ny_zip_snap_2013 t7 on t1.geoid_tiger = t7.geoid_tiger
  join acs_computed.ny_zip_own_rent_2013 t8 on t1.geoid_tiger = t8.geoid_tiger 
  join acs_computed.ny_zip_race_2013 t9 on t1.geoid_tiger = t9.geoid_tiger 
  join spatial.ny_ztca_trimmed_to_land shp on shp.zcta5 = t1.zip5
  ;


drop table if exists acs_computed.ny_tract_variables_table;
create table acs_computed.ny_tract_variables_table as 
select  t1.geo_name, t1.full_geoid, t1.geoid_tiger, t1.total_population, t1.land_area_square_meters, 
  t1.population_density_per_square_kilometer, 
  t2.total_population_non_hispanic, t2.total_population_hispanic, t2.fraction_population_non_hispanic, 
  t2.fraction_population_hispanic , t3.total_households, t3.spanish_language_households, t3.fraction_spanish_language_households, 
  t3.english_language_households, t3.fraction_english_language_households, t4.median_age, t5.median_household_income_2013,
  t6.per_capita_income_2013, t7.total_households_receiving_snap, t7.total_households_with_one_disability, 
  t7.fraction_receiving_snap, t7.fraction_with_one_disability, t8.total_households_owner_occupied, t8.total_households_renter_occupied, 
  t8.fraction_households_owner_occupied, t8.fraction_households_renter_occupied, 
  t9.total, t9.total_white, t9.total_african_american, t9.total_native_american, t9.total_asian_american, 
  t9.total_two_or_more_races, t9.fraction_population_white, 
  t9.fraction_population_african_american, t9.fraction_population_native_american, 
  t9.fraction_population_asian_american, t9.fraction_population_two_or_more_races,
  shp.geom, st_asgeojson(shp.geom) as geom_geojson
  from acs_computed.ny_tract_density_2013 t1
  join acs_computed.ny_tract_hispanic_2013 t2 on t1.geoid_tiger = t2.geoid_tiger
  join acs_computed.ny_tract_language_2013 t3 on t1.geoid_tiger = t3.geoid_tiger
  join acs_computed.ny_tract_median_age_2013 t4 on t1.geoid_tiger = t4.geoid_tiger
  join acs_computed.ny_tract_median_household_income_2013 t5 on t1.geoid_tiger = t5.geoid_tiger
  join acs_computed.ny_tract_per_capita_income_2013 t6 on t1.geoid_tiger = t6.geoid_tiger
  join acs_computed.ny_tract_snap_2013 t7 on t1.geoid_tiger = t7.geoid_tiger
  join acs_computed.ny_tract_own_rent_2013 t8 on t1.geoid_tiger = t8.geoid_tiger 
  join acs_computed.ny_tract_race_2013 t9 on t1.geoid_tiger = t9.geoid_tiger 
  join spatial.ny_tract_trimmed_to_land shp on shp.geoid = t1.geoid_tiger;
  ;

drop table if exists acs_computed.ny_bg_variables_table;
create table acs_computed.ny_bg_variables_table as 
select  t1.geo_name, t1.full_geoid, t1.geoid_tiger, t1.total_population, t1.land_area_square_meters, 
  t1.population_density_per_square_kilometer, 
  t2.total_population_non_hispanic, t2.total_population_hispanic, t2.fraction_population_non_hispanic, 
  t2.fraction_population_hispanic , t3.total_households, t3.spanish_language_households, t3.fraction_spanish_language_households, 
  t3.english_language_households, t3.fraction_english_language_households, t4.median_age, t5.median_household_income_2013,
  t6.per_capita_income_2013, t7.total_households_receiving_snap, t7.total_households_with_one_disability, 
  t7.fraction_receiving_snap, t7.fraction_with_one_disability, t8.total_households_owner_occupied, t8.total_households_renter_occupied, 
  t8.fraction_households_owner_occupied, t8.fraction_households_renter_occupied, 
  t9.total, t9.total_white, t9.total_african_american, t9.total_native_american, t9.total_asian_american, 
  t9.total_two_or_more_races, t9.fraction_population_white, 
  t9.fraction_population_african_american, t9.fraction_population_native_american, 
  t9.fraction_population_asian_american, t9.fraction_population_two_or_more_races
  , shp.geom, st_asgeojson(shp.geom) as geom_geojson
  from acs_computed.ny_bg_density_2013 t1
  join acs_computed.ny_bg_hispanic_2013 t2 on t1.geoid_tiger = t2.geoid_tiger
  join acs_computed.ny_bg_language_2013 t3 on t1.geoid_tiger = t3.geoid_tiger
  join acs_computed.ny_bg_median_age_2013 t4 on t1.geoid_tiger = t4.geoid_tiger
  join acs_computed.ny_bg_median_household_income_2013 t5 on t1.geoid_tiger = t5.geoid_tiger
  join acs_computed.ny_bg_per_capita_income_2013 t6 on t1.geoid_tiger = t6.geoid_tiger
  join acs_computed.ny_bg_snap_2013 t7 on t1.geoid_tiger = t7.geoid_tiger
  join acs_computed.ny_bg_own_rent_2013 t8 on t1.geoid_tiger = t8.geoid_tiger 
  join acs_computed.ny_bg_race_2013 t9 on t1.geoid_tiger = t9.geoid_tiger 
  join spatial.ny_bg_trimmed_to_land shp on shp.geoid = t1.geoid_tiger;