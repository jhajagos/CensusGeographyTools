select * from acs_summary_ny."e20135nyB25077";

create table acs_summary_ny.ny_median_income_trimmed as 
  select acsi.geoid, acsi.geo_name, acsi.table_id, acsi.numeric_value as median_house_hold_income, nbt.geom 
    from acs_summary_ny."e20135nyB19013" acsi 
      join spatial.ny_bg_2013_trimmed_to_land nbt on acsi.geoid_tiger = nbt.geoid;
    
create table acs_summary_ny.ny_median_income_trimmed as 
  select acsi.geoid, acsi.geo_name, acsi.table_id, acsi.numeric_value as median_house_hold_income, nbt.geom from acs_summary_ny."e20135nyB19013" acsi 
    join public.ny_bg_2013_trimmed_to_land nbt on acsi.geoid_tiger = nbt.geoid;

create table acs_summary_ny.ny_language_spoken_trimmed as    
  select  nbt.gid, nbt.statefp, nbt.countyfp, nbt.geoid, nbt.name, nbt.geom, t.geo_name, t.table_id, 
  t.table_name, t.subject_area, t.relative_position, t.context_path, t.file_type, t.field_name, 
  t.numeric_value as total_households, t.geoid_tiger, t.spanish_language_households, t.percentage_spanish_language_households, 
  t.english_language_households, t.percentage_english_language_households from public.ny_bg_2013_trimmed_to_land nbt join 
  (select e1.*, e2.numeric_value as spanish_language_households, 
    case when e1.numeric_value > 0 then e2.numeric_value / e1.numeric_value else null end as percentage_spanish_language_households,
    e3.numeric_value as english_language_households,
    case when e1.numeric_value > 0 then e3.numeric_value / e1.numeric_value else null end as percentage_english_language_households
    from acs_summary_ny."e20135nyB16002" e1 join acs_summary_ny."e20135nyB16002" e2 on e1.geoid = e2.geoid and e1.relative_position = 1
      and e2.relative_position = 3
      join acs_summary_ny."e20135nyB16002" e3 on e3.relative_position = 2 and e1.geoid = e3.geoid and e1.relative_position = 1) t
      on t.geoid_tiger = nbt.geoid;
    ;
   
    
select t.*, sppl.* from suffolk_dsrip_providers.suffolk_pps_partner_list_20150130 sppl 
  left outer join
  (select nc."NPI", ng.latitude, ng.longitude, ng.matched_address, ng.address_formatted, ng.rating, ng.zip5 from provider."NPPES_contact" nc 
    join provider."NPPES_provider_address_geocoded" ng on nc.address_hash = ng.address_hash and ng.is_best_estimate = 1 and nc.address_type = 'practice') 
      t on sppl."NPI_ID" = t."NPI";
      

select * from acs_summary_ny."e20135usB25077";   --median household value   
select * from acs_summary_ny."e20135nyB22010"; -- SNAP assistance
select * from  acs_summary_ny."e20135nyB16002";  --language spoken
select * from acs_summary_ny."e20135nyB02001";   --race
select  distinct field_name from acs_summary_ny."e20135nyB02001";



select * from acs_summary_ny."e20135nyB02008";   --race


select * from acs_summary_ny."e20135nyB03003"; --hispanic or latino origin
--Not Hispanic or Latino
--Hispanic or Latino
--Total



/* By Zip Code */
  select  t.geo_name, t.table_id, 
  t.table_name, t.subject_area, t.relative_position, t.context_path, t.file_type, t.field_name, 
  t.numeric_value as total_households, t.geoid_tiger, sezc.city, t.spanish_language_households, t.percentage_spanish_language_households, 
  t.english_language_households, t.percentage_english_language_households, t.numeric_value as total_households from spatial.tl_2013_us_zcta510 nbt join 
  (select e1.*, e2.numeric_value as spanish_language_households, 
    case when e1.numeric_value > 0 then e2.numeric_value / e1.numeric_value else null end as percentage_spanish_language_households,
    e3.numeric_value as english_language_households,
    case when e1.numeric_value > 0 then e3.numeric_value / e1.numeric_value else null end as percentage_english_language_households
    from acs_summary_us."e20135usB16002" e1 join acs_summary_us."e20135usB16002" e2 on e1.geoid = e2.geoid and e1.relative_position = 1
      and e2.relative_position = 3
      join acs_summary_us."e20135usB16002" e3 on e3.relative_position = 2 and e1.geoid = e3.geoid and e1.relative_position = 1) t
      on t.geoid_tiger = nbt.geoid10 and t.geo_name like 'ZCTA5%'
      join spatial.us_counties uc on /*uc.name = uc.statefp = '36' and */ public.St_Intersects(uc.geom, nbt.geom)
      join spatial.sparcs_empirical_zip_city sezc on sezc.zip5 = t.geoid_tiger;
      ;
     