
/* Build tables related to insurance coverage */

drop table if exists acs_computed.ny_bg_tract_insurance_2013_1;
create table acs_computed.ny_bg_tract_insurance_2013_1 as 
select t_geoid as geoid, geoid_tiger, geo_name, table_name, subject_area,
  population_total,
  population_17,
  medicare_only_17,
  medicaid_only_17,
  medicare_employee_17,
  medicare_medicaid_17,
  no_health_insurance_17,
  population_18_34,
  medicare_only_18_34,
  medicaid_only_18_34,
  medicare_employee_18_34,
  medicare_medicaid_18_34,
  no_health_insurance_18_34,
  population_35_64,
  medicare_only_35_64,
  medicaid_only_35_64,
  medicare_employee_35_64,
  medicare_direct_purchase_35_64,
  medicare_medicaid_35_64,
  no_health_insurance_35_64,
  population_65_plus,
  medicare_only_65_plus,
  medicare_employee_65_plus,
  medicare_direct_purchase_65_plus,
  medicare_medicaid_65_plus,
  no_health_insurance_65_plus,
  now() as time_stamp
from (
 (select geoid, table_name, subject_area, geo_name, geoid as t_geoid, geoid_tiger, numeric_value as population_total  from acs_summary_ny."e20135nyB27010" where relative_position = 1) t1 
join  (select geoid, numeric_value as population_17  from acs_summary_ny."e20135nyB27010" where relative_position = 2) t2 on t1.geoid = t2.geoid
join  (select geoid, numeric_value as medicare_only_17  from acs_summary_ny."e20135nyB27010" where relative_position = 6) t6 on t1.geoid = t6.geoid
join  (select geoid, numeric_value as medicaid_only_17  from acs_summary_ny."e20135nyB27010" where relative_position = 7) t7 on t1.geoid = t7.geoid
join  (select geoid, numeric_value as medicare_employee_17  from acs_summary_ny."e20135nyB27010" where relative_position = 12) t12 on t1.geoid = t12.geoid
join  (select geoid, numeric_value as medicare_medicaid_17  from acs_summary_ny."e20135nyB27010" where relative_position = 13) t13 on t1.geoid = t13.geoid
join  (select geoid, numeric_value as no_health_insurance_17  from acs_summary_ny."e20135nyB27010" where relative_position = 17) t17 on t1.geoid = t17.geoid
join  (select geoid, numeric_value as population_18_34  from acs_summary_ny."e20135nyB27010" where relative_position = 18) t18 on t1.geoid = t18.geoid
join  (select geoid, numeric_value as medicare_only_18_34  from acs_summary_ny."e20135nyB27010" where relative_position = 22) t22 on t1.geoid = t22.geoid
join  (select geoid, numeric_value as medicaid_only_18_34  from acs_summary_ny."e20135nyB27010" where relative_position = 23) t23 on t1.geoid = t23.geoid
join  (select geoid, numeric_value as medicare_employee_18_34  from acs_summary_ny."e20135nyB27010" where relative_position = 28) t28 on t1.geoid = t28.geoid
join  (select geoid, numeric_value as medicare_medicaid_18_34  from acs_summary_ny."e20135nyB27010" where relative_position = 29) t29 on t1.geoid = t29.geoid
join  (select geoid, numeric_value as no_health_insurance_18_34  from acs_summary_ny."e20135nyB27010" where relative_position = 33) t33 on t1.geoid = t33.geoid
join  (select geoid, numeric_value as population_35_64  from acs_summary_ny."e20135nyB27010" where relative_position = 34) t34 on t1.geoid = t34.geoid
join  (select geoid, numeric_value as medicare_only_35_64  from acs_summary_ny."e20135nyB27010" where relative_position = 38) t38 on t1.geoid = t38.geoid
join  (select geoid, numeric_value as medicaid_only_35_64  from acs_summary_ny."e20135nyB27010" where relative_position = 39) t39 on t1.geoid = t39.geoid
join  (select geoid, numeric_value as medicare_employee_35_64  from acs_summary_ny."e20135nyB27010" where relative_position = 44) t44 on t1.geoid = t44.geoid
join  (select geoid, numeric_value as medicare_direct_purchase_35_64  from acs_summary_ny."e20135nyB27010" where relative_position = 45) t45 on t1.geoid = t45.geoid
join  (select geoid, numeric_value as medicare_medicaid_35_64  from acs_summary_ny."e20135nyB27010" where relative_position = 46) t46 on t1.geoid = t46.geoid
join  (select geoid, numeric_value as no_health_insurance_35_64  from acs_summary_ny."e20135nyB27010" where relative_position = 50) t50 on t1.geoid = t50.geoid
join  (select geoid, numeric_value as population_65_plus  from acs_summary_ny."e20135nyB27010" where relative_position = 51) t51 on t1.geoid = t51.geoid
join  (select geoid, numeric_value as medicare_only_65_plus  from acs_summary_ny."e20135nyB27010" where relative_position = 55) t55 on t1.geoid = t55.geoid
join  (select geoid, numeric_value as medicare_employee_65_plus  from acs_summary_ny."e20135nyB27010" where relative_position = 60) t60 on t1.geoid = t60.geoid
join  (select geoid, numeric_value as medicare_direct_purchase_65_plus  from acs_summary_ny."e20135nyB27010" where relative_position = 61) t61 on t1.geoid = t61.geoid
join  (select geoid, numeric_value as medicare_medicaid_65_plus  from acs_summary_ny."e20135nyB27010" where relative_position = 62) t62 on t1.geoid = t62.geoid
join  (select geoid, numeric_value as no_health_insurance_65_plus  from acs_summary_ny."e20135nyB27010" where relative_position = 66) t66 on t1.geoid = t66.geoid
) t;


drop table if exists acs_computed.ny_bg_tract_insurance_2013_2;
create table acs_computed.ny_bg_tract_insurance_2013_2 as 
select 
    case when population_64 > 0 then no_health_insurance_64 / population_64 end as fraction_no_health_insurance_64, 
    case when population_total > 0 then no_health_insurance / population_total end as fraction_no_health_insurance,
    case when population_64 > 0 then medicaid_any_64 / population_64 end as fraction_medicaid_any_64,
    case when population_total > 0 then medicaid_any / population_total end as fraction_medicaid_any,
    tt.* from
(select 
  medicare_medicaid_64 + medicare_only_64 as medicaid_any_64,
  medicaid_only_64 + medicare_medicaid_64 + medicare_medicaid_65_plus as medicaid_any,
  medicare_only_64 + medicare_employee_64 + medicare_direct_purchase_35_64 as medicare_any_64,
  medicare_only_65_plus + medicare_employee_65_plus + medicare_direct_purchase_65_plus + medicare_medicaid_65_plus as medicare_any_65_plus,
t.* 
from (
select 
  medicare_only_17 + medicare_only_18_34 + medicare_only_35_64 + medicare_only_65_plus as medicare_only,
  medicare_only_17 + medicare_only_18_34 + medicare_only_35_64 as medicare_only_64,
  medicaid_only_17 + medicaid_only_18_34 + medicaid_only_35_64 as medicaid_only_64,
  population_17 + population_18_34 + population_35_64 as population_64,
  no_health_insurance_17 + no_health_insurance_18_34 + no_health_insurance_35_64 as no_health_insurance_64,
  no_health_insurance_17 + no_health_insurance_18_34 + no_health_insurance_35_64 + no_health_insurance_65_plus as no_health_insurance,
  medicare_medicaid_17 + medicare_medicaid_18_34 + medicare_medicaid_35_64 + medicare_medicaid_65_plus as medicare_medicaid,
  medicare_medicaid_17 + medicare_medicaid_18_34 + medicare_medicaid_35_64 as medicare_medicaid_64,
  medicare_employee_17 + medicare_employee_18_34 + medicare_employee_35_64 as medicare_employee_64,
  medicare_employee_17 + medicare_employee_18_34 + medicare_employee_35_64 + medicare_employee_65_plus as medicare_employee,
  i.* 
  from acs_computed.ny_bg_tract_insurance_2013_1 i) t) tt;
    
  
create table acs_computed.ny_tract_insurance_2013 as
  select bti.*, tt.name, tt.statefp, tt.countyfp, tt.geom 
    FROM acs_computed.ny_bg_tract_insurance_2013_2 bti join spatial.ny_tract_trimmed_to_land tt on bti.geoid_tiger = tt.geoid ;
  
    
select * from acs_computed.ny_tract_insurance_2013 where geo_name like '%Suffolk%';    
    
create table acs_computed.ny_bg_insurance_2013 as
  select bti.*, tt.name, tt.statefp, tt.countyfp, tt.geom 
    FROM acs_computed.ny_bg_tract_insurance_2013_2 bti join spatial.ny_bg_trimmed_to_land tt on bti.geoid_tiger = tt.geoid ;

    
    
drop table if exists acs_computed.us_zcta_insurance_2013_1;
create table acs_computed.us_zcta_insurance_2013_1 as 
select t_geoid as geoid, geoid_tiger, geo_name, table_name, subject_area,
  population_total,
  population_17,
  medicare_only_17,
  medicaid_only_17,
  medicare_employee_17,
  medicare_medicaid_17,
  no_health_insurance_17,
  population_18_34,
  medicare_only_18_34,
  medicaid_only_18_34,
  medicare_employee_18_34,
  medicare_medicaid_18_34,
  no_health_insurance_18_34,
  population_35_64,
  medicare_only_35_64,
  medicaid_only_35_64,
  medicare_employee_35_64,
  medicare_direct_purchase_35_64,
  medicare_medicaid_35_64,
  no_health_insurance_35_64,
  population_65_plus,
  medicare_only_65_plus,
  medicare_employee_65_plus,
  medicare_direct_purchase_65_plus,
  medicare_medicaid_65_plus,
  no_health_insurance_65_plus,
  now() as time_stamp
from (
 (select geoid, table_name, subject_area, geo_name, geoid as t_geoid, geoid_tiger, numeric_value as population_total  from acs_summary_us."e20135usB27010" where relative_position = 1
 and geo_name like '%ZCTA%'
 ) t1 
join  (select geoid, numeric_value as population_17  from acs_summary_us."e20135usB27010" where relative_position = 2) t2 on t1.geoid = t2.geoid
join  (select geoid, numeric_value as medicare_only_17  from acs_summary_us."e20135usB27010" where relative_position = 6) t6 on t1.geoid = t6.geoid
join  (select geoid, numeric_value as medicaid_only_17  from acs_summary_us."e20135usB27010" where relative_position = 7) t7 on t1.geoid = t7.geoid
join  (select geoid, numeric_value as medicare_employee_17  from acs_summary_us."e20135usB27010" where relative_position = 12) t12 on t1.geoid = t12.geoid
join  (select geoid, numeric_value as medicare_medicaid_17  from acs_summary_us."e20135usB27010" where relative_position = 13) t13 on t1.geoid = t13.geoid
join  (select geoid, numeric_value as no_health_insurance_17  from acs_summary_us."e20135usB27010" where relative_position = 17) t17 on t1.geoid = t17.geoid
join  (select geoid, numeric_value as population_18_34  from acs_summary_us."e20135usB27010" where relative_position = 18) t18 on t1.geoid = t18.geoid
join  (select geoid, numeric_value as medicare_only_18_34  from acs_summary_us."e20135usB27010" where relative_position = 22) t22 on t1.geoid = t22.geoid
join  (select geoid, numeric_value as medicaid_only_18_34  from acs_summary_us."e20135usB27010" where relative_position = 23) t23 on t1.geoid = t23.geoid
join  (select geoid, numeric_value as medicare_employee_18_34  from acs_summary_us."e20135usB27010" where relative_position = 28) t28 on t1.geoid = t28.geoid
join  (select geoid, numeric_value as medicare_medicaid_18_34  from acs_summary_us."e20135usB27010" where relative_position = 29) t29 on t1.geoid = t29.geoid
join  (select geoid, numeric_value as no_health_insurance_18_34  from acs_summary_us."e20135usB27010" where relative_position = 33) t33 on t1.geoid = t33.geoid
join  (select geoid, numeric_value as population_35_64  from acs_summary_us."e20135usB27010" where relative_position = 34) t34 on t1.geoid = t34.geoid
join  (select geoid, numeric_value as medicare_only_35_64  from acs_summary_us."e20135usB27010" where relative_position = 38) t38 on t1.geoid = t38.geoid
join  (select geoid, numeric_value as medicaid_only_35_64  from acs_summary_us."e20135usB27010" where relative_position = 39) t39 on t1.geoid = t39.geoid
join  (select geoid, numeric_value as medicare_employee_35_64  from acs_summary_us."e20135usB27010" where relative_position = 44) t44 on t1.geoid = t44.geoid
join  (select geoid, numeric_value as medicare_direct_purchase_35_64  from acs_summary_us."e20135usB27010" where relative_position = 45) t45 on t1.geoid = t45.geoid
join  (select geoid, numeric_value as medicare_medicaid_35_64  from acs_summary_us."e20135usB27010" where relative_position = 46) t46 on t1.geoid = t46.geoid
join  (select geoid, numeric_value as no_health_insurance_35_64  from acs_summary_us."e20135usB27010" where relative_position = 50) t50 on t1.geoid = t50.geoid
join  (select geoid, numeric_value as population_65_plus  from acs_summary_us."e20135usB27010" where relative_position = 51) t51 on t1.geoid = t51.geoid
join  (select geoid, numeric_value as medicare_only_65_plus  from acs_summary_us."e20135usB27010" where relative_position = 55) t55 on t1.geoid = t55.geoid
join  (select geoid, numeric_value as medicare_employee_65_plus  from acs_summary_us."e20135usB27010" where relative_position = 60) t60 on t1.geoid = t60.geoid
join  (select geoid, numeric_value as medicare_direct_purchase_65_plus  from acs_summary_us."e20135usB27010" where relative_position = 61) t61 on t1.geoid = t61.geoid
join  (select geoid, numeric_value as medicare_medicaid_65_plus  from acs_summary_us."e20135usB27010" where relative_position = 62) t62 on t1.geoid = t62.geoid
join  (select geoid, numeric_value as no_health_insurance_65_plus  from acs_summary_us."e20135usB27010" where relative_position = 66) t66 on t1.geoid = t66.geoid
) t
;
 
drop table if exists acs_computed.us_zcta_insurance_2013_2;
create table acs_computed.us_zcta_insurance_2013_2 as 
select 
    case when population_64 > 0 then no_health_insurance_64 / population_64 end as fraction_no_health_insurance_64, 
    case when population_total > 0 then no_health_insurance / population_total end as fraction_no_health_insurance,
    case when population_64 > 0 then medicaid_any_64 / population_64 end as fraction_medicaid_any_64,
    case when population_total > 0 then medicaid_any / population_total end as fraction_medicaid_any,
    tt.* from
(select 
  medicare_medicaid_64 + medicare_only_64 as medicaid_any_64,
  medicaid_only_64 + medicare_medicaid_64 + medicare_medicaid_65_plus as medicaid_any,
  medicare_only_64 + medicare_employee_64 + medicare_direct_purchase_35_64 as medicare_any_64,
  medicare_only_65_plus + medicare_employee_65_plus + medicare_direct_purchase_65_plus + medicare_medicaid_65_plus as medicare_any_65_plus,
t.* 
from (
select 
  medicare_only_17 + medicare_only_18_34 + medicare_only_35_64 + medicare_only_65_plus as medicare_only,
  medicare_only_17 + medicare_only_18_34 + medicare_only_35_64 as medicare_only_64,
  medicaid_only_17 + medicaid_only_18_34 + medicaid_only_35_64 as medicaid_only_64,
  population_17 + population_18_34 + population_35_64 as population_64,
  no_health_insurance_17 + no_health_insurance_18_34 + no_health_insurance_35_64 as no_health_insurance_64,
  no_health_insurance_17 + no_health_insurance_18_34 + no_health_insurance_35_64 + no_health_insurance_65_plus as no_health_insurance,
  medicare_medicaid_17 + medicare_medicaid_18_34 + medicare_medicaid_35_64 + medicare_medicaid_65_plus as medicare_medicaid,
  medicare_medicaid_17 + medicare_medicaid_18_34 + medicare_medicaid_35_64 as medicare_medicaid_64,
  medicare_employee_17 + medicare_employee_18_34 + medicare_employee_35_64 as medicare_employee_64,
  medicare_employee_17 + medicare_employee_18_34 + medicare_employee_35_64 + medicare_employee_65_plus as medicare_employee,
  i.* 
  from acs_computed.us_zcta_insurance_2013_1 i) t) tt;
    
--create table acs_computed.ny_zcta_insurance_2013 as
select ttt.*, zccn.state, zccn.city, case when sc.gid is not null then 'Suffolk County' else null end is_suffolk from (
  select bti.*, tt.* --tt.geom 
    FROM acs_computed.us_zcta_insurance_2013_2 bti 
    join spatial.ny_ztca_trimmed_to_land tt on bti.geoid_tiger = tt.zcta5) ttt
    join spatial.tl_2013_us_zcta510 z on z.zcta5ce10 = ttt.zcta5
    left outer join spatial.zip_code_city_name zccn on ttt.zcta5 = zccn.zip5
    left outer join spatial.us_counties sc on ST_intersects(z.geom, sc.geom) and sc.name like '%Suffolk%' and sc.statefp = '36';
    
    ;    
    
