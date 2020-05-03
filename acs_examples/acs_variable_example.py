import json
import os
import acs_variable as av
import sqlalchemy as sa
import pandas as pd


def main(connection_uri, output_directory, schema_dict):

    engine = sa.create_engine(connection_uri)
    connection = engine.connect()

    # By tracts in Nassau and Suffolk
    query_to_get_tracts = f"""select distinct geo_name from {schema_dict["acs_ny_tbg"]}."e20185nyB01001" 
    where geo_name ~ '(^Census Tract.+Suffolk.+|^Census Tract.+Nassau.+)'"""

    cursor = connection.execute(query_to_get_tracts)
    tract_geonames = [r["geo_name"] for r in cursor]

    tract_geo_restriction = av.GeographicRestriction("li_tracts", tract_geonames)

    acs_scope_ny_tbg = av.ACSScope(connection, "e", 2018, 5, "ny", schema_dict["acs_ny_tbg"])

    ny_export_tbg = generate_acs_summary(acs_scope_ny_tbg, tract_geo_restriction, "geo_name")
    ny_export_tbg.write_to_csv(os.path.join(output_directory, "acs_tracts_nassau_suffolk_summary.csv"))

    # By zip codes
    acs_scope_us = av.ACSScope(connection, "e", 2018, 5, "us", schema_dict["acs_us"])

    suffolk_county_zips = [
        'ZCTA5 11720',
        'ZCTA5 11727',
        'ZCTA5 11733',
        'ZCTA5 11745',
        'ZCTA5 11754',
        'ZCTA5 11755',
        'ZCTA5 11760',
        'ZCTA5 11764',
        'ZCTA5 11766',
        'ZCTA5 11767',
        'ZCTA5 11776',
        'ZCTA5 11777',
        'ZCTA5 11778',
        'ZCTA5 11779',
        'ZCTA5 11780',
        'ZCTA5 11784',
        'ZCTA5 11786',
        'ZCTA5 11787',
        'ZCTA5 11788',
        'ZCTA5 11789',
        'ZCTA5 11790',
        'ZCTA5 11794',
        'ZCTA5 11953',
        'ZCTA5 11961',
        'ZCTA5 11705',
        'ZCTA5 11713',
        'ZCTA5 11715',
        'ZCTA5 11719',
        'ZCTA5 11738',
        'ZCTA5 11741',
        'ZCTA5 11742',
        'ZCTA5 11763',
        'ZCTA5 11772',
        'ZCTA5 11782',
        'ZCTA5 11967',
        'ZCTA5 11980',
        'ZCTA5 11701',
        'ZCTA5 11702',
        'ZCTA5 11703',
        'ZCTA5 11704',
        'ZCTA5 11706',
        'ZCTA5 11707',
        'ZCTA5 11708',
        'ZCTA5 11716',
        'ZCTA5 11717',
        'ZCTA5 11718',
        'ZCTA5 11722',
        'ZCTA5 11726',
        'ZCTA5 11729',
        'ZCTA5 11730',
        'ZCTA5 11739',
        'ZCTA5 11749',
        'ZCTA5 11751',
        'ZCTA5 11752',
        'ZCTA5 11757',
        'ZCTA5 11769',
        'ZCTA5 11770',
        'ZCTA5 11795',
        'ZCTA5 11796',
        'ZCTA5 11798',
        'ZCTA5 11792',
        'ZCTA5 11901',
        'ZCTA5 11931',
        'ZCTA5 11933',
        'ZCTA5 11934',
        'ZCTA5 11940',
        'ZCTA5 11941',
        'ZCTA5 11942',
        'ZCTA5 11947',
        'ZCTA5 11949',
        'ZCTA5 11950',
        'ZCTA5 11951',
        'ZCTA5 11955',
        'ZCTA5 11959',
        'ZCTA5 11960',
        'ZCTA5 11970',
        'ZCTA5 11972',
        'ZCTA5 11973',
        'ZCTA5 11977',
        'ZCTA5 11978',
        'ZCTA5 11930',
        'ZCTA5 11932',
        'ZCTA5 11937',
        'ZCTA5 11946',
        'ZCTA5 11954',
        'ZCTA5 11962',
        'ZCTA5 11963',
        'ZCTA5 11968',
        'ZCTA5 11969',
        'ZCTA5 11975',
        'ZCTA5 11976',
        'ZCTA5 11721',
        'ZCTA5 11724',
        'ZCTA5 11725',
        'ZCTA5 11731',
        'ZCTA5 11740',
        'ZCTA5 11743',
        'ZCTA5 11746',
        'ZCTA5 11747',
        'ZCTA5 11750',
        'ZCTA5 11768',
        'ZCTA5 11775',
        'ZCTA5 06390',
        'ZCTA5 11935',
        'ZCTA5 11939',
        'ZCTA5 11944',
        'ZCTA5 11948',
        'ZCTA5 11952',
        'ZCTA5 11956',
        'ZCTA5 11957',
        'ZCTA5 11958',
        'ZCTA5 11964',
        'ZCTA5 11965',
        'ZCTA5 11971',
    ]

    zcta5_geo_restriction = av.GeographicRestriction("suffolk_county_zips", suffolk_county_zips)
    zcta5_export = generate_acs_summary(acs_scope_us, zcta5_geo_restriction, "geo_name")

    zcta5_export.df["zip5"] = zcta5_export.df["geo_field"].apply(lambda x: x[6:])

    geo_categories_df = pd.read_csv(os.path.join(output_directory, "geo_categories.csv"), dtype={"zip5": str})

    export_df = pd.merge(geo_categories_df, zcta5_export.df, how="right", on="zip5")
    zcta5_export.write_to_csv(os.path.join(output_directory, "suffolk_county_zips_census_primary.csv"))

    export_df.to_csv(os.path.join(output_directory, "suffolk_county_zips_census_annotated.csv"), index=False)

    acs_scope_ny = av.ACSScope(connection, "e", 2018, 5, "ny", schema_dict["acs_ny"])

    ny_geonames = [
        'New York',
        'Erie County, New York',
        'Essex County, New York',
        'Franklin County, New York',
        'Fulton County, New York',
        'Genesee County, New York',
        'Greene County, New York',
        'Hamilton County, New York',
        'Herkimer County, New York',
        'Albany County, New York',
        'Allegany County, New York',
        'Bronx County, New York',
        'Broome County, New York',
        'Cattaraugus County, New York',
        'Cayuga County, New York',
        'Chautauqua County, New York',
        'Chemung County, New York',
        'Chenango County, New York',
        'Clinton County, New York',
        'Columbia County, New York',
        'Cortland County, New York',
        'Delaware County, New York',
        'Dutchess County, New York',
        'Jefferson County, New York',
        'Kings County, New York',
        'Lewis County, New York',
        'Livingston County, New York',
        'Madison County, New York',
        'Monroe County, New York',
        'Montgomery County, New York',
        'Nassau County, New York',
        'New York County, New York',
        'Niagara County, New York',
        'Oneida County, New York',
        'Onondaga County, New York',
        'Ontario County, New York',
        'Orange County, New York',
        'Orleans County, New York',
        'Oswego County, New York',
        'Otsego County, New York',
        'Putnam County, New York',
        'Queens County, New York',
        'Rensselaer County, New York',
        'Richmond County, New York',
        'Rockland County, New York',
        'St. Lawrence County, New York',
        'Saratoga County, New York',
        'Schenectady County, New York',
        'Schoharie County, New York',
        'Schuyler County, New York',
        'Seneca County, New York',
        'Steuben County, New York',
        'Suffolk County, New York',
        'Sullivan County, New York',
        'Tioga County, New York',
        'Tompkins County, New York',
        'Ulster County, New York',
        'Warren County, New York',
        'Washington County, New York',
        'Wayne County, New York',
        'Westchester County, New York',
        'Wyoming County, New York',
        'Yates County, New York'
    ]

    ny_geo_restriction = av.GeographicRestriction("ny_geonames", ny_geonames)
    ny_export = generate_acs_summary(acs_scope_ny, ny_geo_restriction, "geo_name")

    ny_export.write_to_csv(os.path.join(output_directory, "new_york_census_primary.csv"))



def generate_acs_summary(acs_scope, geographic_restriction, geo_field):

    variable_factory = av.ACSVariableFactory(acs_scope, geographic_restriction, geo_field)
    total = variable_factory.new("B01001", 1)
    total_male = variable_factory.new("B01001", 2)
    fraction_male = total_male / total

    total_female = variable_factory.new("B01001", 26)
    fraction_female = total_female / total

    foreign_born = variable_factory.new("B05006", 1)
    fraction_foreign_born = foreign_born / total

    white = variable_factory.new("B02001", 2)
    african_american = variable_factory.new("B02001", 3)
    native_american = variable_factory.new("B02001", 4)
    asian = variable_factory.new("B02001", 5)
    pacific_islander = variable_factory.new("B02001", 6)
    other_race = variable_factory.new("B02001", 7)
    multi_racial = variable_factory.new("B02001", 8)

    fraction_white = white / total
    fraction_african_american = african_american / total
    fraction_asian = asian / total
    fraction_other_race = other_race / total
    fraction_multi_racial = multi_racial / total

    hispanic = variable_factory.new("B03003", 3)
    fraction_hispanic = hispanic / total

    male_under_5_yr = variable_factory.new("B01001", 3)
    male_5_to_9_yr = variable_factory.new("B01001", 4)
    male_10_to_14_yr = variable_factory.new("B01001", 5)
    male_15_to_17_yr = variable_factory.new("B01001", 6)

    male_under_18_yr = male_under_5_yr + male_5_to_9_yr + male_10_to_14_yr + male_15_to_17_yr

    female_under_5_yr = variable_factory.new("B01001", 27)
    female_5_to_9_yr = variable_factory.new("B01001", 28)
    female_10_to_14_yr = variable_factory.new("B01001", 29)
    female_15_to_17_yr = variable_factory.new("B01001", 30)

    female_under_18_yr = female_under_5_yr + female_5_to_9_yr + female_10_to_14_yr + female_15_to_17_yr
    under_18_yr = male_under_18_yr + female_under_18_yr

    fraction_under_18_yr = under_18_yr / total

    male_65_to_66 = variable_factory.new("B01001", 20)
    male_67_to_69 = variable_factory.new("B01001", 21)
    male_70_to_74 = variable_factory.new("B01001", 22)
    male_75_to_79 = variable_factory.new("B01001", 23)
    male_80_to_84 = variable_factory.new("B01001", 24)
    male_85_plus = variable_factory.new("B01001", 25)

    male_65_plus = male_65_to_66 + male_67_to_69 + male_70_to_74 + male_75_to_79 + male_80_to_84 + male_85_plus

    female_65_to_66 = variable_factory.new("B01001", 44)
    female_67_to_69 = variable_factory.new("B01001", 45)
    female_70_to_74 = variable_factory.new("B01001", 46)
    female_75_to_79 = variable_factory.new("B01001", 47)
    female_80_to_84 = variable_factory.new("B01001", 48)
    female_85_plus = variable_factory.new("B01001", 49)

    female_65_plus = female_65_to_66 + female_67_to_69 + female_70_to_74 + female_75_to_79 + female_80_to_84 + female_85_plus

    sixty_five_plus = male_65_plus + female_65_plus

    fraction_65_plus = sixty_five_plus / total

    total_rentals = variable_factory.new("B25070", 1)
    total_rentals_not_computed = variable_factory.new("B25070", 11)

    perc_gross_35_to_39 = variable_factory.new("B25070", 8)
    perc_gross_40_to_49 = variable_factory.new("B25070", 9)
    perc_gross_50_plus = variable_factory.new("B25070", 10)

    perc_gross_35_plus = perc_gross_35_to_39 + perc_gross_40_to_49 + perc_gross_50_plus
    total_rentals_computed = total_rentals - total_rentals_not_computed

    fraction_more_than_35 = perc_gross_35_plus / total_rentals_computed

    """
   1 Total:	213,649,147	+/-15,761
2 Less than high school graduate	27,818,380	+/-122,561
3 High school graduate (includes equivalency)	58,820,411	+/-182,369
4 Some college or associate's degree	62,242,569	+/-55,692
5 Bachelor's degree	40,189,920	+/-142,140
6 Graduate or professional degree	24,577,867	+/-151,18
    """

    total_25_plus = variable_factory.new("B06009", 1)
    bachelor_degree = variable_factory.new("B06009", 5)
    graduate_prof_degree = variable_factory.new("B06009",6)

    bachelor_degree_or_higher = bachelor_degree + graduate_prof_degree

    fraction_bachelor_degree_or_higher = bachelor_degree_or_higher / total_25_plus

    """
1    Total:	195,226,024	+/-10,224
2 In the labor force:	149,849,229	+/-109,791
3 Employed:	138,920,971	+/-125,249
4 With a disability	6,993,203	+/-16,648
5 No disability	131,927,768	+/-123,746
6 Unemployed:	10,928,258	+/-29,732
7 With a disability	1,285,631	+/-9,361
8 No disability	9,642,627	+/-29,860
9 Not in labor force:	45,376,795	+/-113,164
10 With a disability	11,909,423	+/-46,803
11 No disability	33,467,372	+/-76,139
    """

    total_18_64 = variable_factory.new("C18120", 1)
    in_labor_force = variable_factory.new("C18120", 2)
    unemployed = variable_factory.new("C18120", 6)

    fraction_in_labor_force = in_labor_force / total_18_64
    unemployment_rate = unemployed / in_labor_force

    """
    Total:	310,629,645	+/-11,780
Below 100 percent of the poverty level	46,932,225	+/-284,072
100 to 149 percent of the poverty level	29,044,888	+/-134,530
At or above 150 percent of the poverty level	234,652,532	+/-408,43
    """

    below_100_fpl = variable_factory.new("B06012", 2)
    between_100_150_fpl = variable_factory.new("B06012", 3)

    below_150_fpl = below_100_fpl + between_100_150_fpl

    fraction_below_100_fpl = below_100_fpl / total
    fraction_below_150_fpl = below_150_fpl / total

    """
    2	Total	Under 18 years	5977	5977	11776					1
6	Total|With one type of health insurance coverage	With Medicare coverage only	0	0	11776	1				1
7	Total|With one type of health insurance coverage	With Medicaid/means-tested public coverage only	832	832	11776		1			1
13	Total|With two or more types of health insurance coverage	With Medicare and Medicaid/means-tested public coverage	0	0	11776			1		1
17	Total|With two or more types of health insurance coverage	No health insurance coverage	186	186	11776				1	1
18	Total	18 to 34 years	5414	5414	11776					1
22	Total|With one type of health insurance coverage	With Medicare coverage only	72	72	11776	1				1
23	Total|With one type of health insurance coverage	With Medicaid/means-tested public coverage only	861	861	11776		1			1
29	Total|With two or more types of health insurance coverage	With Medicare and Medicaid/means-tested public coverage	16	16	11776			1		1
33	Total|With two or more types of health insurance coverage	No health insurance coverage	761	761	11776				1	1
34	Total	35 to 64 years	10715	10715	11776					1
39	Total|With one type of health insurance coverage	With Medicaid/means-tested public coverage only	815	815	11776		1			1
46	Total|With two or more types of health insurance coverage	With Medicare and Medicaid/means-tested public coverage	224	224	11776			1		1
50	Total|With two or more types of health insurance coverage	No health insurance coverage	698	698	11776				1	1
55	Total|With one type of health insurance coverage	With Medicare coverage only	1036	1036	11776	1				1
62	Total|With two or more types of health insurance coverage	With Medicare and Medicaid/means-tested public coverage	182	182	11776			1		1
66	Total|With two or more types of health insurance coverage	No health insurance coverage	0	0	11776				1	1
    """

    """
    12	With employer-based and Medicare coverage	0
28	With employer-based and Medicare coverage	0
44	With employer-based and Medicare coverage	122
45	With direct-purchase and Medicare coverage	67
60	With employer-based and Medicare coverage	1311
61	With direct-purchase and Medicare coverage	604

    """

    medicare_only_under_18 = variable_factory.new("B27010", 6)
    medicaid_under_18 = variable_factory.new("B27010", 7)
    medicare_employee_under_18 = variable_factory.new("B27010", 7)

    dual_under_18 = variable_factory.new("B27010", 13)
    no_insurance_under_18 = variable_factory.new("B27010", 17)

    medicare_18_to_34 = variable_factory.new("B27010", 22)
    medicaid_18_to_34 = variable_factory.new("B27010", 23)
    medicare_employee_18_to_34 = variable_factory.new("B27010", 28)

    dual_18_to_34 = variable_factory.new("B27010", 29)
    no_insurance_18_to_34 = variable_factory.new("B27010", 33)

    medicare_only_35_to_64 = variable_factory.new("B27010", 38)
    medicaid_35_to_64 = variable_factory.new("B27010", 39)
    medicare_employee_35_to_64 = variable_factory.new("B27010", 45)

    dual_35_to_64 = variable_factory.new("B27010", 46)
    no_insurance_35_to_64 = variable_factory.new("B27010", 50)

    medicare_only_65_plus = variable_factory.new("B27010", 55)
    medicare_employee_65_plus = variable_factory.new("B27010", 60)
    medicare_direct_65_plus = variable_factory.new("B27010", 61)
    dual_65_plus = variable_factory.new("B27010", 62)
    no_insurance_65_plus = variable_factory.new("B27010", 66)

    medicare_only = medicare_only_under_18 + medicare_18_to_34 + medicare_only_35_to_64 + medicare_only_65_plus
    medicare_employee_direct = medicare_employee_under_18 + medicare_employee_18_to_34 + medicare_employee_35_to_64 \
        + medicare_employee_65_plus + medicare_direct_65_plus

    medicare = medicare_only + medicare_employee_direct

    medicaid = medicaid_under_18 + medicaid_18_to_34 + medicaid_35_to_64
    dual = dual_under_18 + dual_18_to_34 + dual_35_to_64 + dual_65_plus
    no_insurance = no_insurance_under_18 + no_insurance_18_to_34 + no_insurance_35_to_64 + no_insurance_65_plus

    fraction_medicare = medicare / total
    fraction_medicaid = medicaid / total
    fraction_dual = dual / total
    fraction_no_insurance = no_insurance / total

    """
    relative_position	context_path	field_name	numeric_value	geoid_tiger	filter
1    
2	Total	Native	20867	11776	1
7	Total|Speak Spanish	Speak English "not well"	110	11776	1
8	Total|Speak Spanish	Speak English "not at all"	0	11776	1
12	Total|Speak other Indo-European languages	Speak English "not well"	20	11776	1
13	Total|Speak other Indo-European languages	Speak English "not at all"	0	11776	1
17	Total|Speak Asian and Pacific Island languages	Speak English "not well"	0	11776	1
18	Total|Speak Asian and Pacific Island languages	Speak English "not at all"	0	11776	1
22	Total|Speak other languages	Speak English "not well"	0	11776	1
23	Total|Speak other languages	Speak English "not at all"	0	11776	1
28	Total|Speak Spanish	Speak English "well"	497	11776	1
29	Total|Speak Spanish	Speak English "not well"	557	11776	1
34	Total|Speak other Indo-European languages	Speak English "not well"	96	11776	1
35	Total|Speak other Indo-European languages	Speak English "not at all"	6	11776	1
39	Total|Speak Asian and Pacific Island languages	Speak English "not well"	44	11776	1
40	Total|Speak Asian and Pacific Island languages	Speak English "not at all"	47	11776	1
44	Total|Speak other languages	Speak English "not well"	0	11776	1
45	Total|Speak other languages	Speak English "not at all"	0	11776	1
    """

    total_5_plus = variable_factory.new("B16005", 1)

    native_born_speak_spanish_english_not_well = variable_factory.new("B16005", 7)
    native_born_speak_spanish_english_not_all = variable_factory.new("B16005", 8)
    native_born_speak_spanish_english_limited = native_born_speak_spanish_english_not_well + native_born_speak_spanish_english_not_all

    native_born_speak_euro_english_not_well = variable_factory.new("B16005", 12)
    native_born_speak_euro_english_not_all = variable_factory.new("B16005", 13)
    native_born_speak_euro_english_limited = native_born_speak_euro_english_not_well + native_born_speak_euro_english_not_all

    native_born_speak_asian_english_not_well = variable_factory.new("B16005", 17)
    native_born_speak_asian_english_not_all = variable_factory.new("B16005", 18)
    native_born_speak_asian_english_limited = native_born_speak_asian_english_not_well + native_born_speak_asian_english_not_all

    native_born_speak_other_english_not_well = variable_factory.new("B16005", 22)
    native_born_speak_other_english_not_all = variable_factory.new("B16005", 23)
    native_born_speak_other_english_limited = native_born_speak_other_english_not_well + native_born_speak_other_english_not_all

    native_born_speak_english_limited = native_born_speak_spanish_english_limited + native_born_speak_euro_english_limited \
                                   + native_born_speak_asian_english_limited + native_born_speak_other_english_limited

    foreign_born_speak_spanish_english_not_well = variable_factory.new("B16005", 28)
    foreign_born_speak_spanish_english_not_all = variable_factory.new("B16005", 29)
    foreign_born_speak_spanish_english_limited = foreign_born_speak_spanish_english_not_well + foreign_born_speak_spanish_english_not_all

    foreign_born_speak_euro_english_not_well = variable_factory.new("B16005", 34)
    foreign_born_speak_euro_english_not_all = variable_factory.new("B16005", 35)
    foreign_born_speak_euro_english_limited = foreign_born_speak_euro_english_not_well + foreign_born_speak_euro_english_not_all

    foreign_born_speak_asian_english_not_well = variable_factory.new("B16005", 39)
    foreign_born_speak_asian_english_not_all = variable_factory.new("B16005", 40)
    foreign_born_speak_asian_english_limited = foreign_born_speak_asian_english_not_well + foreign_born_speak_asian_english_not_all

    foreign_born_speak_other_english_not_well = variable_factory.new("B16005", 44)
    foreign_born_speak_other_english_not_all = variable_factory.new("B16005", 45)
    foreign_born_speak_other_english_limited = foreign_born_speak_other_english_not_well + foreign_born_speak_other_english_not_all

    foreign_born_speak_english_limited = foreign_born_speak_spanish_english_limited + foreign_born_speak_asian_english_limited \
        + foreign_born_speak_euro_english_limited + foreign_born_speak_other_english_limited

    speak_english_limited = native_born_speak_english_limited + foreign_born_speak_english_limited

    fraction_speak_english_limited = speak_english_limited / total_5_plus

    median_household_income = variable_factory.new("B19013", 1)

    acs_export_obj = av.ACSExport([("total_population", total),
                               ("total_male", total_male),
                               ("fraction_male", fraction_male),
                               ("total_female", total_female),
                               ("fraction_female", fraction_female),
                               ("foreign_born", foreign_born),
                               ("fraction_foreign_born", fraction_foreign_born),
                               ("white", white),
                               ("fraction_white", fraction_white),
                               ("african_american", african_american),
                               ("fraction_african_american", fraction_african_american),
                               ("asian", asian),
                               ("fraction_asian", fraction_asian),
                               ("native_american", native_american),
                               ("pacific_islander", pacific_islander),
                               ("other_race", other_race),
                               ("fraction_other_race", fraction_other_race),
                               ("multi_racial", fraction_multi_racial),
                               ("hispanic", hispanic),
                               ("fraction_hispanic", fraction_hispanic),
                               ("total_5_plus", total_5_plus),
                               ("speak_english_limited", speak_english_limited),
                               ("fraction_speak_english_limited", fraction_speak_english_limited),
                               ("under_18_yr", under_18_yr),
                               ("fraction_under_18_yr", fraction_under_18_yr),
                               ("65_plus", sixty_five_plus),
                               ("fraction_65_plus", fraction_65_plus),
                               ("total_rental_used_in_calc", total_rentals_computed),
                               ("35_plus_rent_gross", perc_gross_35_plus),
                               ("fraction_more_than_35_gross_rent", fraction_more_than_35),
                               ("total_25_plus", total_25_plus),
                               ("bachelor_and_higher", bachelor_degree_or_higher),
                               ("fraction_bachelor_and_higher", fraction_bachelor_degree_or_higher),
                               ("total_18_64", total_18_64),
                               ("in_labor_force", in_labor_force),
                               ("fraction_in_labor_force", fraction_in_labor_force),
                               ("unemployed", unemployed),
                               ("fraction_unemployed", unemployment_rate),
                               ("below_100_fpl", below_100_fpl),
                               ("fraction_below_100_fpl", fraction_below_100_fpl),
                               ("below_150_fpl", below_150_fpl),
                               ("fraction_below_150_fpl", fraction_below_150_fpl),
                               ("medicaid", medicaid),
                               ("fraction_medicaid", fraction_medicaid),
                               ("medicare", medicare),
                               ("fraction_medicare", fraction_medicare),
                               ("dual", dual),
                               ("fraction_dual", fraction_dual),
                               ("no_insurance", no_insurance),
                               ("fraction_no_insurance", fraction_no_insurance),
                               ("median_household_income", median_household_income)
                            ])

    return acs_export_obj


if __name__ == "__main__":

    with open("config.json") as f:
        config = json.load(f)

    main(config["connection_uri"], config["output_directory"], config["schemas"])
