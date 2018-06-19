import json
import os
import acs_variable as av
import sqlalchemy as sa
import pandas as pd


def main(connection_uri, schema, output_directory):

    engine = sa.create_engine(connection_uri)
    connection = engine.connect()

    acs_scope = av.ACSScope(connection, "e", 2016, 5, "us", schema)

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

    geo_restriction = av.GeographicRestriction("suffolk_county_zips", suffolk_county_zips)

    variable_factory = av.ACSVariableFactory(acs_scope, geo_restriction)
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
    fraction_multi_racial =  multi_racial / total
    #fraction_non_white = av.ACSConstant(1) - fraction_white

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

    male_65_plus = male_65_to_66 + male_67_to_69 + male_70_to_74 + male_75_to_79 +  male_80_to_84 + male_85_plus

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

    """
    Total:	310,629,645	+/-11,780
Below 100 percent of the poverty level	46,932,225	+/-284,072
100 to 149 percent of the poverty level	29,044,888	+/-134,530
At or above 150 percent of the poverty level	234,652,532	+/-408,43
    """



    export_obj = av.ACSExport([("total_population", total),
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
                               ("under_18_yr", under_18_yr),
                               ("fraction_under_18_yr", fraction_under_18_yr),
                               ("65_plus", sixty_five_plus),
                               ("fraction_65_plus", fraction_65_plus),
                               ("total_rental_used_in_calc", total_rentals_computed),
                               ("35_plus_rent_gross", perc_gross_35_plus),
                               ("fraction_more_than_35_gross_rent", fraction_more_than_35),
                               ("total_25_plus", total_25_plus),
                               ("bachelor_and_higher", bachelor_degree_or_higher),
                               ("fraction_bachelor_and_higher", fraction_bachelor_degree_or_higher)
                               ])

    export_obj.df["zip5"] = export_obj.df["geo_field"].apply(lambda x: x[6:])

    geo_categories_df = pd.read_csv(os.path.join(output_directory, "geo_categories.csv"), dtype={"zip5": str})

    export_df = pd.merge(geo_categories_df, export_obj.df, how="right", on="zip5")
    export_obj.write_to_csv(os.path.join(output_directory, "suffolk_county_zips_census_primary.csv"))

    export_df.to_csv(os.path.join(output_directory, "suffolk_county_zips_census_annotated.csv"), index=False)


if __name__ == "__main__":

    with open("config.json") as f:
        config = json.load(f)

    main(config["connection_uri"], config["schema"], config["output_directory"])


    """
    Total:	313,576,137	+/-10,365
Under 18 years:	73,475,378	+/-8,041
With one type of health insurance coverage:	65,109,722	+/-25,755
With employer-based health insurance only	34,410,287	+/-141,052
With direct-purchase health insurance only	3,852,219	+/-26,651
With Medicare coverage only	202,751	+/-6,454
With Medicaid/means-tested public coverage only	25,400,518	+/-165,975
With TRICARE/military health coverage only	1,217,945	+/-10,014
With VA Health Care only	26,002	+/-1,829
With two or more types of health insurance coverage:	4,032,588	+/-30,220
With employer-based and direct-purchase coverage	737,085	+/-11,238
With employer-based and Medicare coverage	25,752	+/-1,612
With Medicare and Medicaid/means-tested public coverage	167,565	+/-4,321
Other private only combinations	355,361	+/-5,796
Other public only combinations	13,168	+/-1,012
Other coverage combinations	2,733,657	+/-21,451
No health insurance coverage	4,333,068	+/-32,505
18 to 34 years:	72,771,471	+/-13,992
With one type of health insurance coverage:	54,202,794	+/-70,603
With employer-based health insurance only	37,925,831	+/-76,330
With direct-purchase health insurance only	5,787,275	+/-29,139
With Medicare coverage only	229,426	+/-4,352
With Medicaid/means-tested public coverage only	9,191,150	+/-36,524
With TRICARE/military health coverage only	864,298	+/-7,638
With VA Health Care only	204,814	+/-3,670
With two or more types of health insurance coverage:	3,675,873	+/-30,027
With employer-based and direct-purchase coverage	1,224,407	+/-14,066
With employer-based and Medicare coverage	48,835	+/-1,499
With Medicare and Medicaid/means-tested public coverage	376,465	+/-4,964
Other private only combinations	307,982	+/-5,123
Other public only combinations	38,731	+/-1,547
Other coverage combinations	1,679,453	+/-17,420
No health insurance coverage	14,892,804	+/-102,653
35 to 64 years:	122,454,553	+/-11,795
With one type of health insurance coverage:	94,934,763	+/-126,322
With employer-based health insurance only	71,416,584	+/-189,264
With direct-purchase health insurance only	9,715,444	+/-23,649
With Medicare coverage only	2,110,731	+/-19,432
With Medicaid/means-tested public coverage only	9,828,956	+/-63,463
With TRICARE/military health coverage only	1,153,106	+/-9,541
With VA Health Care only	709,942	+/-7,820
With two or more types of health insurance coverage:	10,452,354	+/-45,356
With employer-based and direct-purchase coverage	2,445,849	+/-28,185
With employer-based and Medicare coverage	687,338	+/-6,606
With direct-purchase and Medicare coverage	376,799	+/-4,727
With Medicare and Medicaid/means-tested public coverage	2,372,431	+/-17,871
Other private only combinations	599,045	+/-7,686
Other public only combinations	303,901	+/-3,811
Other coverage combinations	3,666,991	+/-27,096
No health insurance coverage	17,067,436	+/-156,091
65 years and over:	44,874,735	+/-6,103
With one type of health insurance coverage:	13,064,961	+/-37,633
With employer-based health insurance only	1,028,312	+/-7,954
With direct-purchase health insurance only	175,021	+/-3,188
With Medicare coverage only	11,818,560	+/-32,937
With TRICARE/military health coverage only	13,306	+/-927
With VA Health Care only	29,762	+/-1,226
With two or more types of health insurance coverage:	31,402,836	+/-44,454
With employer-based and direct-purchase coverage	39,755	+/-1,519
With employer-based and Medicare coverage	8,997,503	+/-19,463
With direct-purchase and Medicare coverage	8,840,780	+/-17,703
With Medicare and Medicaid/means-tested public coverage	3,798,418	+/-22,750
Other private only combinations	6,579	+/-542
Other public only combinations	1,039,999	+/-6,772
Other coverage combinations	8,679,802	+/-49,029
No health insurance coverage	406,938	+/-7,734
    """