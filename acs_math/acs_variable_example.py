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
                               ("fraction_65_plus", fraction_65_plus)
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