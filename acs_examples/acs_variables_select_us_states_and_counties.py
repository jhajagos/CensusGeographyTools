import json
from acs_variable_example import generate_acs_summary
from acs_variable import ACSScope, GeographicRestriction
import sqlalchemy as sa
import pprint
import os

def main(config, output_directory="./"):

    state_query_dict = {
        "tx": """select distinct geoid, geo_name from acs_summary_2018tx."e20185txB00001" where geoid like '05000US48%%' order by geo_name""",
        "fl": """select distinct geoid, geo_name from acs_summary_2018fl."e20185flB00001" where geoid like '05000US12%%' order by geo_name"""
    }

    engine = sa.create_engine(config["connection_uri"])
    schema_dict = config["schemas"]
    with engine.connect() as connection:
        state_list = ["tx", "fl"]

        for state in state_list:
            state_query = state_query_dict[state]
            print(state_query)
            cursor = connection.execute(state_query)
            geo_name_list = []
            for row in cursor:
                geo_name_list += [row["geo_name"]]

            pprint.pprint(geo_name_list)

            geo_restriction = GeographicRestriction(state + "_geonames", geo_name_list)
            acs_scope_state_county = ACSScope(connection, "e", 2018, 5, state, schema_dict["acs_" + state])

            state_export = generate_acs_summary(acs_scope_state_county, geo_restriction, "geo_name")
            state_export.write_to_csv(os.path.join(output_directory, state + "_census_county.csv"))


if __name__ == "__main__":
    with open("./config.json") as f:
        config = json.load(f)

    pprint.pprint(config)
    main(config)