import sqlalchemy as sa
import datetime
import time
import csv
import pprint
import json
import argparse

def main(acs_json_file_name, connection_uri, variables_to_load=None, geo_conditions=None, acs_estimate_type="e", detail_type="unabridged"):

    engine = sa.create_engine(connection_uri)

    with engine.connect() as connection:

        with open(acs_json_file_name, "r") as f:
            acs_files = json.load(f)

        load_dict = {}
        for variable in variables_to_load:

            if variable in acs_files:

                acs_file = acs_files[variable]
                acs_variable_detail = acs_file[acs_estimate_type][detail_type]

                sql_table_name = acs_variable_detail["base_version_table_name"]
                csv_file_name = acs_variable_detail["file_name"]

                load_dict[csv_file_name] = sql_table_name

                esc_sql_table_name = '"' + sql_table_name + '"'

                connection.execute("drop table if exists %s" % sql_table_name)

                connection.execute("""create table %s 
(
   geoid VARCHAR (63),
   geo_name VARCHAR (255),
   table_id VARCHAR (16),
   table_name VARCHAR (255),
   subject_area VARCHAR (255),
   relative_position integer,
   context_path VARCHAR (255),
   file_type VARCHAR (16),
   field_name VARCHAR (255),
   value VARCHAR(255),
   str_value VARCHAR (255),
   numeric_value float,
   geoid_tiger VARCHAR (15)
)
                """ % esc_sql_table_name)

            else:
                print("ACS variable '%s' not found" % variable)

        load_csv_files_into_db(connection_uri, load_dict, conditions=geo_conditions)

        for file_name in load_dict:

            base_table_name = load_dict[file_name]
            full_base_table_name = '"' + base_table_name + '"'

            sql_script = ""

            sql_script += 'update %s set str_value="value";' % full_base_table_name
            sql_script += """update %s set numeric_value = case when str_value = '.' then NULL else cast(str_value as float) end;\n""" % full_base_table_name

            sql_script += """update %s set geoid_tiger = substr(geoid, 8);\n""" % full_base_table_name

            sql_script += "create index idx_%s_geoid_tiger on %s(geoid_tiger);\n" % (
            base_table_name, full_base_table_name)
            sql_script += "create index idx_%s_str_value on %s(str_value);\n" % (base_table_name, full_base_table_name)
            sql_script += "create index idx_%s_relative_position on %s(relative_position);\n" % (
            base_table_name, full_base_table_name)
            sql_script += "create index idx_%s_numeric_value on %s(numeric_value);\n" % (
            base_table_name, full_base_table_name)
            sql_script += "create index idx_%s_field_name on %s(field_name);\n\n" % (
            base_table_name, full_base_table_name)

            for statement in sql_script.split(";"):
                connection.execute(statement)


def load_csv_files_into_db(connection_string, data_dict, schema=None, delimiter=",",
                           lower_case_keys=True, i_print_update=10000, truncate=False, truncate_long_fields=True,
                           conditions=None
                           ):

    db_engine = sa.create_engine(connection_string)
    db_connection = db_engine.connect()

    table_names = []

    for key in data_dict:
        table_name = data_dict[key]
        if table_name not in table_names:
            if schema:
                table_name = schema + "." + table_name
            table_names += [table_name]

    if truncate:
        for table_name in table_names:
            truncate_sql = "truncate %s" % table_name
            db_connection.execute(truncate_sql)

    meta_data = sa.MetaData(db_connection, schema=schema)
    meta_data.reflect()

    for data_file in data_dict:

        varchar_fields = {}
        table_name = data_dict[data_file]
        if schema:
            table_name = schema + "." + table_name

        table_obj = meta_data.tables[table_name]

        for column in table_obj.columns:
            column_name = column.name
            column_type = table_obj.c[column_name].type

            if "CHAR" in str(column_type):
                varchar_fields[column_name.lower()] = table_obj.c[column_name].type.length

        print("Loading %s" % table_name)

        db_transaction = db_connection.begin()
        try:
            with open(data_file) as f:
                dict_reader = csv.DictReader(f, delimiter=delimiter)
                start_time = time.time()
                elapsed_time = start_time
                i = 0
                for dict_row in dict_reader:
                    cleaned_dict = {}
                    for key in dict_row:
                        if len(dict_row[key]):

                            if "date" in key or "DATE" in key:
                                if "-" in dict_row[key]:
                                    if " " in dict_row[key]:
                                        cleaned_dict[key.lower()] = datetime.datetime.strptime(dict_row[key], "%Y-%m-%d %H:%M:%S")
                                    else:
                                        cleaned_dict[key.lower()] = datetime.datetime.strptime(dict_row[key], "%Y-%m-%d")
                                else:
                                    cleaned_dict[key.lower()] = datetime.datetime.strptime(dict_row[key], "%Y%m%d")
                            else:
                                cleaned_dict[key.lower()] = dict_row[key]

                    if lower_case_keys:
                        temp_cleaned_dict = {}
                        for key in cleaned_dict:
                            if truncate_long_fields:
                                if key.lower() in varchar_fields:
                                    field_length = varchar_fields[key.lower()]
                                    if len(cleaned_dict[key.lower()]) > field_length:
                                        print("Truncating: '%s'" % cleaned_dict[key.lower()])
                                        cleaned_dict[key.lower()] = cleaned_dict[key.lower()][0:field_length]
                            temp_cleaned_dict[key.lower()] = cleaned_dict[key]
                        cleaned_dict = temp_cleaned_dict

                    if conditions is None:
                        insert_data = True

                    else:
                        insert_data = False
                        for condition in conditions:
                            field_key = condition[0]
                            if field_key in cleaned_dict:
                                field_value = cleaned_dict[field_key]
                                if field_value in condition[1]:
                                    insert_data = True

                    if insert_data:
                        s = table_obj.insert(cleaned_dict)
                        try:
                            db_connection.execute(s)
                        except:
                            pprint.pprint(cleaned_dict)
                            raise

                        if i > 0 and i % i_print_update == 0:
                            current_time = time.time()
                            time_difference = current_time - elapsed_time
                            print("Loaded %s total rows at %s seconds per %s rows" % (i, time_difference, i_print_update))
                            elapsed_time = time.time()
                        i += 1

                db_transaction.commit()
                current_time = time.time()
                total_time_difference = current_time - start_time
                print("Loaded %s total row in %s seconds" % (i, total_time_difference))

        except:
            db_transaction.rollback()
            raise


if __name__ == "__main__":

    """This was used to generate a test file for the ACS Math library"""

    arg_parse_obj = argparse.ArgumentParser(description="Used to build a SQLite version of the ACS tables")
    arg_parse_obj.add_argument("-c", "--config-json-filename", dest="config_json_filename",
                               default="config_example.json")
    arg_obj = arg_parse_obj.parse_args()

    with open(arg_obj.config_json_filename) as f:
        config = json.load(f)

    geo_restrictions = [
        '11720',
        '11727',
        '11733',
        '11745',
        '11754',
        '11755',
        '11760',
        '11764',
        '11766',
        '11767',
        '11776',
        '11777',
        '11778',
        '11779',
        '11780',
        '11784',
        '11786',
        '11787',
        '11788',
        '11789',
        '11790',
        '11794',
        '11953',
        '11961',
        '11705',
        '11713',
        '11715',
        '11719',
        '11738',
        '11741',
        '11742',
        '11763',
        '11772',
        '11782',
        '11967',
        '11980',
        '11701',
        '11702',
        '11703',
        '11704',
        '11706',
        '11707',
        '11708',
        '11716',
        '11717',
        '11718',
        '11722',
        '11726',
        '11729',
        '11730',
        '11739',
        '11749',
        '11751',
        '11752',
        '11757',
        '11769',
        '11770',
        '11795',
        '11796',
        '11798',
        '11792',
        '11901',
        '11931',
        '11933',
        '11934',
        '11940',
        '11941',
        '11942',
        '11947',
        '11949',
        '11950',
        '11951',
        '11955',
        '11959',
        '11960',
        '11970',
        '11972',
        '11973',
        '11977',
        '11978',
        '11930',
        '11932',
        '11937',
        '11946',
        '11954',
        '11962',
        '11963',
        '11968',
        '11969',
        '11975',
        '11976',
        '11721',
        '11724',
        '11725',
        '11731',
        '11740',
        '11743',
        '11746',
        '11747',
        '11750',
        '11768',
        '11775',
        '6390',
        '11935',
        '11939',
        '11944',
        '11948',
        '11952',
        '11956',
        '11957',
        '11958',
        '11964',
        '11965',
        '11971',
    ]

    geo_restrictions = ["ZCTA5 " + g for g in geo_restrictions]
    geo_conditions = [("geo_name", geo_restrictions)]

    main(config["geography_directory"] + "acs_files_generated.json", "sqlite:///acs_load.db3", ["B01001", "B02001"], geo_conditions)