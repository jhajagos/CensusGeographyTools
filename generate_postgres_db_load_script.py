__author__ = "janos"

"""
    This script creates a PostGreSQL database load script. The assumption is that the files
    will be loaded on a local instance. Where the files are located are locally
    accessible.
"""

import json
import os
import argparse


def main(acs_json_file, schema=None, abridged=False, load_estimates_only=True, psql_load_script=False, table_space=None):

    if table_space is not None:
        table_space_str = " tablespace %s "
    else:
        table_space_str = ""

    if abridged:
        abridged_status = "abridged"
    else:
        abridged_status = "unabridged"

    with open(acs_json_file, "r") as fj:
        acs_dict = json.load(fj)

    sql_script = ""
    i = 0

    sql_script += """drop table if exists %s.exported_acs_tables; \n""" % schema
    sql_script += """create table %s.exported_acs_tables (
  table_id varchar(15),
  table_name varchar(255),
  file_type varchar(1),
  reference_year varchar(4),
  period_covered varchar(1),
  base_version_table_name varchar(255),
  geographic_unit varchar(255),
  created_on timestamp
  ) ;\n\n""" % (schema, table_space_str)

    for table in acs_dict:
        estimate_table_info = acs_dict[table]["e"][abridged_status]
        margin_of_error_table_info = acs_dict[table]["m"][abridged_status]

        if load_estimates_only:
            tables_to_load = [estimate_table_info]
        else:
            tables_to_load = [estimate_table_info, margin_of_error_table_info]

        for table_info in tables_to_load:

            table_info["table_id"] = table

            split_table_name = table_info["table_name"].split("'")
            table_info["table_name_escaped"] = "''".join(split_table_name)

            sql_script += "insert into %s.exported_acs_tables \n   (" % schema
            sql_script += "table_id, table_name, file_type, reference_year, period_covered, base_version_table_name, geographic_unit, created_on)"
            sql_script += "\n   values ('%(table_id)s','%(table_name_escaped)s','%(file_type)s', '%(reference_year)s', '%(period_covered)s', '%(base_version_table_name)s', '%(geographic_unit)s', now());\n\n" % table_info

            header_text = "geoid	geo_name	table_id	table_name	subject_area	relative_position	context_path	file_type	field_name	str_value"

            data_types_map = {"geoid": "varchar(63)", "geo_name": "varchar(255)", "table_id": "varchar(16)",
                              "table_name": "varchar(255)",
                              "subject_area": "varchar(255)",
                              "relative_position": "int2", "context_path": "varchar(255)",
                              "file_type": "varchar(16)", "field_name": "varchar(255)",
                              "str_value": "varchar(255)",
                              "value_txt": "varchar(255)", "value_int": "integer", "value_float": "float"}

            header = header_text.split('\t')

            base_table_name = table_info["base_version_table_name"]
            full_base_table_name = '"%s"."%s"' % (schema, base_table_name)
            create_table_sql = "drop table if exists %s;\n" % full_base_table_name
            create_table_sql += 'create table  %s (\n' % full_base_table_name

            for field in header:
                create_table_sql += "    %s %s,\n" % (field, data_types_map[field])

            sql_script += create_table_sql[:-2] + ") %s;\n\n" % table_space_str

            full_path_file_name = table_info["file_name"]
            base_directory, file_name = os.path.split(full_path_file_name)

            if psql_load_script:
                sql_script += "\\copy "
            else:
                sql_script += "copy "

            sql_script += """ %s from '%s' WITH DELIMITER ',' CSV HEADER QUOTE '"';\n\n""" % (full_base_table_name, os.path.abspath(full_path_file_name))

            sql_script += """alter table %s add numeric_value double precision;\n""" % full_base_table_name
            sql_script += """update %s set numeric_value = case when str_value = '.' then NULL else cast(str_value as double precision) end;\n""" % full_base_table_name
            sql_script += """alter table %s add geoid_tiger varchar(15);\n""" % full_base_table_name
            sql_script += """update %s set geoid_tiger = substring(geoid from 8);\n""" % full_base_table_name

            sql_script += "create index idx_%s_geoid_tiger on %s(geoid_tiger);\n" % (base_table_name, full_base_table_name)
            sql_script += "create index idx_%s_str_value on %s(str_value);\n" % (base_table_name, full_base_table_name)
            sql_script += "create index idx_%s_relative_position on %s(relative_position);\n" % (base_table_name, full_base_table_name)
            sql_script += "create index idx_%s_numeric_value on %s(numeric_value);\n" % (base_table_name, full_base_table_name)
            sql_script += "create index idx_%s_field_name on %s(field_name);\n\n" % (base_table_name, full_base_table_name)

        i += 1

        if i % 100 == 0:
            print("Processed %s tables" % i)

    with open(os.path.join(base_directory, schema + "_load.sql"), "w") as fw:
        fw.write(sql_script)


if __name__ == "__main__":

    arg_parse_obj = argparse.ArgumentParser(
        description="Creates load script for ACS census data to PostGres")

    arg_parse_obj.add_argument("-f", "--acs_json_filename", dest="acs_json_filename",
                               required=True)

    arg_parse_obj.add_argument("-s", "--schema-name", dest="schema_name", required=True)

    arg_parse_obj.add_argument("-a", "--abridged", dest="abridged", default=False, action="store_true",
                               help="Load abridged version without labels")

    arg_parse_obj.add_argument("-p", "--psql-load-script", default=False, dest="psql_load_script", action="store_true",
                               help="Writes load scripts for psql command load")

    arg_parse_obj.add_argument("-t", "--table-space", dest="table_space", default=None, help="Specify a tablespace to load data in")

    arg_obj = arg_parse_obj.parse_args()
    main(arg_obj.acs_json_filename, schema=arg_obj.schema_name, abridged=arg_obj.abridged,
         psql_load_script=arg_obj.psql_load_script, table_space=arg_obj.table_space)