__author__ = 'janos'


"""
    This script creates a PostGreSQL database load script. The assumption is that the files
    will be loaded on a local instance. Where the files are located are locally
    accessible.

"""


import sys
import json

def main(acs_json_file, schema="acs_summary", abridged=False):
    if abridged:
        abridged_status = "abridged"
    else:
        abridged_status = "unabridged"

    with open(acs_json_file, "r") as fj:
        acs_dict = json.load(fj)

    sql_script = ""
    for table in acs_dict:
        table_info = acs_dict[table]["e"][abridged_status]
        print(table_info)

        header_text = "goeid	geo_name	table_id	table_name	subject_area	relative_position	context_path	file_type	field_name	value_txt	value_int	value_double"

        data_types_map = {'goeid': "varchar(63)", 'geo_name': "varchar(255)", 'table_id': "varchar(16)", 'table_name': "varchar(255)",
                          'subject_area': "varchar(255)",
                          'relative_position': "int2", 'context_path': "varchar(255)",
                          'file_type': "varchar(16)", 'field_name': "varchar(255)",
                          'value_txt': "varchar(255)", 'value_int': "integer", 'value_double': "double"}

        header = header_text.split('\t')

        base_table_name = table_info["base_version_table_name"]
        full_base_table_name = '"%s"."%s"' % (schema, base_table_name)
        create_table_sql = 'create table  %s (\n' % full_base_table_name

        for field in header:
            create_table_sql += "    %s %s,\n" % (field, data_types_map[field])

        sql_script += create_table_sql[:-2] + ");\n\n"
        print(sql_script)
        exit()


if __name__ == "__main__":
    main("./test/acs_files_generated.json")
    exit()
    if len(sys.argv) == 1:
        print("Help")
    elif len(sys.argv) == 2:
        if sys.argv[1] == "test":
            main("./test/acs_files_generated.json")


