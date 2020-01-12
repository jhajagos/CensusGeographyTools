__author__ = 'janos'
import json
import csv
import glob
import os
import sys
import argparse

"""
Export an ACS variable identified "B16002" across all geographies that are identified.
"""


def open_csv_file(file_name, mode="w"):

    ver_info = sys.version_info[0]
    if ver_info == 2:
        return open(file_name, mode=mode + "b")
    else:
        return open(file_name, newline="", mode=mode)


def pad_number(number, length):
    """To left pad a number with zeros"""

    string_number = str(number)
    number_of_zeros = length - len(string_number)
    if number_of_zeros >= 0:
        return "0" * number_of_zeros + string_number
    else:
        return string_number


def main(table_to_publish, directory, geographic_unit="NY", reference_year="2013", years_covered="5", iteration="000",
         file_types=["e", "m"], abridged=False, sequence_file_name="./support_files/table_number_to_sequence_number.json"):

    """
        file_types :
            e=estimate, m=margin of error
        reference year
        period_covered="5" is the 5 year file
        iteration="000"

        abridged:
            Supports two formats for the table. A full version which has repetitive details and an abridged version which has
            only unique elements.
    """

    print("Loading table to sequence file mappings")

    with open(sequence_file_name) as fj:
        table_number_to_sequence_number = json.load(fj)

    print("Loading geographic file which provides mapping")
    geographic_data_file = glob.glob(os.path.join(directory, "g*.json"))[0]
    print(geographic_data_file)
    with open(geographic_data_file, "r") as f:
        geographic_data = json.load(f)

    variable_mapping_information_file = os.path.join(directory, "acs_files_generated.json")
    if os.path.exists(variable_mapping_information_file):
        with open(variable_mapping_information_file) as f:
            variable_mapping_information_dict = json.load(f)
    else:
        variable_mapping_information_dict = {}

    if table_to_publish in table_number_to_sequence_number:
        table_data = table_number_to_sequence_number[table_to_publish]

        sequence_number = table_data["sequence number"]
        sequence_number_string = pad_number(sequence_number, 4)

        suffix_file = reference_year + years_covered + geographic_unit.lower() + sequence_number_string + iteration + ".txt"

        if table_to_publish in variable_mapping_information_dict:
            pass
        else:
            variable_mapping_information_dict[table_to_publish] = {}

        for file_type in file_types:
            if abridged:
                abridged = "a_"
                abridged_key = "abridged"
            else:
                abridged = ""
                abridged_key = "unabridged"

            base_version_table_name = abridged + file_type + reference_year + years_covered + geographic_unit.lower() + table_to_publish
            file_to_write = os.path.join(directory, base_version_table_name + ".csv")
            file_to_read = os.path.join(directory, file_type + suffix_file)

            FIELD_LAYOUT = ["FILEDID", "FILETYPE", "STUSAB", "CHARITER", "SEQUENCE", "LOGRECNO"]
            field_dict = {}

            publishing_data_dict = {"file_name": os.path.abspath(file_to_write), "file_type": file_type,
                                    "reference_year": reference_year,
                                    "period_covered": years_covered, "geographic_unit": geographic_unit,
                                    "table_to_publish": table_to_publish,
                                    "geographic_file": os.path.abspath(geographic_data_file),
                                    "sequence_file": os.path.abspath(sequence_file_name),
                                    "base_version_table_name": base_version_table_name
                                    }
            if file_type in variable_mapping_information_dict[table_to_publish]:
                pass
            else:
                variable_mapping_information_dict[table_to_publish][file_type] = {}

            variable_mapping_information_dict[table_to_publish][file_type][abridged_key] = publishing_data_dict

            for i in range(len(FIELD_LAYOUT)):
                field_dict[FIELD_LAYOUT[i]] = i

            if os.path.exists(file_to_read):
                print("Census file exists")

                with open_csv_file(file_to_read, "r") as f:
                    cr = csv.reader(f)
                    print("Writing CSV file '%s'" % file_to_write)
                    with open_csv_file(file_to_write, "w") as fw:
                        cwr = csv.writer(fw)

                        if abridged:
                            cwr.writerow(["geoid", "table_id", "relative_position", "value"])
                        else:
                            cwr.writerow(["geoid", "geo_name", "table_id", "table_name", "subject_area",
                                          "relative_position", "context_path", "file_type", "field_name", "value"])

                        i = 0
                        for row in cr:
                            if i == 0:
                                publishing_data_dict["table_name"] = table_data["table name"]
                                publishing_data_dict["subject_area"] = table_data["subject area"]
                                publishing_data_dict["geographic_unit"] = geographic_unit
                            i += 1

                            logical_record = row[field_dict["LOGRECNO"]]
                            if logical_record in geographic_data:
                                geographic = geographic_data[logical_record]
                                geoid = geographic["GEOID"]
                                row_to_write_template = [geoid]
                                if not abridged:
                                    geo_name = geographic["NAME"]
                                    row_to_write_template += [geo_name]

                                row_to_write_template += [table_to_publish]

                                if not abridged:
                                    if "subject area" in table_data:
                                        subject = table_data["subject area"]
                                    else:
                                        subject = ""

                                    if "table name" in table_data:
                                        table_name = table_data["table name"]
                                    else:
                                        table_name = ""

                                    row_to_write_template += [table_name, subject]

                                for field in table_data["fields"]: #Iterate through all fields associated with a table
                                    row_to_write = list(row_to_write_template)
                                    relative_position = field["relative position"]
                                    row_to_write += [relative_position]
                                    if not abridged:
                                        context_path = "|".join(field["context path"])
                                        field_name = field["field name"]
                                        row_to_write += [context_path, file_type, field_name]

                                    absolute_position = field["table position"] - 1
                                    try:
                                        value = row[absolute_position]
                                    except(IndexError):
                                        print(table_data)
                                        print(row, absolute_position, relative_position, field["table position"])
                                        raise

                                    row_to_write += [value]
                                    cwr.writerow(row_to_write)

            else:
                print("File '%s' does not exist" % file_to_read)
    else:
        print("Table '%s' is not in the mapping file" % table_to_publish)

    with open(variable_mapping_information_file, "w") as fw:
        json.dump(variable_mapping_information_dict, fw, sort_keys=True, indent=4, separators=(',', ': '))


if __name__ == "__main__":

    arg_parse_obj = argparse.ArgumentParser(
        description="Parses geographic file for use in processing Census data so we can map names")
    arg_parse_obj.add_argument("-c", "--config-json-filename", dest="config_json_filename",
                               default="config_example.json")
    arg_obj = arg_parse_obj.parse_args()

    with open(arg_obj.config_json_filename) as f:
        config = json.load(f)

    acs_fields = config["acs_fields_to_export"]
    for acs_field in acs_fields:
        main(acs_field, config["geography_directory"], geographic_unit=config["geographic_unit"],
             reference_year=config["reference_year"], years_covered=config["years_covered"], abridged=False)

        main(acs_field, config["geography_directory"], geographic_unit=config["geographic_unit"],
             reference_year=config["reference_year"], years_covered=config["years_covered"], abridged=True)