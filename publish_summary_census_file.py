__author__ = 'janos'
import json
import csv
import glob
import os

"""
Export an ACS variable identified "B16002" across all geographies that are identified.
"""


def pad_number(number, length):
    """To left pad a number with zeros"""

    string_number = str(number)
    number_of_zeros = length - len(string_number)
    if number_of_zeros >= 0:
        return "0" * number_of_zeros + string_number
    else:
        return string_number


def main(table_to_publish, directory, state="NY", reference_year="2012", period_covered="5", iteration="000",
         file_types=["e", "m"], abridged=False):

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
    with open("./support_files/table_number_to_sequence_number.json") as fj:
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

        suffix_file = reference_year + period_covered + state.lower() + sequence_number_string + iteration + ".txt"

        if table_to_publish in variable_mapping_information_dict:
            pass
        else:
            variable_mapping_information_dict[table_to_publish] = {}

        for file_type in file_types:
            if abridged:
                abridged = "a_"
            else:
                abridged = ""

            file_to_write = os.path.join(directory, abridged + file_type + reference_year + period_covered + state.lower() + table_to_publish + ".csv")
            file_to_read = os.path.join(directory, file_type + suffix_file)

            FIELD_LAYOUT = ["FILEDID", "FILETYPE", "STUSAB", "CHARITER", "SEQUENCE", "LOGRECNO"]
            field_dict = {}

            publishing_data_dict = {"file_name": file_to_write, "file_type": file_type, "reference_year": reference_year,
                                    "period_covered": period_covered, "state": state,
                                    "table_to_publish": table_to_publish}

            variable_mapping_information_dict[table_to_publish][file_type] = publishing_data_dict

            for i in range(len(FIELD_LAYOUT)):
                field_dict[FIELD_LAYOUT[i]] = i

            if os.path.exists(file_to_read):
                print("Census file exists")

                with open(file_to_read, "r") as f:
                    cr = csv.reader(f)
                    print("Writing CSV file '%s'" % file_to_write)
                    with open(file_to_write, "wb") as fw:
                        cwr = csv.writer(fw)

                        if abridged:
                            cwr.writerow(["geoid", "table_id", "relative_position", "value"])
                        else:
                            cwr.writerow(["goeid", "geo_name", "table_id", "table_name", "subject area",
                                          "relative_position", "context_path", "file_type", "field_name", "value"])

                        for row in cr:
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
                                    except IndexError:
                                        print(table_data)
                                        print(row, absolute_position, relative_position, field["table position"])
                                        raise

                                    row_to_write += [value]
                                    cwr.writerow(row_to_write)

            else:
                print("File '%s' does not exist" % file_to_read)
    else:
        print("Table '%s' not in mapping file" % table_to_publish)

    with open(variable_mapping_information_file, "w") as fw:
        json.dump(variable_mapping_information_dict, fw, sort_keys=True, indent=4, separators=(',', ': '))

if __name__ == "__main__":

    try:
        import config
    except ImportError:
        import config_example as config

    acs_fields = config.acs_fields_to_export
    for acs_field in acs_fields:
         main(acs_field, config.geography_directory)