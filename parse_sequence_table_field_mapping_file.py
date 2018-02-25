__author__ = 'janos'

"""
Finding where an element is stored in the Census ACS Summary files is a challenge.
"""


"""
File ID,Table ID,Sequence Number,Line Number,Start Position,Total Cells in Table,Total Cells in Sequence,Table Title,Subject Area
ACSSF,B00001,1, ,7,1 CELL, ,UNWEIGHTED SAMPLE COUNT OF THE POPULATION,Unweighted Count
ACSSF,B00001,1, , ,, ,Universe:  Total population,
ACSSF,B00001,1,1, ,, ,Total,
ACSSF,B00002,1, ,8,1 CELL,2,UNWEIGHTED SAMPLE HOUSING UNITS,Unweighted Count
ACSSF,B00002,1, , ,, ,Universe:  Housing units,
ACSSF,B00002,1,1, ,, ,Total,
ACSSF,B01002B,3, ,106,3 CELLS, ,MEDIAN AGE BY SEX (BLACK OR AFRICAN AMERICAN ALONE),Age-Sex
ACSSF,B01002B,3, , ,, ,Universe:  Black or African American alone,
ACSSF,B01002B,3,0.5, ,, ,Median age --,
ACSSF,B01002B,3,1, ,, ,Total:,
ACSSF,B01002B,3,2, ,, ,Male,
ACSSF,B01002B,3,3, ,, ,Female,
"""

"""
    The goal is to transform this:
"""

{"B00001": {"title": "UNWEIGHTED SAMPLE COUNT OF THE POPULATION", "starting_position": 7,
            "fields": [{"sequence": 1, "relative_position": 1,
                      "actual_position": 7,
                      "title": "Total",
                      "type": "Universe:  Total population"}]}}

import json
import csv
import re
import sys


def open_csv_file(file_name, mode="w"):

    ver_info = sys.version_info[0]
    if ver_info == 2:
        return open(file_name, mode=mode + "b")
    else:
        return open(file_name, newline="", mode=mode)


def main(sequence_number_to_csv_file_name):
    """
    Parse the CSV file which lists the structure and transform into a JSON representative

    Parser States:
        Header
        New Table
        Universe Defined
        Measurement Context
        Reading Fields
        Parent Category
        New Measure
    """

    # TODO: Context mapping is wrong, see B25033 -- Should just be Total
    """
    {
                "context path": [
                    "Total",
                    "Owner occupied"
                ],
                "field name": "Renter occupied",
                "relative position": 8,
                "row": 21600,
                "table position": 244
            },
    """

    table_sequence_mappings = {}

    with open_csv_file(sequence_number_to_csv_file_name, "r") as fc:
        csv_reader = csv.reader(fc)
        state = "Header"
        field_position_mappings = {}

        # table_name = None
        context_path = None
        set_universe = None
        i = 0
        for row in csv_reader:

            if state == "Header":
                header = row
                for j in range(len(header)):
                    field_position_mappings[header[j]] = j
                state = "New Table"
                context_path = []
            else:
                row_dict = {}
                for key in field_position_mappings:
                    if sys.version_info[0] == 2:
                        row_dict[key] = row[field_position_mappings[key]].decode('utf8', errors="replace")
                    else:
                        row_dict[key] = str(row[field_position_mappings[key]])

                if state == "New Table":
                    if i == 1:
                        table_name = row_dict["Table Title"]
                        table_id = row_dict["Table ID"]
                        subject_area = row_dict["Subject Area"]
                        sequence_number = row_dict["Sequence Number"]
                        start_position = row_dict["Start Position"]

                        table_sequence_mappings[table_id] = {
                            "table name": table_name,
                            "table id": table_id,
                            "subject area": subject_area,
                            "sequence number": sequence_number,
                            "start position": int(start_position),
                            "universe": None,
                            "fields": []
                        }

                    context_path = []
                    state = "Universe Defined"

                else:
                    if state == "Universe Defined":
                        if not len(row_dict["Line Number"].strip()):
                            set_universe = row_dict["Table Title"]
                        state = "Reading Fields"

                    if state == "Reading Fields":

                        if len(row_dict["Subject Area"].strip()):
                                state = "New Table"
                                table_name = row_dict["Table Title"]
                                table_id = row_dict["Table ID"]
                                subject_area = row_dict["Subject Area"]
                                sequence_number = row_dict["Sequence Number"]
                                start_position = int(row_dict["Start Position"])

                                table_sequence_mappings[table_id] = {
                                    "table name": table_name,
                                    "table id": table_id,
                                    "subject area": subject_area,
                                    "sequence number": sequence_number,
                                    "start position": start_position,
                                    "universe": None,
                                    "fields": []
                                }
                                context_path = []

                        elif len(row_dict["Line Number"].strip()):
                            field_name = row_dict["Table Title"]
                            modified_field_name = field_name
                            if modified_field_name[-1] == ":":
                                modified_field_name = modified_field_name[:-1]
                            position = row_dict["Line Number"]

                            if "." not in position:
                                table_sequence_mappings[table_id]["fields"] += [
                                    {"row": i, "context path": list(context_path), "relative position": int(position),
                                     "field name": modified_field_name,
                                     "table position": int(start_position) + int(position) - 1}]
                            else:
                                pass # Record medians

                            if field_name[-1] == ":":
                                if len(context_path) > 1:
                                    context_path.pop()
                                context_path += [field_name[:-1]]

                            if len(context_path):
                                    if field_name == "Other " + context_path[-1]:
                                        context_path.pop()
                                    else:
                                        rcase1 = re.compile(r"(.+) \(.+\)")

                                        match = rcase1.match(context_path[-1])
                                        if match is not None:
                                            groups = match.groups()
                                            if field_name == "Other " + groups[0]:
                                                context_path.pop()
            i += 1

    with open("./support_files/table_number_to_sequence_number.json", "w") as fw:
        json.dump(table_sequence_mappings, fw,  sort_keys=True, indent=4, separators=(',', ': '))


if __name__ == "__main__":

    try:
        import config
    except ImportError:
        import config_example as config

    main(config.sequence_number_table_csv_file)