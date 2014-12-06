__author__ = 'janos'

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


def main():
    """
    States:
        Header
        New Table
        Universe Defined
        Measurement Context
        Reading Fields
        Parent Category
        New Measure
    """

    table_sequence_mappings = {}

    with open("./support_files/Sequence_Number_and_Table_Number_Lookup.csv", "rb") as fc:
        csv_reader = csv.reader(fc)
        state = "Header"
        field_position_mappings = {}

        # table_name = None
        context_path = None
        set_universe = None
        i = 0
        for row in csv_reader:

            # if i == 100:
            #     import pprint
            #     pprint.pprint(table_sequence_mappings)
            #     exit()
            #print(state,i)
            if state == "Header":
                header = row
                for j in range(len(header)):
                    field_position_mappings[header[j]] = j
                state = "New Table"
                context_path = []
            else:
                row_dict = {}
                for key in field_position_mappings:
                    row_dict[key] = row[field_position_mappings[key]].decode('utf8', errors="replace")

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
                                     "field name": modified_field_name, "table position": int(start_position) + int(position)}]
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
    main()