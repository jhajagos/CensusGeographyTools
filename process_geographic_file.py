__author__ = 'janos'

import csv
import json
import os
import sys
import argparse

def open_csv_file(file_name, mode="w"):

    ver_info = sys.version_info[0]
    if ver_info == 2:
        return open(file_name, mode=mode + "b")
    else:
        return open(file_name, newline="", mode=mode)


def main(path_to_geographic_file, path_to_template_file):

    with open_csv_file(path_to_template_file, "r") as f: # This is just a template for the header
        cvr = csv.reader(f)

        if sys.version_info[0] == 2:
            header = cvr.next()
            header_description = cvr.next() #We do not need this
        else:
            header = cvr.__next__()
            header_description = cvr.__next__()

    with open(path_to_geographic_file, "r", errors="replace") as fc:
        cvd = csv.DictReader(fc, header)

        logical_record_dict = {}

        for row_dict in cvd:
            logical_record_id = row_dict["LOGRECNO"]
            slim_row_dict = {}
            for key in row_dict:
                if len(row_dict[key]):
                    if sys.version_info[0] == 2:
                        slim_row_dict[key] = row_dict[key].decode("utf8", errors="replace")
                    else:
                        slim_row_dict[key] = str(row_dict[key])

            logical_record_dict[logical_record_id] = slim_row_dict

    with open(path_to_geographic_file + ".json", "w") as fw:
        json.dump(logical_record_dict, fw, sort_keys=True, indent=4, separators=(',', ': '))


if __name__ == "__main__":

    arg_parse_obj = argparse.ArgumentParser(description="Parses geographic file for use in processing Census data so we can map names")
    arg_parse_obj.add_argument("-c", "--config-json-filename", dest="config_json_filename", default="config_2018_ny_tract_bg.json")
    arg_obj = arg_parse_obj.parse_args()

    with open(arg_obj.config_json_filename) as f:
        config = json.load(f)

    geography_csv_file = os.path.join(config["geography_directory"], config["geography_csv_file"])
    main(geography_csv_file, config["geographic_template_file"])
