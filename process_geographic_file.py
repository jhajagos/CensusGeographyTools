__author__ = 'janos'


import csv
import json
import pprint

def main(path_to_geographic_file):

    with open("./support_files/2012_SFGeoFileTemplate.csv", "r") as f:
        cvr = csv.reader(f)
        header = cvr.next()
        header_description = cvr.next()

    with open(path_to_geographic_file, "r") as fc:
        cvd = csv.DictReader(fc, header)

        logical_record_dict = {}

        for row_dict in cvd:
            logical_record_id = row_dict["LOGRECNO"]
            slim_row_dict = {}
            for key in row_dict:
                if len(row_dict[key]):
                    slim_row_dict[key] = row_dict[key]

            logical_record_dict[logical_record_id] = slim_row_dict

    with open(path_to_geographic_file + ".json", "w") as fw:
        json.dump(logical_record_dict, fw, sort_keys=True, indent=4, separators=(',', ': '))



if __name__ == "__main__":
    main("C:\\Users\\janos\\Downloads\\NewYork_All_Geographies_Tracts_Block_Groups_Only\\g20125ny.csv")