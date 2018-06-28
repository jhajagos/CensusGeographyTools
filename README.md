CensusGeographyTools
====================

A set of tools for working with the American Community Survey summary files from the US Census Bureau. The ACS
provides detailed demographic and social information on communities and geographic regions.

Start here:

https://www.census.gov/programs-surveys/acs/data/summary-file.html

For 2016:

https://www2.census.gov/programs-surveys/acs/summary_file/2016/data/5_year_by_state/

```bash
wget https://www2.census.gov/programs-surveys/acs/summary_file/2016/data/5_year_by_state/NewYork_All_Geographies_Not_Tracts_Block_Groups.zip
wget https://www2.census.gov/programs-surveys/acs/summary_file/2016/data/5_year_by_state/NewYork_Tracts_Block_Groups_Only.zip
```

If you want ZCTA5 (Zip Code Tabulation Area 5 digits) then you need the all geographies file:

```bash
wget https://www2.census.gov/programs-surveys/acs/summary_file/2016/data/5_year_by_state/UnitedStates_All_Geographies_Not_Tracts_Block_Groups.zip
```

Get correct version of the file at:

https://www.census.gov/programs-surveys/acs/technical-documentation/summary-file-documentation.html

```bash
wget https://www2.census.gov/programs-surveys/acs/summary_file/2016/documentation/user_tools/ACS_5yr_Seq_Table_Number_Lookup.xls
```

Save this file as CSV file. Make a non-version controlled version of "config_example.py" called "config.py"

Edit "config.py" set the variable `sequence_number_table_csv_file` in the file equal to "../support_files/ACS_5yr_Seq_Table_Number_Lookup.csv".

Run the script:

```bash
python parse_sequence_table_field_mapping_file.py
```

Set in config.py the data directory where your downloaded files are.

Run the script `process_geographic_file.py` to generate needed JSON files which are used to annotate files with geographic information.
```bash
python process_geographic_file.py
```

This step writes a CSV file for each selected variable. Large amount of data is generated during this step.
```bash
python publish_summary_census_file.py
```

```bash
 python generate_postgres_db_load_script.py ~/data/acs/newyork_bg_tract/acs_files_generated.json
```

where the acs_files_generated.json points to the directory to load.
