CensusGeographyTools
====================

A set of tools for working with the American Community Survey summary files from the 
US Census Bureau. The ACS provides detailed demographic and social information on 
communities and geographic regions.

Start here:

https://www.census.gov/programs-surveys/acs/data/summary-file.html

For 2018:

```bash
# US Data Containing ZCTA5 (Zip codes)
wget https://www2.census.gov/programs-surveys/acs/summary_file/2018/data/5_year_by_state/UnitedStates_All_Geographies_Not_Tracts_Block_Groups.zip

# New York Counties
wget https://www2.census.gov/programs-surveys/acs/summary_file/2018/data/5_year_by_state/NewYork_All_Geographies_Not_Tracts_Block_Groups.zip

# New York Block Groups and Tracks
wget https://www2.census.gov/programs-surveys/acs/summary_file/2018/data/5_year_by_state/NewYork_Tracts_Block_Groups_Only.zip
```

Get correct version of the file at:

https://www.census.gov/programs-surveys/acs/technical-documentation/summary-file-documentation.html

```bash
wget https://www2.census.gov/programs-surveys/acs/summary_file/2018/documentation/user_tools/ACS_5yr_Seq_Table_Number_Lookup.csv
```

Save this file as CSV file. Make a non-version controlled version of "config_example.py" called "config.py"

Edit "config.json" set the variable `sequence_number_table_csv_file` in the file equal to "../support_files/ACS_5yr_Seq_Table_Number_Lookup.csv".

Run the script:

```bash
python parse_sequence_table_field_mapping_file.py
```

Set in config.json the data directory where your downloaded files are.

Run the script `process_geographic_file.py` to generate needed JSON files which are used to annotate files with geographic information.
```bash
python process_geographic_file.py -c ./config_2018_ny_tract_bg.json
```

This step writes a CSV file for each selected variable. Large amount of data is generated during this step.
```bash
python publish_summary_census_file.py -c ./config_2018_ny_tract_bg.json
```

```bash
python3 generate_postgres_db_load_script.py -f /census/acs/2018/Full_UnitedStates_All_Geographies_Not_Tracts_Block_Groups/TX/acs_files_generated.json \
-s acs_summary_2018tx -p -t bmi_clinical_extended
```

```bash
psql -h bmi-clinical-informatics-p1 -U jhajagos bmi_clinical < acs_summary_2018tx_load.sql
```

where the acs_files_generated.json points to the directory to load.
