CensusGeographyTools
====================

A set of tools for working with the summary files from the US Census

Start here:

https://www.census.gov/programs-surveys/acs/data/summary-file.html

For 2016:

https://www2.census.gov/programs-surveys/acs/summary_file/2016/data/5_year_by_state/

```bash
wget https://www2.census.gov/programs-surveys/acs/summary_file/2016/data/5_year_by_state/NewYork_All_Geographies_Not_Tracts_Block_Groups.zip
wget https://www2.census.gov/programs-surveys/acs/summary_file/2016/data/5_year_by_state/NewYork_Tracts_Block_Groups_Only.zip
```


Get correct version of the file at:

https://www.census.gov/programs-surveys/acs/technical-documentation/summary-file-documentation.html

```bash
wget https://www2.census.gov/programs-surveys/acs/summary_file/2016/documentation/user_tools/ACS_5yr_Seq_Table_Number_Lookup.xls
```

Save this file as CSV file. Make a non-version controlled version of "config_example.py" called "config.py"

Edit "config.py" set the variable `sequence_number_table_csv_file` in the file equal to "../support_filesACS_5yr_Seq_Table_Number_Lookup.csv".

Run the script:

```bash
python parse_sequence_table_field_mapping_file.py
```

Set in config.py the data directory where your downloaded files are.

```bash B25033
python
```