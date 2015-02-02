__author__ = 'janos'


"""
Path to the geography download
"""

geography_directory = "H:\\data\\acs_summary_file\\NewYork_Tracts_Block_Groups_Only_2013\\"

geography_csv_file = "g20135ny.csv"

reference_year = "2013"
years_covered = "5"
geographic_unit = "NY"

# No geographic limitations
acs_fields_to_export = ["B00001",
"B00002",
"B01001",
"B01002",
"B01002A",
"B01002B",
"B01002C",
"B01002D",
"B01002E",
"B01002F",
"B01002G",
"B01002H",
"B01002I",
"B01003",
"B02001",
"B02008",
"B02009",
"B02010",
"B02011",
"B02012",
"B02013",
"B03002",
"B03003",
"B08007",
"B08008",
"B08009",
"B08016",
"B08017",
"B08018",
"B08134",
"B08135",
"B08136",
"B08301",
"B08302",
"B08303",
"B09002",
"B09018",
"B09019",
"B09020",
"B11001",
"B11001A",
"B11001B",
"B11001C",
"B11001D",
"B11001E",
"B11001F",
"B11001G",
"B11001H",
"B11001I",
"B11002",
"B11002A",
"B11002B",
"B11002C",
"B11002D",
"B11002E",
"B11002F",
"B11002G",
"B11002H",
"B11002I",
"B11003",
"B11004",
"B11005",
"B11006",
"B11007",
"B11012",
"B11015",
"B11016",
"B12001",
"B14002",
"B14005",
"B14007",
"B14007A",
"B14007B",
"B14007C",
"B14007D",
"B14007E",
"B14007F",
"B14007G",
"B14007H",
"B14007I",
"B15002",
"B15003",
"B15011",
"B15012",
"B16002",
"B16004",
"B17010",
"B17011",
"B17017",
"B17021",
"B19001",
"B19013",
"B19013A",
"B19013B",
"B19013C",
"B19013D",
"B19013E",
"B19013F",
"B19013G",
"B19013H",
"B19013I",
"B19025",
"B19025A",
"B19025B",
"B19025C",
"B19025D",
"B19025E",
"B19025F",
"B19025G",
"B19025H",
"B19025I",
"B19037",
"B19049",
"B19050",
"B19051",
"B19052",
"B19053",
"B19054",
"B19055",
"B19056",
"B19057",
"B19059",
"B19060",
"B19061",
"B19062",
"B19063",
"B19064",
"B19065",
"B19066",
"B19067",
"B19069",
"B19070",
"B19101",
"B19113",
"B19113A",
"B19113B",
"B19113C",
"B19113D",
"B19113E",
"B19113F",
"B19113G",
"B19113H",
"B19113I",
"B19127",
"B19201",
"B19202",
"B19202A",
"B19202B",
"B19202C",
"B19202D",
"B19202E",
"B19202F",
"B19202G",
"B19202H",
"B19202I",
"B19214",
"B19301",
"B19301A",
"B19301B",
"B19301C",
"B19301D",
"B19301E",
"B19301F",
"B19301G",
"B19301H",
"B19301I",
"B19313",
"B19313A",
"B19313B",
"B19313C",
"B19313D",
"B19313E",
"B19313F",
"B19313G",
"B19313H",
"B19313I",
"B20001",
"B20002",
"B20003",
"B20017C",
"B20017D",
"B20017E",
"B20017H",
"B21001",
"B21002",
"B22010",
"B23003",
"B23007",
"B23008",
"B23009",
"B23022",
"B23024",
"B23025",
"B24080",
"B25001",
"B25002",
"B25003",
"B25003A",
"B25003B",
"B25003C",
"B25003D",
"B25003E",
"B25003F",
"B25003G",
"B25003H",
"B25003I",
"B25004",
"B25006",
"B25007",
"B25008",
"B25009",
"B25010",
"B25014",
"B25015",
"B25017",
"B25018",
"B25019",
"B25020",
"B25021",
"B25022",
"B25023",
"B25024",
"B25032",
"B25033",
"B25034",
"B25035",
"B25036",
"B25037",
"B25038",
"B25039",
"B25040",
"B25041",
"B25042",
"B25043",
"B25044",
"B25045",
"B25046",
"B25051",
"B25053",
"B25054",
"B25055",
"B25056",
"B25057",
"B25058",
"B25059",
"B25060",
"B25061",
"B25062",
"B25063",
"B25064",
"B25065",
"B25066",
"B25067",
"B25068",
"B25069",
"B25070",
"B25071",
"B25072",
"B25073",
"B25074",
"B25075",
"B25076",
"B25077",
"B25078",
"B25079",
"B25080",
"B25081",
"B25082",
"B25083",
"B25085",
"B25086",
"B25087",
"B25088",
"B25089",
"B25091",
"B25092",
"B25093",
"B27010",
"B98001",
"B99011",
"B99012",
"B99021",
"B99031",
"B99051",
"B99053",
"B99061",
"B99071",
"B99072",
"B99080",
"B99081",
"B99082",
"B99083",
"B99084",
"B99086",
"B99087",
"B99088",
"B99089",
"B99092",
"B99102",
"B99103",
"B99104",
"B99121",
"B99141",
"B99142",
"B99151",
"B99152",
"B99161",
"B99162",
"B99163",
"B99171",
"B99172",
"B99191",
"B99192",
"B99193",
"B99194",
"B99201",
"B99211",
"B99212",
"B99231",
"B99232",
"B99233",
"B99234",
"B99241",
"B99242",
"B99243",
"B992510",
"B992511",
"B992512",
"B992513",
"B992514",
"B992515",
"B992516",
"B992518",
"B992519",
"B99252",
"B992520",
"B992521",
"B992522",
"B99253",
"B99254",
"B99255",
"B99256",
"B99257",
"B99258",
"C02003",
"C15010",
"C15010A",
"C15010B",
"C15010C",
"C15010D",
"C15010E",
"C15010F",
"C15010G",
"C15010H",
"C15010I",
"C17002",
"C21007",
"C23023",
"C24010",
"C24010A",
"C24010B",
"C24010C",
"C24010D",
"C24010E",
"C24010F",
"C24010G",
"C24010H",
"C24010I",
"C24020",
"C24030",
"C25095"]
#  For testing purposes
# acs_fields_to_export = ["B00001", "B00002", "B01001", "B01002","B16002"]