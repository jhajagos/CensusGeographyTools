import unittest
import acs_variable as av
import json
import sqlalchemy as sa

class MyTestCase(unittest.TestCase):

    def setUp(self):

        self.zcta5 = [
            "11720",
            "11727",
            "11733",
            "11745",
            "11754",
            "11755",
            "11764",
            "11766",
            "11767",
            "11776",
            "11777",
            "11778",
            "11779",
            "11780",
            "11784",
            "11786",
            "11787",
            "11788",
            "11789",
            "11790",
            "11794",
            "11953",
            "11961"
        ]

        self.geo_restriction = av.GeographicRestriction("north_central", self.zcta5)
        with open("./config_test.json") as f:
            config = json.load(f)

        connection_uri = config["connection_uri"]
        engine = sa.create_engine(connection_uri)

        connection = engine.connect()

        self.acs_scope = av.ACSScope(connection, "e", 2016, 5, "US")

    def test_setup(self):

        self.assertEqual("e20165us", self.acs_scope.table_prefix)

    def test_variables(self):

        variable_factory = av.ACSVariableFactory(self.acs_scope, None)

        total = variable_factory.new("B01001", 1)
        total_male = variable_factory.new("B01001", 2)
        total_female = variable_factory.new("B01001", 26)

        fraction_male = total_male / total

        self.assertIsNotNone(fraction_male)

        one_minus_fraction_male = av.ACSConstant(1) - fraction_male

        fraction_female = total_female / total

        export = av.ACSExport([("total population", total),
                      ("total_male", total_male),
                      ("fraction_male", fraction_male),
                      ("total_female", total_female),
                      ("fraction_female", fraction_female)
                      ])

        export.df.to_csv("test.csv")

    def test_variables_with_geo_restriction(self):
        variable_factory = av.ACSVariableFactory(self.acs_scope, self.geo_restriction,
                                                 geo_restriction_refresh=True, geo_field='geoid_tiger')
        total = variable_factory.new("B01001", 1)
        total_male = variable_factory.new("B01001", 2)

        self.assertEqual(22, len(total_male.series))


if __name__ == '__main__':
    unittest.main()
