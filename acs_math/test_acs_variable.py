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

        with open("./config_test.json") as f:
            config = json.load(f)

        connection_uri = config["connection_uri"]
        engine = sa.create_engine(connection_uri)

        connection = engine.connect()

        self.acs_scope = av.ACSScope(connection, "e", 2016, 5, "US")

        self.geo_restriction = av.GeographicRestriction("north_central", self.zcta5)

        self.variable_factory = av.ACSVariableFactory(self.acs_scope, self.geo_restriction)


    def test_setup(self):

        self.assertEqual("e20165us", self.acs_scope.table_prefix)

    def test_variables(self):

        total = self.variable_factory.new("B01001", 1)
        total_male = self.variable_factory.new("B01001", 2)
        #total_female = self.variable_factory.new("B01001", 26)

        fraction_male = total_male / total

        self.assertIsNotNone(fraction_male)

        print(fraction_male.series.sort_values(ascending=False))


if __name__ == '__main__':
    unittest.main()
