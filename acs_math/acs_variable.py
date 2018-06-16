import pandas as pd
import sqlalchemy as sa
import json


class ACSScope(object):

    def __init__(self,  connection, estimate_type="e", estimate_year="2006", estimate_time="5", geo_scope="us"):

        self.connection = connection
        self.meta_data = sa.MetaData(connection)
        self.meta_data.reflect()

        self.estimate_type = estimate_type
        self.estimate_year = str(estimate_year)
        self.estimate_time = str(estimate_time)
        self.geo_scope = geo_scope.lower()

        self.table_prefix = self.estimate_type + self.estimate_year + self.estimate_time + self.geo_scope  # "e20165usC24030"


class GeographicRestriction(object):

    def __init__(self, name, codes):
        self.name = name
        self.codes = codes

    def create(self):
        pass


class ACSVariable(object):

    def __init__(self, variable_name, relative_position, acs_scope, geographic_restriction=None):
        self.variable_name = variable_name
        self.relative_position = relative_position
        self.acs_scope = acs_scope
        self.geographic_restriction = geographic_restriction

    def _get_acs_variable(self):
        pass


class ACSVariableFactory(object):

    def __init__(self, acs_scope, geographic_restriction=None):
        self.acs_scope = acs_scope
        self.geographic_restriction = geographic_restriction

    def new(self, variable_name, relative_position):
        return ACSVariable(variable_name, relative_position, self.acs_scope, self.geographic_restriction)


class ACSExport(object):

    def __init__(self, pairs_with_export):
        pass