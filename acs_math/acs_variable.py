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

    def __truediv__(self, other):
        return ACSVariableDerived(self.series / other.series)


class ACSVariableData(ACSVariable):

    def __init__(self, variable_name, relative_position, acs_scope, geographic_restriction=None,
                 geo_field="geoid_tiger", numeric_field="numeric_value"):
        self.variable_name = variable_name
        self.relative_position = relative_position
        self.acs_scope = acs_scope
        self.geographic_restriction = geographic_restriction
        self.df = None
        self.acs_table_name_desc = None
        self.variable_desc = None
        self.geo_field = geo_field
        self.numeric_field = numeric_field

        self._get_acs_variable()

    def _table_name(self):
        return self.acs_scope.table_prefix + self.variable_name

    def _check_if_table_name_exists(self):

        tables = self.acs_scope.meta_data.tables

        if self._table_name() not in tables:
            raise(RuntimeError)

    def _get_table(self):
        return self.acs_scope.meta_data.tables[self._table_name()]

    def _get_acs_variable(self):
        self._check_if_table_name_exists()
        table = self._get_table()

        # Geographic restriction

        q = sa.sql.select([table]).where(table.c.relative_position == self.relative_position)
        self.df = pd.read_sql(q, self.acs_scope.connection)

        index_series = self.df[self.geo_field]
        numeric_series = self.df[self.numeric_field]

        self.series = pd.Series(numeric_series.as_matrix(), index=index_series.as_matrix())

        # print(self.df.head(10))

    def _merge_dfs(self, other_df):
        pass


class ACSVariableDerived(ACSVariable):

    def __init__(self, series):
        self.series = series


class ACSVariableFactory(object):

    def __init__(self, acs_scope, geographic_restriction=None):
        self.acs_scope = acs_scope
        self.geographic_restriction = geographic_restriction

    def new(self, variable_name, relative_position):
        return ACSVariableData(variable_name, relative_position, self.acs_scope, self.geographic_restriction)


class ACSExport(object):

    def __init__(self, pairs_with_export):
        pass