import pandas as pd
import sqlalchemy as sa


class ACSScope(object):

    def __init__(self,  connection, estimate_type="e", estimate_year="2016", estimate_time="5", geo_scope="us",
                 schema=None):

        self.connection = connection
        self.meta_data = sa.MetaData(connection, schema=schema)
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
        self.restriction_table = None

    def create(self, connection, meta_data, refresh=False):

        schema = meta_data.schema
        if schema is None:
            full_table_name = self.name
        else:
            full_table_name = schema + "." + self.name

        try:
            connection.execute("drop table if exists public." + self.name)
        except():
            pass

        if refresh or full_table_name not in meta_data.tables:

            if full_table_name in meta_data.tables:
                restriction_table = meta_data.tables[full_table_name]
                restriction_table.drop()

            meta_data = sa.MetaData(connection, meta_data.schema)  # We have to refresh the metadata
            meta_data.reflect()

            restriction_table = sa.Table(self.name, meta_data, sa.Column("code", sa.VARCHAR(50)))

            meta_data.create_all()

            for code in self.codes:
                connection.execute(restriction_table.insert({"code": code}))

        else:
            restriction_table = meta_data.tables[self.name]

        self.restriction_table = restriction_table

        return restriction_table


class ACSVariable(object):
    """Parent class for defining operations"""

    def __truediv__(self, other):
        return ACSVariableDerived(self.series / other.series)

    def __rtruediv__(self, other):
        return ACSVariableDerived(self.series / other)

    def __add__(self, other):
        return ACSVariableDerived(self.series + other.series)

    def __radd__(self, other):
        return ACSVariableDerived(self.series / other)

    def __sub__(self, other):
        return ACSVariableDerived(self.series - other.series)

    def __rsub__(self, other):
        return ACSVariableDerived(self.series - other)

    def __mul__(self, other):
        return ACSVariableDerived(self.series * other.series)

    def __rmul__(self, other):
        return ACSVariableDerived(self.series * other)


class ACSConstant(ACSVariable):

    """A constant for example if you want to ACSConstant(1) - fraction_white"""

    def __init__(self, constant):
        self.constant = constant
        self.series = constant


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

    def _full_table_name(self):

        if self.acs_scope.meta_data.schema is not None:
            return self.acs_scope.meta_data.schema + "." + self._table_name()
        else:
            return self._table_name()

    def _check_if_table_name_exists(self):

        tables = self.acs_scope.meta_data.tables

        if self._full_table_name() not in tables:
            raise(RuntimeError)

    def _get_table(self):
        return self.acs_scope.meta_data.tables[self._full_table_name()]

    def _get_acs_variable(self):
        self._check_if_table_name_exists()
        table = self._get_table()

        # Geographic restriction

        if self.geographic_restriction is None:
            q = sa.sql.select([table]).where(table.c.relative_position == self.relative_position)
        else:

            restriction_table = self.geographic_restriction.restriction_table
            q = sa.sql.select([table.join(restriction_table, table.c[self.geo_field] == restriction_table.c["code"])]).where(table.c.relative_position == self.relative_position)

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

    """Makes it easy to create ACS derivived   """

    def __init__(self, acs_scope, geographic_restriction=None, geo_restriction_refresh=False, geo_field="geo_name"):
        self.acs_scope = acs_scope
        self.geographic_restriction = geographic_restriction
        self.geo_field = geo_field

        if self.geographic_restriction is not None:
            self.geographic_restriction.create(acs_scope.connection, acs_scope.meta_data, refresh=geo_restriction_refresh)

    def new(self, variable_name, relative_position):
        return ACSVariableData(variable_name, relative_position, self.acs_scope, self.geographic_restriction,
                               geo_field=self.geo_field)


class ACSExport(object):

    def __init__(self, pairs_with_df):
        self.pairs_with_df = pairs_with_df
        self._create_data_frame()

    def _create_data_frame(self):
        dfs = [d[1].series for d in self.pairs_with_df]
        dfs_names = [d[0] for d in self.pairs_with_df]

        self.df = pd.concat(dfs, axis=1)
        self.df.reset_index(inplace=True)

        self.df.columns = ["geo_field"] + dfs_names

    def write_to_csv(self, file_name):

        self.df.to_csv(file_name, index=False)
