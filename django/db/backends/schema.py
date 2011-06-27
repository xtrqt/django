import sys
import time


from django.conf import settings
from django.core.management.color import no_style

# The prefix to put on the default database name when creating
# the test database.
TEST_DATABASE_PREFIX = 'test_'

class BaseDatabaseSchemaManagement(object):
    """
    This class encapsulates all backend-specific differences that pertain to
    database schema management *creation*, *alteration*, *dropping*, etc.,
    such as the column types to use for particular Django Fields, the 
    SQL used to create and destroy tables, and the creation and destruction 
    of test databases. This class superseeds BaseDatabaseCreation
    """
    
    def __init__(self, connection):
        self.connection = connection
    
    def create_table(self, table_name, fields):
        """
        Creates the table 'table_name'. 'fields' is a tuple of fields,
        each repsented by a 2-part tuple of field name and a
        django.db.models.fields.Field object
        """

        if len(table_name) > 63:
            print "   ! WARNING: You have a table name longer than 63 characters; this will not fully work on PostgreSQL or MySQL."

        columns = [
            self.column_sql(table_name, field_name, field)
            for field_name, field in fields
        ]

        self.execute('CREATE TABLE %s (%s);' % (
            self.quote_name(table_name),
            ', '.join([col for col in columns if col]),
        ))

    
    def rename_table(self, old_table_name, table_name):
        pass
        
    def delete_table(self, table_name, cascade=True):
        pass
    
    def clear_table(self, table_name):
        pass
    
    def add_column(self, table_name, name, field, keep_default=True):
        pass
        
    def alter_column(self, table_name, name, field, explicit_name=True, 
                     ignore_constraints=False):
        pass
    
    def create_unique(self, table_name, columns):
        pass
        
    def delete_unique(self, table_name, columns):
        pass
        
    def foreign_key_sql(self, from_table_name, from_column_name, 
                        to_table_name, to_column_name):
        # todo: why to generate sql
        pass
        
    def delete_foreign_key(self, table_name, column):
        pass
        
    def create_index(self, table_name, column_names, unique=False, 
                     db_tablespace=''):
        pass
        
    def delete_index(self, table_name, column_names, db_tablespace=''):
        pass
        
    def delete_column(self, table_name, name):
        pass
        
    def rename_column(self, table_name, old, new):
        pass
        
    def delete_primary_key(self, table_name):
        pass
        
    def create_primary_key(self, table_name, columns):  
        pass

    def execute(self, sql, params=[]):
        """
        Executes the given SQL statement, with optional parameters.
        If the instance's debug attribute is True, prints out what it executes.
        """
        
        #self._possibly_initialise()
        
        cursor = self.connection.cursor()
        #if self.debug:
        print "   = %s" % sql, params

        #get_logger().debug('south execute "%s" with params "%s"' % (sql, params))

        #if self.dry_run:
        #    return []

        cursor.execute(sql, params)
        try:
            return cursor.fetchall()
        except:
            return []

    def quote_name(self, name):
        """
        Uses the database backend to quote the given table/column name.
        """
        return self.connection.ops.quote_name(name)
    
    def column_sql(self, table_name, field_name, field, tablespace='', with_name=True, field_prepared=False):
        """
        Creates the SQL snippet for a column. Used by add_column and add_table.
        """

        # If the field hasn't already been told its attribute name, do so.
        if not field_prepared:
            field.set_attributes_from_name(field_name)

        # hook for the field to do any resolution prior to it's attributes being queried
        #if hasattr(field, 'south_init'):
        #    field.south_init()

        # Possible hook to fiddle with the fields (e.g. defaults & TEXT on MySQL)
        field = self._field_sanity(field)

        try:
            sql = field.db_type(connection=self.connection)
        except TypeError:
            sql = field.db_type()
        
        if sql:
            
            # Some callers, like the sqlite stuff, just want the extended type.
            if with_name:
                field_output = [self.quote_name(field.column), sql]
            else:
                field_output = [sql]
            
            field_output.append('%sNULL' % (not field.null and 'NOT ' or ''))
            if field.primary_key:
                field_output.append('PRIMARY KEY')
            elif field.unique:
                # Just use UNIQUE (no indexes any more, we have delete_unique)
                field_output.append('UNIQUE')

            tablespace = field.db_tablespace or tablespace
            if tablespace and self.connection.features.supports_tablespaces and field.unique:
                # We must specify the index tablespace inline, because we
                # won't be generating a CREATE INDEX statement for this field.
                field_output.append(self.connection.ops.tablespace_sql(tablespace, inline=True))
            
            sql = ' '.join(field_output)
            sqlparams = ()
            # if the field is "NOT NULL" and a default value is provided, create the column with it
            # this allows the addition of a NOT NULL field to a table with existing rows
            if not getattr(field, '_suppress_default', False):
                if field.has_default():
                    default = field.get_default()
                    # If the default is actually None, don't add a default term
                    if default is not None:
                        # If the default is a callable, then call it!
                        if callable(default):
                            default = default()
                        # Now do some very cheap quoting. TODO: Redesign return values to avoid this.
                        if isinstance(default, basestring):
                            default = "'%s'" % default.replace("'", "''")
                        elif isinstance(default, (datetime.date, datetime.time, datetime.datetime)):
                            default = "'%s'" % default
                        # Escape any % signs in the output (bug #317)
                        if isinstance(default, basestring):
                            default = default.replace("%", "%%")
                        # Add it in
                        sql += " DEFAULT %s"
                        sqlparams = (default)
                elif (not field.null and field.blank) or (field.get_default() == ''):
                    if field.empty_strings_allowed and self._get_connection().features.interprets_empty_strings_as_nulls:
                        sql += " DEFAULT ''"
                    # Error here would be nice, but doesn't seem to play fair.
                    #else:
                    #    raise ValueError("Attempting to add a non null column that isn't character based without an explicit default value.")

            if field.rel and self.supports_foreign_keys:
                self.add_deferred_sql(
                    self.foreign_key_sql(
                        table_name,
                        field.column,
                        field.rel.to._meta.db_table,
                        field.rel.to._meta.get_field(field.rel.field_name).column
                    )
                )

        # Things like the contrib.gis module fields have this in 1.1 and below
        if hasattr(field, 'post_create_sql'):
            for stmt in field.post_create_sql(no_style(), table_name):
                self.add_deferred_sql(stmt)
        
        # In 1.2 and above, you have to ask the DatabaseCreation stuff for it.
        # This also creates normal indexes in 1.1.
        if hasattr(self.connection.creation, "sql_indexes_for_field"):
            # Make a fake model to pass in, with only db_table
            model = self.mock_model("FakeModelForGISCreation", table_name)
            for stmt in self.connection.creation.sql_indexes_for_field(model, field, no_style()):
                self.add_deferred_sql(stmt)
        
        if sql:
            return sql % sqlparams
        else:
            return None
        
    def _field_sanity(self, field):
        """
        Placeholder for DBMS-specific field alterations (some combos aren't valid,
        e.g. DEFAULT and TEXT on MySQL)
        """
        return field

    def mock_model(self, model_name, db_table, db_tablespace='', 
                   pk_field_name='id', pk_field_type=None,
                   pk_field_args=[], pk_field_kwargs={}):
        """
        Generates a MockModel class that provides enough information
        to be used by a foreign key/many-to-many relationship.

        Migrations should prefer to use these rather than actual models
        as models could get deleted over time, but these can remain in
        migration files forever.

        Depreciated.
        """
        from django.db.models import AutoField
        
        if pk_field_type == None:
            pk_field_type = AutoField;
        
        class MockOptions(object):
            def __init__(self):
                self.db_table = db_table
                self.db_tablespace = db_tablespace or settings.DEFAULT_TABLESPACE
                self.object_name = model_name
                self.module_name = model_name.lower()

                if pk_field_type == AutoField:
                    pk_field_kwargs['primary_key'] = True

                self.pk = pk_field_type(*pk_field_args, **pk_field_kwargs)
                self.pk.set_attributes_from_name(pk_field_name)
                self.abstract = False

            def get_field_by_name(self, field_name):
                # we only care about the pk field
                return (self.pk, self.model, True, False)

            def get_field(self, name):
                # we only care about the pk field
                return self.pk

        class MockModel(object):
            _meta = None

        # We need to return an actual class object here, not an instance
        MockModel._meta = MockOptions()
        MockModel._meta.model = MockModel
        return MockModel
