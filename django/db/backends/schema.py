import sys
import time


from django.conf import settings
from django.core.management.color import no_style
import operator
from django.db.backends.util import truncate_name
from django.db import transaction


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
    
    add_column_string = 'ALTER TABLE %(table_name)s ADD COLUMN %(column_definition)s;'
    create_table_string = 'CREATE TABLE %(table_name)s (%(columns)s);'
    rename_table_string = 'ALTER TABLE %(table_name)s RENAME TO %(new_table_name)s;'
    delete_table_string = 'DROP TABLE %(table_name)s%(cascade)s;'
    clear_table_string = 'DELETE FROM %(table_name)s;'
    
    alter_string_set_type = 'ALTER COLUMN %(column)s TYPE %(type)s'
    alter_string_set_null = 'ALTER COLUMN %(column)s DROP NOT NULL'
    alter_string_drop_null = 'ALTER COLUMN %(column)s SET NOT NULL'
    create_index_string = 'CREATE %(unique)sINDEX %(index_name)s ON %(table_name)s (%(columns)s)%(table_space)s;'
    delete_check_sql = 'ALTER TABLE %(table_name)s DROP CONSTRAINT %(constraint)s'
    delete_unique_sql = "ALTER TABLE %(table_name)s DROP CONSTRAINT %(constraint)s"
    delete_foreign_key_sql = 'ALTER TABLE %(table_name)s DROP CONSTRAINT %(constraint)s'
    create_unique_string = "ALTER TABLE %(table_name)s ADD CONSTRAINT %(name)s UNIQUE (%(columns)s)"
    drop_index_string = 'DROP INDEX %(index_name)s'
    delete_column_string = 'ALTER TABLE %(table_name)s DROP COLUMN %(column_name)s CASCADE;'
    create_primary_key_string = "ALTER TABLE %(table)s ADD CONSTRAINT %(constraint)s PRIMARY KEY (%(columns)s)"
    delete_primary_key_sql = "ALTER TABLE %(table)s DROP CONSTRAINT %(constraint)s"
    foreign_key_string = 'ALTER TABLE %(table_name)s ADD CONSTRAINT %(constraint)s FOREIGN KEY (%(column)s) REFERENCES %(to_table)s (%to_column)s)%(deferrable_sql)s;'
    
    has_check_constraints = True
    default_schema_name = "public"
    max_index_name_length = 63
    
    
    def __init__(self, connection):
        # stores reference to connection object passed during creation
        self.connection = connection
        
        # states if any function is really going to affect database
        self.dry_run = False
        
        # place to buffer all SQL code that is not executed due to `dry_run`.
        self.dry_run_buffer = []
        
        self.debug = True
        
    def start_dry_run(self):
        """
        Starts dry run mode, in which no SQL is executed.
        SQL is only buffered and returned on finish_dry_run()
        """
        
        self.dry_run_buffer = []
        self.dry_run = True
        
    def finish_dry_run(self):
        """
        Finish dry run mode, in which no SQL is executed.
        SQL is only buffered and returned from this function
        """
        
        self.dry_run = False
        return self.dry_run_buffer
    
    def create_table(self, table_name, fields, explicit_name=False):
        """
        Creates the table 'table_name'. 'fields' is a tuple of fields,
        each repsented by a 2-part tuple of field name and a
        django.db.models.fields.Field object
        """

        if len(table_name) > 63:
            print "   ! WARNING: You have a table name longer than 63 characters; this will not fully work on PostgreSQL or MySQL."

        # generate sql for all columns, based on respective Fields 
        # definitions
        columns = [
            self.column_sql(table_name, field_name, field, explicit_name)
            for field_name, field in fields
        ]

        self.execute(self.create_table_string % {
            'table_name': self.quote_name(table_name),
            'columns': ', '.join([col for col in columns if col]),
        })

    
    def rename_table(self, table_name, new_table_name):
        """
        Renames the table 'table_name' to 'new_table_name'.
        """
        
        # if old and new name are equal do nothing
        if new_table_name == table_name:
            return
        
        params = {'table_name': self.quote_name(table_name),
                  'new_table_name': self.quote_name(new_table_name)}
        
        self.execute(self.rename_table_string % params)
        
    def delete_table(self, table_name, cascade=True):
        """
        Deletes the table 'table_name'.
        """
        params = {'table_name': self.quote_name(table_name),
                  'cascade': cascade and ' CASCADE' or '' }
        
        self.execute(self.delete_table_string % params)
        
    
    def clear_table(self, table_name):
        """
        Deletes all rows from 'table_name'.
        """
        params = {'table_name': self.quote_name(table_name)}
        
        self.execute(self.clear_table_string % params)
    
    def add_column(self, table_name, column_name, field, explicit_name=True, keep_default=True):
        """
        Adds the column 'column_name' to the table 'table_name'.
        Uses the 'field' parameter, a django.db.models.fields.Field instance,
        to generate the necessary sql

        @param table_name: The name of the table to add the column to
        @param name: The name of the column to add
        @param field: The field definition to use
        """
        
        # create code for column
        
        # we should check if the field can be null, or have set default
        
        ## it needs to have default value, because existing rows
        ## don't have needed value.
        sql = self.column_sql(table_name, column_name, field, explicit_name)
        if not sql:
            return None
        
        params = {'table_name': self.quote_name(table_name),
                  'column_definition': sql }
        self.execute(self.add_column_string % params)
        
        from django.db.models.fields import NOT_PROVIDED
        # Now, drop the default if we need to
        if not keep_default and field.default is not None:
            field.default = NOT_PROVIDED
            self.alter_column(table_name, column_name, field, explicit_name=False, ignore_constraints=True)
        
    def alter_column(self, table_name, column_name, field, explicit_name=True, 
                     ignore_constraints=False):
        """
        Alters the given column name so it will match the given field.
        Note that conversion between the two by the database must be possible.
        Will not automatically add _id by default; to have this behavior, pass
        explicit_name=False.

        @param table_name: The name of the table to add the column to
        @param name: The name of the column to alter
        @param field: The new field definition to use
        """
        
        
        # Add _id or whatever if we need to
        field.set_attributes_from_name(column_name)
        if not explicit_name:
            column_name = field.column
        else:
            field.column = column_name

        if not ignore_constraints:
            # Drop all check constraints. TODO: Add the right ones back.
            if self.has_check_constraints:
                check_constraints = self._constraints_affecting_columns(table_name, [column_name], "CHECK")
                for constraint in check_constraints:
                    self.execute(self.delete_check_sql % {
                        'table_name': self.quote_name(table_name),
                        'constraint': self.quote_name(constraint),
                    })
        
            # Drop all foreign key constraints
            try:
                self.delete_foreign_key(table_name, column_name)
            except ValueError:
                # There weren't any
                pass

        # First, change the type
        params = {
            "column": self.quote_name(name),
            "type": self._db_type_for_alter_column(field),            
            "table_name": table_name
        }

        # SQLs is a list of (SQL, values) pairs.
        sqls = []
        
        # Only alter the column if it has a type (Geometry ones sometimes don't)
        if params["type"] is not None:
            sqls.append((self.alter_string_set_type % params, []))
        
        # Next, nullity
        if field.null:
            sqls.append((self.alter_string_set_null % params, []))
        else:
            sqls.append((self.alter_string_drop_null % params, []))

        # Next, set any default
        self._alter_set_defaults(field, column_name, params, sqls)

        # Finally, actually change the column
        if self.allows_combined_alters:
            sqls, values = zip(*sqls)
            self.execute(
                "ALTER TABLE %s %s;" % (self.quote_name(table_name), ", ".join(sqls)),
                reduce(operator.add, values), #flatten(values),
            )
        else:
            # Databases like e.g. MySQL don't like more than one alter at once.
            for sql, values in sqls:
                self.execute("ALTER TABLE %s %s;" % (self.quote_name(table_name), sql), values)
        
        if not ignore_constraints:
            # Add back FK constraints if needed
            if field.rel and self.supports_foreign_keys:
                self.execute(
                    self.foreign_key_sql(
                        table_name,
                        field.column,
                        field.rel.to._meta.db_table,
                        field.rel.to._meta.get_field(field.rel.field_name).column
                    )
                )
                
                
    def _constraints_affecting_columns(self, table_name, columns, type="UNIQUE"):
        """
        Gets the names of the constraints affecting the given columns.
        If columns is None, returns all constraints of the type on the table.
        """

        if self.dry_run:
            raise ValueError("Cannot get constraints for columns during a dry run.")

        if columns is not None:
            columns = set(columns)

        if type == "CHECK":
            ifsc_table = "constraint_column_usage"
        else:
            ifsc_table = "key_column_usage"

        schema = self._get_schema_name()            

        # First, load all constraint->col mappings for this table.
        rows = self.execute("""
            SELECT kc.constraint_name, kc.column_name
            FROM information_schema.%s AS kc
            JOIN information_schema.table_constraints AS c ON
                kc.table_schema = c.table_schema AND
                kc.table_name = c.table_name AND
                kc.constraint_name = c.constraint_name
            WHERE
                kc.table_schema = %%s AND
                kc.table_name = %%s AND
                c.constraint_type = %%s
        """ % ifsc_table, [schema, table_name, type])
        
        # Load into a dict
        mapping = {}
        for constraint, column in rows:
            mapping.setdefault(constraint, set())
            mapping[constraint].add(column)
        
        # Find ones affecting these columns
        for constraint, itscols in mapping.items():
            # If columns is None we definitely want this field! (see docstring)
            if itscols == columns or columns is None:
                yield constraint

    
    def create_unique(self, table_name, columns):
        """
        Creates a UNIQUE constraint on the columns on the given table.
        """

        if not isinstance(columns, (list, tuple)):
            columns = [columns]

        name = self.create_index_name(table_name, columns, suffix="_uniq")

        cols = ", ".join(map(self.quote_name, columns))
        self.execute(self.create_unique_string % {
            'table_name': self.quote_name(table_name), 
            'name': self.quote_name(name), 
            'columns': cols,
        })
        return name
        
    def delete_unique(self, table_name, columns):
        """
        Deletes a UNIQUE constraint on precisely the columns on the given table.
        """

        if not isinstance(columns, (list, tuple)):
            columns = [columns]

        constraints = list(self._constraints_affecting_columns(table_name, columns))
        if not constraints:
            raise ValueError("Cannot find a UNIQUE constraint on table %s, columns %r" % (table_name, columns))
        for constraint in constraints:
            self.execute(self.delete_unique_sql % {
                'table_name': self.quote_name(table_name), 
                'constraint': self.quote_name(constraint),
            })

        
    def foreign_key_sql(self, from_table_name, from_column_name, to_table_name, to_column_name):
        """
        Generates a full SQL statement to add a foreign key constraint
        """
        constraint_name = '%s_refs_%s_%x' % (from_column_name, to_column_name, abs(hash((from_table_name, to_table_name))))
        
        return self.foreign_key_string % {
            'table_name': self.quote_name(from_table_name),
            'constraint': self.quote_name(truncate_name(constraint_name, self.connection.ops.max_name_length())),
            'column': self.quote_name(from_column_name),
            'to_table': self.quote_name(to_table_name),
            'to_column': self.quote_name(to_column_name),
            'deferrable_sql': self._get_connection().ops.deferrable_sql() # Django knows this
        }
    
        
    def delete_foreign_key(self, table_name, column_name):
        "Drop a foreign key constraint"
        if self.dry_run:
            return # We can't look at the DB to get the constraints
        constraints = list(self._constraints_affecting_columns(table_name, [column_name], "FOREIGN KEY"))
        if not constraints:
            raise ValueError("Cannot find a FOREIGN KEY constraint on table %s, column %s" % (table_name, column_name))
        for constraint_name in constraints:
            self.execute(self.delete_foreign_key_sql % {
                "table_name": self.quote_name(table_name),
                "constraint": self.quote_name(constraint_name),
            })
        
    def create_index_name(self, table_name, column_names, suffix=""):
        """
        Generate a unique name for the index
        """

        table_name = table_name.replace('"', '').replace('.', '_')
        index_unique_name = ''

        if len(column_names) > 1:
            index_unique_name = '_%x' % abs(hash((table_name, ','.join(column_names))))

        # If the index name is too long, truncate it
        index_name = ('%s_%s%s%s' % (table_name, column_names[0], index_unique_name, suffix)).replace('"', '').replace('.', '_')
        if len(index_name) > self.max_index_name_length:
            part = ('_%s%s%s' % (column_names[0], index_unique_name, suffix))
            index_name = '%s%s' % (table_name[:(self.max_index_name_length-len(part))], part)

        return index_name


    def create_index_sql(self, table_name, column_names, unique=False, db_tablespace=''):
        """
        Generates a create index statement on 'table_name' for a list of 'column_names'
        """
        if not column_names:
            print "No column names supplied on which to create an index"
            return ''

        connection = self._get_connection()
        if db_tablespace and connection.features.supports_tablespaces:
            tablespace_sql = ' ' + connection.ops.tablespace_sql(db_tablespace)
        else:
            tablespace_sql = ''

        index_name = self.create_index_name(table_name, column_names)
        return self.create_index_string % {
            'unique': unique and 'UNIQUE ' or '',
            'index_name': self.quote_name(index_name),
            'table_name': self.quote_name(table_name),
            'columns': ','.join([self.quote_name(field) for field in column_names]),
            'table_space': tablespace_sql
        }

    def create_index(self, table_name, column_names, unique=False, 
                     db_tablespace=''):
        """ Executes a create index statement """
        sql = self.create_index_sql(table_name, column_names, unique, db_tablespace)
        self.execute(sql)
        
    def delete_index(self, table_name, column_names, db_tablespace=''):
        """
        Deletes an index created with create_index.
        This is possible using only columns due to the deterministic
        index naming function which relies on column names.
        """
        if isinstance(column_names, (str, unicode)):
            column_names = [column_names]
        name = self.create_index_name(table_name, column_names)
        sql = self.drop_index_string % {
            "index_name": self.quote_name(name),
            "table_name": self.quote_name(table_name),
        }
        self.execute(sql)

    def delete_column(self, table_name, column_name):
        """
        Deletes the column 'column_name' from the table 'table_name'.
        """
        params = { 'table_name': self.quote_name(table_name), 
                   'column_name': self.quote_name(column_name) }
        self.execute(self.delete_column_string % params, [])

    def rename_column(self, table_name, column_name, new_column_name):
        """
        Renames the column 'old' from the table 'table_name' to 'new'.
        """
        raise NotImplementedError("rename_column has no generic SQL syntax")

    
    def delete_primary_key(self, table_name):
        """
        Drops the old primary key.
        """
        # Dry runs mean we can't do anything.
        if self.dry_run:
            return
        
        constraints = list(self._constraints_affecting_columns(table_name, None, type="PRIMARY KEY"))
        if not constraints:
            raise ValueError("Cannot find a PRIMARY KEY constraint on table %s" % (table_name,))
        
        for constraint in constraints:
            self.execute(self.delete_primary_key_sql % {
                "table": self.quote_name(table_name),
                "constraint": self.quote_name(constraint),
            })
    
        
    def create_primary_key(self, table_name, columns):
        """
        Creates a new primary key on the specified columns.
        """
        if not isinstance(columns, (list, tuple)):
            columns = [columns]
        self.execute(self.create_primary_key_string % {
            "table": self.quote_name(table_name),
            "constraint": self.quote_name(table_name+"_pkey"),
            "columns": ", ".join(map(self.quote_name, columns)),
        })


    def start_transaction(self):
        """
        Makes sure the following commands are inside a transaction.
        Must be followed by a (commit|rollback)_transaction call.
        """
        if self.dry_run:
            self.pending_transactions += 1
            return
            
        transaction.commit_unless_managed()
        transaction.enter_transaction_management()
        transaction.managed(True)


    def commit_transaction(self):
        """
        Commits the current transaction.
        Must be preceded by a start_transaction call.
        """
        if self.dry_run:
            return
        transaction.commit()
        transaction.leave_transaction_management()


    def rollback_transaction(self):
        """
        Rolls back the current transaction.
        Must be preceded by a start_transaction call.
        """
        if self.dry_run:
            self.pending_transactions -= 1
            return
        transaction.rollback()
        transaction.leave_transaction_management()

    def rollback_transactions_dry_run(self):
        """
        Rolls back all pending_transactions during this dry run.
        """
        if not self.dry_run:
            return
        while self.pending_transactions > 0:
            self.rollback_transaction()
        if transaction.is_dirty():
            # Force an exception, if we're still in a dirty transaction.
            # This means we are missing a COMMIT/ROLLBACK.
            transaction.leave_transaction_management()


    def execute(self, sql, params=[]):
        """
        Executes the given SQL statement, with optional parameters.
        If the instance's debug attribute is True, prints out what it executes.
        """
        
        
        cursor = self.connection.cursor()
        if self.debug:
            print "   = %s" % sql, params

        if self.dry_run:
            if len(params) > 0:
                self.dry_run_buffer.append( (sql, params) )
            else:
                self.dry_run_buffer.append( sql )
                
            return []
        
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
    
    def column_sql(self, table_name, field_name, field, explicit_name=False, tablespace='', with_name=True, field_prepared=False):
        """
        Creates the SQL snippet for a column. Used by add_column and add_table.
        """

        # If the field hasn't already been told its attribute name, do so.
        if not field_prepared:
            field.set_attributes_from_name(field_name)

        # Possible hook to fiddle with the fields (e.g. defaults & TEXT on MySQL)
        field = self._field_sanity(field)

        try:
            sql = field.db_type(connection=self.connection)
        except TypeError:
            sql = field.db_type()
        
        if sql:
            
            # Some callers, like the sqlite stuff, just want the extended type.
            if with_name:
                if explicit_name:
                    field_output = [self.quote_name(field_name), sql]
                else:
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
                    if field.empty_strings_allowed and self.connection.features.interprets_empty_strings_as_nulls:
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

    def _get_setting(self, setting_name):
        """
        Allows code to get a setting (like, for example, STORAGE_ENGINE)
        """
        return self.connection.settings_dict[setting_name.upper()] 
        
    def _has_setting(self, setting_name):
        """
        Existence-checking version of _get_setting.
        """
        try:
            self._get_setting(setting_name)
        except (KeyError, AttributeError):
            return False
        else:
            return True

    def _get_schema_name(self):
        try:
            return self._get_setting('schema')
        except (KeyError, AttributeError):
            return self.default_schema_name
        
    