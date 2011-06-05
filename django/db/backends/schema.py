import sys
import time

from django.conf import settings

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
        pass
    
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

    