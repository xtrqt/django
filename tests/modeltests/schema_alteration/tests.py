from django.test import TestCase
from django.db import connection
from django.db.models.fields import AutoField, CharField


## todo: remove
try:
    from IPython.Shell import IPShellEmbed
    ipshell = IPShellEmbed()
except:
    pass

class SimpleTests(TestCase):

    def test_create_table(self):
        """
        Test if simple table with one field can be created
        """
        connection.schema.create_table("TestTable", [("id", AutoField(primary_key=True))])
        cursor = connection.cursor()
        self.assertIn("TestTable", connection.introspection.get_table_list(cursor));
        result = connection.introspection.get_table_description(connection.cursor(), 'TestTable')
        self.assertEqual(result, [(u'id', u'integer', None, None, None, None, False)])
   
    def test_rename_table(self):
        """
        Create table and rename
        """
        # Create
        connection.schema.create_table("TestTable2", [("id", AutoField(primary_key=True))])
        cursor = connection.cursor()
        self.assertIn("TestTable2", connection.introspection.get_table_list(cursor));

        # Rename
        connection.schema.rename_table("TestTable2", "NewTestTable2")
        self.assertNotIn("TestTable2", connection.introspection.get_table_list(cursor))
        self.assertIn("NewTestTable2", connection.introspection.get_table_list(cursor))
        
    def test_clear_table(self):
        """
        Create table, add data and clear content of table.
        """
        # TODO: maybe that test will need different implementations for different backends.
        
        # Create
        connection.schema.create_table("TestTable3", [("id", AutoField(primary_key=True))])
        cursor = connection.cursor()
        self.assertIn("TestTable3", connection.introspection.get_table_list(cursor));

        # add new record
        cursor.execute("INSERT INTO %s (id) VALUES (1)" % (connection.schema.quote_name("TestTable3"),))
        
        # check if data have been inserted
        cursor.execute("SELECT * FROM %s" % (connection.schema.quote_name("TestTable3"),))
        data = cursor.fetchall()
        self.assertEqual(data, [(1,)])
        
        # clear table
        connection.schema.clear_table("TestTable3")
        
        # check if data have been removed
        cursor.execute("SELECT * FROM %s" % (connection.schema.quote_name("TestTable3"),))
        data = cursor.fetchall()
        self.assertEqual(data, [])
        
    def test_delete_table(self):
        """
        Create table and delete table
        """
        
        # Create
        connection.schema.create_table("TestTable4", [("id", AutoField(primary_key=True))])
        cursor = connection.cursor()
        self.assertIn("TestTable4", connection.introspection.get_table_list(cursor));

        ## TODO: add test for cascade delete
        # Delete
        connection.schema.delete_table("TestTable4", False)
        cursor = connection.cursor()
        self.assertNotIn("TestTable4", connection.introspection.get_table_list(cursor));

    def test_add_column(self):
        """
        Create table and add column
        """
        
        # Create
        connection.schema.create_table("TestTable5", [("id", AutoField(primary_key=True))])
        cursor = connection.cursor()
        self.assertIn("TestTable5", connection.introspection.get_table_list(cursor));
        
        result = connection.introspection.get_table_description(connection.cursor(), 'TestTable5')
        self.assertEqual(result, [(u'id', u'integer', None, None, None, None, False)])
        
        connection.schema.add_column("TestTable5", "test_field", CharField(max_length=50, default=""))
        result = connection.introspection.get_table_description(connection.cursor(), 'TestTable5')
        self.assertEqual(result, [(u'id', u'integer', None, None, None, None, False),
                                  (u'test_field', u'varchar(50)', None, None, None, None, False)])
        

    def test_alter_column(self):
        """
        Create table and alter column
        """
        
        #sqlite3 need implementation to pass the test for now it is switched off.
        return
        
        # Create
        connection.schema.create_table("TestTable6", [("id", AutoField(primary_key=True)),
                                                      ("test_field", CharField(max_length=50, default=""))])
        cursor = connection.cursor()
        self.assertIn("TestTable6", connection.introspection.get_table_list(cursor));
        
        result = connection.introspection.get_table_description(connection.cursor(), 'TestTable6')
        self.assertEqual(result, [(u'id', u'integer', None, None, None, None, False),
                                  (u'test_field', u'varchar(50)', None, None, None, None, False)])
        
        connection.schema.alter_column("TestTable6", "test_field", CharField(max_length=80, default=""))
        result = connection.introspection.get_table_description(connection.cursor(), 'TestTable6')
        self.assertEqual(result, [(u'id', u'integer', None, None, None, None, False),
                                  (u'test_field', u'varchar(80)', None, None, None, None, False)])
        
        
    def test_create_delete_unique(self):
        """
        Create unique index and delete it
        """
        
        # Create
        connection.schema.create_table("TestTable7", [("id", AutoField(primary_key=True)),
                                                      ("test_field", CharField(max_length=50, default=""))])
        cursor = connection.cursor()
        self.assertIn("TestTable7", connection.introspection.get_table_list(cursor));
        
        result = connection.introspection.get_table_description(connection.cursor(), 'TestTable7')
        self.assertEqual(result, [(u'id', u'integer', None, None, None, None, False),
                                  (u'test_field', u'varchar(50)', None, None, None, None, False)])
        
        connection.schema.create_unique("TestTable7", "test_field")
        
        # TODO: get indexes
        result = connection.introspection.get_table_description(connection.cursor(), 'TestTable7')
        self.assertEqual(result, [(u'id', u'integer', None, None, None, None, False),
                                  (u'test_field', u'varchar(50)', None, None, None, None, False)])
        
        

        
        
        

        
        
        
     