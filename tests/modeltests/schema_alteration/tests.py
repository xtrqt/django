from django.test import TestCase
from django.db import connection


class SimpleTests(TestCase):

    def test_create_table(self):
        """
        Test if an empty table can be created.
        """
        connection.schema.create_table("TestTable", [])
        cursor = connection.cursor()
        self.assertEqual(connection.introspection.get_table_list(cursor), ["TestTable"]);
        