from django.test import TestCase
from django.db import connection
from django.db.models.fields import AutoField


class SimpleTests(TestCase):

    def test_create_table(self):
        """
        Test if an empty table can be created.
        """
        connection.schema.create_table("TestTable", [("id", AutoField(primary_key=True))])
        cursor = connection.cursor()
        self.assert_("TestTable" in connection.introspection.get_table_list(cursor));
        