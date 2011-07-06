from django.db.backends.schema import BaseDatabaseSchemaManagement


class DatabaseSchemaManagement(BaseDatabaseSchemaManagement):
    def _create_unique(self, table_name, columns):
        self.execute("CREATE UNIQUE INDEX %s ON %s(%s);" % (
            self.quote_name('%s_%s' % (table_name, '__'.join(columns))),
            self.quote_name(table_name),
            ', '.join(self.quote_name(c) for c in columns),
        ))
        
    def create_unique(self, table_name, columns):
        """
        Create an unique index on columns
        """
        if not isinstance(columns, (list, tuple)):
            columns = [columns]
        
        self._create_unique(table_name, columns)
    
