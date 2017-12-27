from django.db.backends import BaseDatabaseOperations

class DatabaseOperations(BaseDatabaseOperations):
    compiler_module = "django_myob_odbc.compiler"

    def quote_name(self, name):
        """
        Returns a quoted version of the given table, index or column name. Does
        not quote the given name if it's already been quoted.
        """
        if name.startswith('"') and name.endswith('"'):
            return name # Quoting once is enough.
        return '"%s"' % name

    def sequence_reset_sql(self, *args, **kwargs):
        print 'ahhhhhh'
        return None
