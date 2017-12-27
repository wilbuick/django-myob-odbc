from django.db.models.sql import compiler
from django.db.utils import DatabaseError

class SQLCompiler(compiler.SQLCompiler):
    pass

class SQLInsertCompiler(compiler.SQLInsertCompiler):
    pass

class SQLDeleteCompiler(compiler.SQLDeleteCompiler):
    """
    Prevent sql delete queries being run on the database.
    """
    def as_sql(self):
        raise DatabaseError('Delete operations not permitted')

class SQLUpdateCompiler(compiler.SQLUpdateCompiler):
    pass

class SQLAggregateCompiler(compiler.SQLAggregateCompiler):
    pass

class SQLDateCompiler(compiler.SQLDateCompiler):
    pass
