from django.db.backends.creation import BaseDatabaseCreation
from django.db.utils import DatabaseError

class DatabaseCreation(BaseDatabaseCreation):
    """
    Prevent this driver from creating databases.
    """
    def sql_create_model(self):
        raise DatabaseError('Cannot create database for MYOB')
