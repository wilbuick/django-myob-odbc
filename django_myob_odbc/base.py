"""
MYOB ODBC Server database backend for Django.
"""

import os
import sys

try:
    import pyodbc as Database
except ImportError, e:
    from django.core.exceptions import ImproperlyConfigured
    raise ImproperlyConfigured("Error loading pyodbc module: %s" % e)

from django.db import utils
from django.db.backends import *
from django.db.backends.signals import connection_created
from django.conf import settings

from operations import DatabaseOperations
from client import DatabaseClient
from creation import DatabaseCreation
from introspection import DatabaseIntrospection

DatabaseError = Database.DatabaseError
IntegrityError = Database.IntegrityError


class CursorWrapper(object):
    """
    A wrapper around the pyodbc's cursor that takes in account a) some pyodbc
    DB-API 2.0 implementation and b) some common ODBC driver particularities.
    """
    def __init__(self, cursor):
        self.cursor = cursor
        self.last_sql = ''
        self.last_params = ()

    def format_sql(self, sql, n_params=None):
        # pyodbc uses '?' instead of '%s' as parameter placeholder.
        if n_params is not None:
            sql = sql % tuple('?' * n_params)
        else:
            if '%s' in sql:
                sql = sql.replace('%s', '?')
        return sql

    def format_params(self, params):
        fp = []
        for p in params:
            if isinstance(p, unicode):
                fp.append(p)
            elif isinstance(p, str):
                fp.append(p)
            elif isinstance(p, type(True)):
                if p:
                    fp.append(1)
                else:
                    fp.append(0)
            else:
                fp.append(p)
        return tuple(fp)

    def execute(self, sql, params=()):
        self.last_sql = sql
        sql = self.format_sql(sql, len(params))
        params = self.format_params(params)
        self.last_params = params
        try:
            return self.cursor.execute(sql, params)
        except Database.IntegrityError, e:
            raise utils.IntegrityError, utils.IntegrityError(*tuple(e)), sys.exc_info()[2]
        except Database.DatabaseError, e:
            raise utils.DatabaseError, utils.DatabaseError(*tuple(e)), sys.exc_info()[2]
            
    def executemany(self, sql, params_list):
        sql = self.format_sql(sql)
        # pyodbc's cursor.executemany() doesn't support an empty param_list
        if not params_list:
            if '?' in sql:
                return
        else:
            raw_pll = params_list
            params_list = [self.format_params(p) for p in raw_pll]
        try:
            return self.cursor.executemany(sql, params_list)
        except Database.IntegrityError, e:
            raise utils.IntegrityError, utils.IntegrityError(*tuple(e)), sys.exc_info()[2]
        except Database.DatabaseError, e:
            raise utils.DatabaseError, utils.DatabaseError(*tuple(e)), sys.exc_info()[2]

    def fetchone(self):
        row = self.cursor.fetchone()
        if row is not None:
            return tuple(row)
        return []

    def fetchmany(self, chunk):
        return [tuple(row) for row in self.cursor.fetchmany(chunk)]

    def fetchall(self):
        return [tuple(row) for row in self.cursor.fetchall()]

    @property
    def lastrowid(self):
        """Required to stop errors propagating."""
        return None
    
    def __getattr__(self, attr):
        if attr in self.__dict__:
            return self.__dict__[attr]
        return getattr(self.cursor, attr)
    
    def __iter__(self):
        return iter(self.cursor)



class DatabaseFeatures(BaseDatabaseFeatures):
    uses_custom_query_class = False
    can_use_chunked_reads = False
    can_return_id_from_insert = False
    supports_unspecified_pk = True


class DatabaseWrapper(BaseDatabaseWrapper):
    vendor = 'myob'
    Database = Database

    operators = {
        'exact': '= %s',
        'iexact': '= UPPER(%s)',
        'contains': 'LIKE %s',
        'icontains': 'LIKE UPPER(%s)',
        'regex': '~ %s',
        'iregex': '~* %s',
        'gt': '> %s',
        'gte': '>= %s',
        'lt': '< %s',
        'lte': '<= %s',
        'startswith': 'LIKE %s',
        'endswith': 'LIKE %s',
        'istartswith': 'LIKE UPPER(%s)',
        'iendswith': 'LIKE UPPER(%s)',
    }

    def __init__(self, *args, **kwargs):
        super(DatabaseWrapper, self).__init__(*args, **kwargs)
        self.features = DatabaseFeatures(self)
        self.ops = DatabaseOperations(self)
        self.client = DatabaseClient(self)
        self.creation = DatabaseCreation(self)
        self.introspection = DatabaseIntrospection(self)
        self.validation = BaseDatabaseValidation(self)
        self.connection = None
    
    def create_cursor(self):
        return CursorWrapper(self.connection.cursor())

    def get_connection_params(self):
        settings_dict = self.settings_dict
        db_str, user_str, passwd_str, port_str = None, None, "", None
        options = settings_dict['OPTIONS']
        if settings_dict['NAME']:
            db_str = settings_dict['NAME']
        if settings_dict['HOST']:
            host_str = settings_dict['HOST']
        else:
            host_str = 'localhost'
        if settings_dict['USER']:
            user_str = settings_dict['USER']
        if settings_dict['PASSWORD']:
            passwd_str = settings_dict['PASSWORD']
        if settings_dict['PORT']:
            port_str = settings_dict['PORT']

        if not db_str:
            from django.core.exceptions import ImproperlyConfigured
            raise ImproperlyConfigured('You need to specify NAME in your Django settings file.')

        if 'driver' not in options:
            raise ImproperlyConfigured('You need to specify a driver')

        cstr_parts = []
        cstr_parts.append('DRIVER=%s' % options['driver'])
        cstr_parts.append('DSN=%s' % options['dsn'])
        cstr_parts.append('DATABASE=%s' % db_str)
        if 'extra_params' in options:
            cstr_parts.append(options['extra_params'])

        connstr = ';'.join(cstr_parts)
        return connstr

    def init_connection_state(self):
        pass
        
    def get_new_connection(self, connstr):
        autocommit = self.settings_dict['OPTIONS'].get('autocommit', False)
        return Database.connect(connstr, autocommit=autocommit)

    def _set_autocommit(self, autocommit):
        pass
        
    def _commit(self):
        if self.connection is not None:
            try:
                return self.connection.commit()
            except Database.IntegrityError, e:
                raise utils.IntegrityError, utils.IntegrityError(*tuple(e)), sys.exc_info()[2]
