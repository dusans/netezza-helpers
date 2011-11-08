import pyodbc
from mako.template import Template
import yaml
import os
import shutil
import re

def to_include(table, include, exclude, others_remove):
    if table in include:
        return True

    for e in exclude:
        if table.startswith(e):
            return False

    if others_remove == True:
        return False

    return True

def validate_column_name(c):
    invalid_characters = '#'
    reserved_keywords = ['PRIMARY', 'POSITION'] 

    for char in invalid_characters:
        c = c.replace(char, '_')

    if c in reserved_keywords:
        c = c + '_'

    return c

def get_columns(cursor, schema, table):
    c = cursor.execute('''
        SELECT
            OWNER                                AS TABLE_SCHEM,
            TABLE_NAME                           AS TABLE_NAME,
            COLUMN_NAME                          AS COLUMN_NAME,
            DATA_TYPE                            AS DATA_TYPE,
            NVL(DATA_PRECISION, DATA_LENGTH)     AS COLUMN_SIZE,
            DATA_SCALE                           AS DECIMAL_DIGITS,
            DECODE(NULLABLE, 'N', 'NO', 'YES')   AS IS_NULLABLE
        FROM	ALL_TAB_COLUMNS
        WHERE	OWNER = '%s'
        AND         TABLE_NAME = '%s'
        ORDER	BY	COLUMN_ID
        ;''' % (schema, table))

    return c.fetchall()


class Table:
    def __init__(self, table_name, table_schem='', columns=[], pks=[], foreign_keys=[], settings=[]):
        self.table_name = table_name
        self.table_schem = table_schem
        self.columns = columns
        self.pks = pks
        self.foreign_keys = foreign_keys
        self.settings = settings

    def pre_print(self):
        self.full_table_name = '%s%s%s' % (settings['table_prefix'],
                self.table_name, settings['table_postfix'])

    def get_fks_string(self):
        self.pre_print()
        t = Template(filename='migrate_oracle_to_nz_ddl_fks.sql')
        return t.render(table=self, settings=self.settings)

    def __str__(self):
        self.pre_print()
        t = Template(filename='migrate_oracle_to_nz_ddl.sql')
        return t.render(table=self, settings=self.settings)

def to_int(x):
    try:
        return int(x)
    except:
        return x

class Column:
    def __init__(self, column_name, data_type, column_size,
            decimal_digits=None, is_nullable='NO'):

        self.column_name = validate_column_name(column_name)
        self.data_type = data_type
        self.column_size = to_int(column_size)
        self.decimal_digits = to_int(decimal_digits)
        self.is_nullable = is_nullable
        self.nz_column = self.to_nz_column()

    def to_nz_column(self):
        decimal_digits_string = ['', ',%s' % self.decimal_digits][self.decimal_digits != None]

        if self.data_type == 'DECIMAL':
            return 'NUMERIC(%s%s)' % (self.column_size,
                    self.decimal_digits_string)

        if self.data_type in ('CHAR'):
            return 'CHAR(%s)' % (min(settings['max_string_len'], self.column_size))

        if self.data_type in ('VARCHAR'):
            return 'VARCHAR(%s)' % (min(settings['max_string_len'], self.column_size))

        if self.data_type in ('VARCHAR2', 'CLOB', 'BLOB', 'NVARCHAR'):
            return 'NVARCHAR(%s)' % (min(settings['max_string_len'], self.column_size))
        
        if self.data_type in ('TIMESTAMP', 'DATE') or self.data_type.startswith('TIMESTAMP'):
            return 'TIMESTAMP'

        if self.data_type in ('LONG'):
            return 'BIGINT'

        if self.data_type in ('NUMERIC', 'NUMBER'):
            if self.decimal_digits != None:
                if self.decimal_digits == 0:
                    if 1 <= self.column_size < 2:
                        return 'BYTEINT'
                    elif 2 <= self.column_size < 6:
                        return 'SMALLINT'
                    elif 6 <= self.column_size < 12:
                        return 'INTEGER'
                    elif 12 <= self.column_size < 20:
                        return 'BIGINT'

            return 'NUMERIC(%s%s)' % (self.column_size,
                    decimal_digits_string)

        return 'NVARCHAR(%s) /*default*/' % (min(settings['max_string_len'], max(1, self.column_size)))
    
    def __str__(self):
        s = '%s %s' % (self.column_name, self.nz_column)
        if self.is_nullable == 'NO' or settings['all_not_null']:
            s += ' NOT NULL'
        return s


# settings
settings_file = 'migrate_oracle_to_nz_ddl.yml'
if not os.path.exists(settings_file):
    shutil.copy(settings_file + '.example', settings_file)

definition = yaml.load(open(settings_file))
settings = definition['settings']
print settings

# connect
odbc_conn = pyodbc.connect('DSN=%s;PWD=%s' % (settings['odbc_dns'], settings['password']))
odbc_cur = odbc_conn.cursor()

# do it
db_tables = list(odbc_cur.tables(schema=settings['schema'], tableType='TABLE'))
tables = []

for table in db_tables[:]:
    if to_include(table.table_name, settings['include'], settings['exclude'],
            settings['others_remove']):
        #print table.table_name
        # columns
        db_columns = get_columns(odbc_cur, settings['schema'], table.table_name)
        columns = [Column(i.COLUMN_NAME, i.DATA_TYPE, i.COLUMN_SIZE,
            i.DECIMAL_DIGITS, i.IS_NULLABLE) for i in db_columns]
        # pk
        pks = list(odbc_cur.primaryKeys(schema=settings['schema'], table=table.table_name))
        # fk
        db_foreign_keys = list(odbc_cur.foreignKeys(schema=settings['schema'], foreignTable=table.table_name))
        #print db_foreign_keys
        # table
        t = Table(table.table_name, settings['schema'], columns, pks, db_foreign_keys, settings)
        table_ddl =  str(t)
        if not 'PRIMARY KEY' in table_ddl:
            table_ddl = table_ddl.replace(',\r\n)\r\n    DISTRIBUTE ON', '\r\n)\r\n    DISTRIBUTE ON')

        print table_ddl
        tables.append(t)

for t in tables:
    print t.get_fks_string()
