import pyodbc
from mako.template import Template
import yaml
import os
import shutil

def to_include(table, include, exclude, others_remove):
    if table in include:
        return True

    for e in exclude:
        if table.startswith(e):
            return False

    if others_remove == True:
        return False

    return True


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
    def __str__(self):
        self.pre_print()
        t = Template(filename='migrate_oracle_to_nz_ddl.sql')
        return t.render(table=self, settings=self.settings)

class Column:
    def __init__(self, column_name, data_type, column_size,
            decimal_digits=None, is_nullable='NO'):

        self.column_name = column_name
        self.data_type = data_type
        self.column_size = column_size
        self.decimal_digits = decimal_digits
        self.is_nullable = is_nullable
        self.nz_column = self.to_nz_column()

    def to_nz_column(self):
        decimal_digits_string = ['', ',%s' % self.decimal_digits][self.decimal_digits != None]

        if self.data_type == 'DECIMAL':
            return 'NUMERIC(%s%s)' % (self.column_size,
                    self.decimal_digits_string)

        if self.data_type in ('VARCHAR', 'NVARCHAR', 'CLOB', 'BLOB'):
            return 'NVARCHAR(%s)' % (self.column_size)
        
        if self.data_type in ('TIMESTAMP', 'DATE'):
            return 'TIMESTAMP'

        if self.data_type in ('NUMERIC', 'NUMBER'):
            if 1 <= self.column_size < 2:
                return 'BYTEINT'
            elif 2 <= self.column_size < 6:
                return 'SMALLINT'
            elif 6 <= self.column_size < 12:
                return 'INTEGER'
            elif 12 <= self.column_size < 20:
                return 'BYTEINT'
            else:
                return 'NUMERIC(%s%s)' % (self.column_size,
                        self.decimal_digits_string)

        return 'NVARCHAR'
    
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
db_tables = list(odbc_cur.tables(schema=settings['schema'], table='PARTY', tableType='TABLE'))
tables = []

for table in db_tables[:1]:
    if to_include(table.table_name, settings['include'], settings['exclude'],
            settings['others_remove']):
        #print table.table_name
        # columns
        db_columns = get_columns(odbc_cur, settings['schema'], table.table_name)
        columns = [Column(i.COLUMN_NAME, i.DATA_TYPE, i.COLUMN_SIZE,
            i.DECIMAL_DIGITS, i.IS_NULLABLE) for i in db_columns]
        # pk
        pks = list(odbc_cur.primaryKeys(table=table.table_name))
        # fk
        db_foreign_keys = list(odbc_cur.foreignKeys(foreignTable=table.table_name))
        #print db_foreign_keys
        # table
        t = Table(table.table_name, settings['schema'], columns, pks, db_foreign_keys, settings)
        print t
        tables.append(t)
           
