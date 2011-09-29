import time
import re
import os
import sys

if len(sys.argv) < 2:
    print 'Usage: python erwin_to_nz_sql.py path_to_file [table_prefix]'
else:
    sql_file_path = sys.argv[1]
    if len(sys.argv) > 2:
        table_prefix = sys.argv[2]
    else:
        table_prefix = ''

    def changetype(t):
        m = {}
        t = t.replace("DECIMAL(", "NUMERIC(")
        t = t.replace("VARCHAR(", "NVARCHAR(")
        t = t.replace("VARCHAR2(", "NVARCHAR(")
        t = re.sub("TIMESTAMP\(\d+\)", "TIMESTAMP", t)
        t = re.sub("CLOB\(\d+\)", "NVARCHAR(4000)", t)
        t = re.sub("BLOB\(\d+\)", "NVARCHAR(4000)", t)

        t = t.replace("NUMERIC(1)", "BYTEINT")

        t = t.replace("NUMERIC(2)", "SMALLINT")
        t = t.replace("NUMERIC(3)", "SMALLINT")
        t = t.replace("NUMERIC(4)", "SMALLINT")
        t = t.replace("NUMERIC(5)", "SMALLINT")

        t = t.replace("NUMERIC(6)", "INTEGER")
        t = t.replace("NUMERIC(7)", "INTEGER")
        t = t.replace("NUMERIC(9)", "INTEGER")
        t = t.replace("NUMERIC(10)", "INTEGER")

        t = t.replace("NUMERIC(12)", "BIGINT")
        t = t.replace("NUMERIC(13)", "BIGINT")
        t = t.replace("NUMERIC(15)", "BIGINT")
        t = t.replace("NUMERIC(19)", "BIGINT")

        return t

    if os.path.exists(sql_file_path):
        erwin_forward = open(sql_file_path).read()
        create_table = re.findall("CREATE\sTABLE.*?\;", erwin_forward, re.MULTILINE + re.DOTALL)
        alter_table = re.findall("ALTER\sTABLE.*?;", erwin_forward, re.MULTILINE + re.DOTALL)

        print "--", "*" * 40
        print "-- %s" % "ERWIN TO NETEZZA SQL @ %s" % time.ctime()
        print "--", "*" * 40
        print "-- %s" % "TABLES"
        print "--", "*" * 40

        nima_pk = []

        for table in create_table:
            table_name = re.split("[\s\n]", table)[2]

            print "\n"
            print "-" * 20
            print "-- %s%s" % (table_prefix, table_name)
            print "-" * 20
            print "-- DROP TABLE %s%s;" % (table_prefix, table_name)
            table2 = changetype(table)
            table2 = table2.replace("CREATE TABLE ", "CREATE TABLE %s" % table_prefix)
            try:
                primary_key = re.findall("PRIMARY\sKEY\s\(.+\)", table2)[0].split("(")[1][:-1]
                distribute_on = "(%s)" % primary_key
            except:
                distribute_on = "RANDOM"
                nima_pk.append(table_name)

            print re.sub("\)\n\;", ") DISTRIBUTE ON %s;" % distribute_on, table2)

        print "\n" * 3
        print "--", "*" * 40
        print "-- %s" % "FOREIN KEYS"
        print "--", "*" * 40
        for alter in alter_table:
            alter = alter.replace("FOREIGN KEY", "CONSTRAINT")

            alter = alter.splitlines()
            table_name = alter[0].split(" ")[2]
            alter[0] = alter[0].replace("ALTER TABLE ", "ALTER TABLE %s" % table_prefix)
            alter[1] = alter[1].split(" ")
            alter[1].insert(3, "FOREIGN KEY")
            alter[1] = " ".join(alter[1])
            alter[1] = alter[1].replace("REFERENCES ", "REFERENCES %s" % table_prefix)

            print "\n".join(alter)

