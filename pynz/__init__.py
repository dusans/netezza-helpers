import pyodbc

def getDbConn(server, db, user, password=None):
    if password == None:
        password = getpass()

    try:
        #password =
        conn = pyodbc.connect('DRIVER={NetezzaSQL};SERVER=%s;DATABASE=%s;UID=%s;PWD=%s;' % (server, db, user, password))
        return conn
    except Exception, e:
        print e
        return None

def query(db, sql):
    #print sql
    try:
        sql = db.execute(sql)
        return sql
    except Exception, e:
        #print e
        return []

def execute(db, sql):
    #print sql
    try:
        sql = db.execute(sql)
    except Exception, e:
        print e
        return []
