import psycopg2
from setting import DB_CONNECTION

def create_conn():
    conn_string = '''host='%(host)s', port=%(port)s,dbname='%(dbname)s',
    user='%(user)s', password='%(password)s' '''%DB_CONNECTION
    conn = psycopg2.connect(conn_string)
    return conn

def run_update(sql):
    conn = create_conn()
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()

def run_get(sql):
    conn = create_conn()
    cur = conn.cursor()
    cur.execute(sql)
    data = cur.fetchall()
    cur.close()
    conn.close()
    return data