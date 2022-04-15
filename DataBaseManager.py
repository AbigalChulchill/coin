import psycopg2
from setting import DB_CONNECTION

def create_conn():
    conn = psycopg2.connect(**DB_CONNECTION)
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


if __name__ == "__main__":
    print(0)
    # conn_string =
    # print(**DB_CONNECTION)
    psycopg2.connect(**DB_CONNECTION)