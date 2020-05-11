import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Drop the tables on the the given cursor and connection"""
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
        


def create_tables(cur, conn):
    """Create the tables on the the given cursor and connection"""
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
        


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    print("Connecting to the cluster\n")
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print("Connection to the cluster established\n")

    drop_tables(cur, conn)
    print("Tables Dropped before creation\n")
    
    create_tables(cur, conn)
    print("Tables creation succesfull\n")

    conn.close()
    print("Connection to the cluster closed\n")


if __name__ == "__main__":
    main()