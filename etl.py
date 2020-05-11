import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Copy data from the source to staging given the curson and the connection"""
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Insert data from the staging area into the dimension and fact tables as specified"""
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    print("Connecting to the cluster...\n")
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print("Connected to the cluster\n")
    
    print("Copying the data from json...\n")
    load_staging_tables(cur, conn)
    print("Copied the data succesfully\n")
    
    print("Inserting the data into the table\n")
    insert_tables(cur, conn)
    
    conn.close()
    print('ETL Ended')
    
    


if __name__ == "__main__":
    main()