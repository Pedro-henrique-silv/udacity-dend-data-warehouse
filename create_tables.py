import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
        Drop existing tables from AWS redshift
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
        Create tables into AWS redshift
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
        Connect to AWS redshift and executes drop_tables and create_tables functions
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    #DWH_DB_USER = config.get('CLUSTER','LOG_DATA')
    #DWH_DB_PASSWORD = 
    #HOST = 
    #DWH_PORT
    #DWH_DB
    

    
    #conn_string="postgresql://{}:{}@{}:{}/{}".format(DWH_DB_USER, DWH_DB_PASSWORD, HOST, DWH_PORT,DWH_DB)

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    #conn = psycopg2.connect(conn_string)
    cur = conn.cursor()
    

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()