import configparser
import psycopg2
from sql_create_tables import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    #Drops all the Tables in the Database
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    #Creates all the tebles
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    #Read in the dwh.cfg file and setup the connection to Redshift
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    HOST                    = config.get('CLUSTER','HOST')
    DB_NAME                 = config.get('CLUSTER','DB_NAME')
    DB_USER                 = config.get('CLUSTER','DB_USER')
    DB_PASSWORD             = config.get('CLUSTER','DB_PASSWORD')
    DB_PORT                 = config.get('CLUSTER','DB_PORT')
    ARN                     = config.get('IAM_ROLE','ARN')
    LOG_DATA                = config.get('S3','LOG_DATA')
    LOG_JSONPATH            = config.get('S3','LOG_JSONPATH')
    SONG_DATA               = config.get('S3','SONG_DATA')


    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(
            HOST,
            DB_NAME,
            DB_USER,
            DB_PASSWORD,
            DB_PORT
            )
        )
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()