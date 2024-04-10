import psycopg2
import os


POSTGRES_DB_ADDRESS = os.getenv("POSTGRES_DB_ADDRESS", "localhost")
POSTGRES_DB_PORT = os.getenv("POSTGRES_DB_PORT", "5432")
POSTGRES_DM_USERNAME = os.getenv("POSTGRES_DM_USERNAME")
POSTGRES_DM_PASSWORD = os.getenv("POSTGRES_DM_PASSWORD")

class Psycopg2Driver:
    def __init__(self, username:str, password:str, host_addr:str, port:str, database_name:str) -> None:
        self._machine_id = "machine123"
        self.database_name = database_name
        self.host_addr = host_addr
        self.port = port
        self.create_database(self.database_name, username, password, self.host_addr, self.port)
        self._conn_str = f"dbname='{self.database_name}' user='{username}' password='{password}' host='{self.host_addr}' port='{self.port}'"

    def check_database_existence(self, cursor, db_name):
        cursor.execute("SELECT datname FROM pg_database;")
        databases = [row[0] for row in cursor.fetchall()]
        print(f"existed databases - {databases}")
        return db_name.lower() in databases
        
    def create_database(self, db_name, username, password, host='localhost', port='5432'):
        try:
            _temp_db_connection = psycopg2.connect(f"user='{username}' password='{password}' host='{host}' port='{port}'")
            _temp_db_connection.autocommit = True
            with _temp_db_connection.cursor() as cur:
                if not self.check_database_existence(cursor=cur, db_name=db_name):
                    _sql_command_str = "CREATE DATABASE %s;"%db_name
                    cur.execute(_sql_command_str)
                    print(f"Database {db_name} created successfully.")
        except Exception as e:
            print(f"(f) check_database_existence - An error occured: {e}")
            raise SystemExit(1)
        
    def check_table_existence(self, cursor, table_name):
        cursor.execute(
                f"""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_name = '{table_name}'
                    );
                """
            )
        return cursor.fetchone()[0]

    def create_test_table(self, table_name):
        try:
            _temp_db_connection = psycopg2.connect(self._conn_str)
            _temp_db_connection.autocommit = True
            with _temp_db_connection.cursor() as cur:
                if not self.check_table_existence(cur, table_name):
                    _sql_create_table_str = f"""
                            CREATE TABLE {table_name} (
                                id SERIAL PRIMARY KEY,
                                status VARCHAR(50) NOT NULL,
                                status_desc VARCHAR(255) NOT NULL,
                                status_level INTEGER NOT NULL,
                                color_code VARCHAR(50) NOT NULL,
                                timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                            );
                        """
                    cur.execute(_sql_create_table_str)
                    print(f"Table {table_name} created successfully.")
        except Exception as e:
            print(f"(f) create_test_table - An error occured: {e}")
            raise SystemExit(1)
 
if __name__=="__main__":
    test = Psycopg2Driver(username=POSTGRES_DM_USERNAME, password=POSTGRES_DM_PASSWORD, host_addr=POSTGRES_DB_ADDRESS, port=POSTGRES_DB_PORT, database_name="testdb")
    test.create_test_table("table_test1")