import csv
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, select, Boolean
from sqlalchemy.engine import URL
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import os


POSTGRES_DB_ADDRESS = os.getenv("POSTGRES_DB_ADDRESS", "localhost")
POSTGRES_DB_PORT = os.getenv("POSTGRES_DB_PORT", "5432")
POSTGRES_DM_USERNAME = os.getenv("POSTGRES_DM_USERNAME")
POSTGRES_DM_PASSWORD = os.getenv("POSTGRES_DM_PASSWORD")

Base = declarative_base()
class ChildDm(Base):
    __tablename__ = "child_dm"

    id = Column(Integer, primary_key=True, index=True)
    parent_id = Column(Integer)
    row_name = Column(String)
    row_desc = Column(String)
    flag = Column(Boolean)
   

class Whatever:
    def __init__(self, dialect:str, driver:str, username:str, password:str, host_addr:str, port:str, database_name:str) -> None:
        self.dialect = dialect
        self.driver = driver
        self.host_addr = host_addr
        self.port = port
        self.database_name = database_name
        url = URL.create(
            drivername=f"{self.dialect}+{self.driver}",
            username=username,
            password=password,
            host=self.host_addr,
            port=self.port,
            database=self.database_name
        )
        self.engine = create_engine(url=url)
        self.metadata = MetaData()
        self.defined_tables_dict = {
            'table_test1': Table(
                'table_test1', self.metadata,
                Column('id', Integer, primary_key=True),
                Column('status', String),
                Column('status_desc', String),
                Column('status_level', Integer),
                Column('color_code', String)
            )
        }
        self.last_id = None

    def insert_csv_data_to_table(self, table_name, csv_file_path):
        if table_name in self.defined_tables_dict.keys():
            with open(csv_file_path, 'r') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    print(f"inserting data - {row}")
                    self.engine.execute(self.defined_tables_dict[table_name].insert().values(**row))

    def insert_row_to_table(self):
        new_id = self.last_id + 1
        new_row = ChildDm(id=new_id, parent_id=3, row_name=f"row_{new_id}", row_desc=f"row_{new_id} description", flag=True)
        _temp_session = sessionmaker(bind=self.engine)
        _temp_session_for_table = _temp_session()
        _temp_session_for_table.add(new_row)
        _temp_session_for_table.commit()
        _temp_session_for_table.close()

    def fetch_all_from_table(self, table_name):
        # Reflect the existing table structure
        existing_table = Table(table_name, self.metadata, autoload_with=self.engine)

        select_statement = existing_table.select()
        print(select_statement)
        # Construct a select statement
        # select_statement = select([existing_table.columns.id])

        # Execute the select statement
        with self.engine.connect() as connection:
            result = connection.execute(select_statement)
            
            # Iterate over the result set
            for row in result:
                self.last_id = row.id if self.last_id is None or row.id > self.last_id else self.last_id
                print(row)  # Process the retrieved data here
             

if __name__=="__main__":
    test = Whatever(dialect="postgresql", driver="psycopg2", username=POSTGRES_DM_USERNAME, password=POSTGRES_DM_PASSWORD, host_addr=POSTGRES_DB_ADDRESS, port=POSTGRES_DB_PORT, database_name="testing")
    # test.insert_csv_data_to_table("table_test1", "app/initialdata/csvdata/table_test1.csv")
    test.fetch_all_from_table("child_dm")
    test.insert_row_to_table()
    test.fetch_all_from_table("child_dm")
    