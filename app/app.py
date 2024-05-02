import csv
from datetime import datetime, timezone
from sqlalchemy import ForeignKey, create_engine, MetaData, Table, Column, Integer, DECIMAL, String, select, Boolean, TIMESTAMP
from sqlalchemy.engine import URL
from sqlalchemy.orm import sessionmaker, relationship, declarative_base, DeclarativeBase
import os


POSTGRES_DB_ADDRESS = os.getenv("POSTGRES_DB_ADDRESS", "localhost")
POSTGRES_DB_PORT = os.getenv("POSTGRES_DB_PORT", "5432")
POSTGRES_DM_USERNAME = os.getenv("POSTGRES_DM_USERNAME")
POSTGRES_DM_PASSWORD = os.getenv("POSTGRES_DM_PASSWORD")

# Base = declarative_base()

class Base(DeclarativeBase):
    pass

class ChildDm(Base):
    __tablename__ = "child_dm"

    id = Column(Integer, primary_key=True, index=True)
    parent_id = Column(Integer, ForeignKey("parent_dm.id"))
    child_number_enum_id = Column(Integer, ForeignKey("child_number_enum_dm.id"))
    row_name = Column(String)
    row_desc = Column(String)
    flag = Column(Boolean)
    timestamp = Column(TIMESTAMP)
    parent = relationship("ParentDm", back_populates="childs")
    child_number = relationship("ChildNumberEnumDm")
    # child_number = relationship("ChildNumberEnumDm", back_populates="child")

class ParentDm(Base):
    __tablename__ = "parent_dm"

    id = Column(Integer, primary_key=True, index=True)
    grandparent_id = Column(Integer, ForeignKey("grandparent_dm.id"))
    row_name = Column(String)
    row_desc = Column(String)
    int_val = Column(Integer)
    decimal_val = Column(DECIMAL)
    flag = Column(Boolean)
    parent = relationship("GrandParentDm", back_populates="childs")
    childs = relationship("ChildDm", back_populates="parent")

class GrandParentDm(Base):
    __tablename__ = "grandparent_dm"

    id = Column(Integer, primary_key=True, index=True)
    row_name = Column(String)
    row_desc = Column(String)
    int_val = Column(Integer)
    decimal_val = Column(DECIMAL)
    flag = Column(Boolean)
    childs = relationship("ParentDm", back_populates="parent")

class ChildNumberEnumDm(Base):
    __tablename__ = "child_number_enum_dm"

    id = Column(Integer, primary_key=True, index=True)
    number_desc = Column(String)

    # child = relationship("ChildDm", back_populates="child_number")
   


# Function to convert datetime object to string
def datetime_to_string(dt_obj):
    return dt_obj.strftime("%Y-%m-%d %H:%M:%S.%f%z")

# Function to convert string to datetime object
def string_to_datetime(dt_str):
    return datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S.%f%z")

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

    def insert_row_to_child_table(self):
        self.fetch_all_from_table("child_dm")
        new_id = self.last_id + 1
        new_row = ChildDm(id=new_id, child_number_enum_id=3, parent_id=3, row_name=f"row_{new_id}", row_desc=f"row_{new_id} description", flag=True)
        _temp_session = sessionmaker(bind=self.engine)
        _temp_session_for_table = _temp_session()
        _temp_session_for_table.add(new_row)
        _temp_session_for_table.commit()
        _temp_session_for_table.close()

    def fetch_child_info(self, child_id:int=None, parent_id:int=None, child_number:int=None, grandparent_id:int=None):
        _temp_session = sessionmaker(bind=self.engine)
        _temp_session_for_table = _temp_session()
        if child_id is not None:
            results = _temp_session_for_table.query(ChildDm).filter(ChildDm.id == child_id).all()
            for result in results:
                if result:
                    print("child name", result.row_name)
                    print("child id", result.id)
                    print("child parent id", result.parent.id)
                    print("child grandparent id", result.parent.parent.id)
                    print("child number", result.child_number.number_desc)
                else:
                    print("Not Found")
        elif grandparent_id is not None:
            if parent_id is not None and child_number is not None:
                results = _temp_session_for_table.query(ChildDm).join(ParentDm, ParentDm.id == ChildDm.parent_id).join(GrandParentDm, ParentDm.grandparent_id == GrandParentDm.id).join(ChildNumberEnumDm, ChildNumberEnumDm.id == ChildDm.child_number_enum_id).filter((ParentDm.id == parent_id) & (GrandParentDm.id == grandparent_id) & (ChildNumberEnumDm.id == child_number)).all()
            elif parent_id is not None:
                results = _temp_session_for_table.query(ChildDm).join(ParentDm, ParentDm.id == ChildDm.parent_id).join(GrandParentDm, GrandParentDm.id == ParentDm.grandparent_id).filter(GrandParentDm.id == grandparent_id).filter(ParentDm.id == parent_id).all()
            elif child_number is not None:
                results = _temp_session_for_table.query(ChildDm).join(ParentDm, ParentDm.id == ChildDm.parent_id).join(GrandParentDm, GrandParentDm.id == ParentDm.grandparent_id).join(ChildNumberEnumDm, ChildNumberEnumDm.id == ChildDm.child_number_enum_id).filter(GrandParentDm.id == grandparent_id).filter(ChildNumberEnumDm.id == child_number).all()
            else:
                results = _temp_session_for_table.query(ChildDm).join(ParentDm, ParentDm.id == ChildDm.parent_id).join(GrandParentDm, ParentDm.grandparent_id == GrandParentDm.id).filter(GrandParentDm.id == grandparent_id).all()
            for result in results:
                if result:
                    print("child name", result.row_name)
                    print("child id", result.id)
                    print("child parent id", result.parent.id)
                    print("child grandparent id", result.parent.parent.id)
                    print("child number", result.child_number.number_desc)
                else:
                    print("Not Found.")
        elif parent_id is not None:
            if child_number is not None:
                results = _temp_session_for_table.query(ChildDm).join(ParentDm, ParentDm.id == ChildDm.parent_id).join(ChildNumberEnumDm, ChildNumberEnumDm.id == ChildDm.child_number_enum_id).filter(ParentDm.id == parent_id).filter(ChildNumberEnumDm.id == child_number).all()
            else:
                results = _temp_session_for_table.query(ChildDm).join(ParentDm, ParentDm.id == ChildDm.parent_id).join(GrandParentDm, ParentDm.grandparent_id == GrandParentDm.id).filter(ParentDm.id == parent_id).all()
            for result in results:
                if result:
                    print("child name", result.row_name)
                    print("child id", result.id)
                    print("child parent id", result.parent.id)
                    print("child grandparent id", result.parent.parent.id)
                    print("child number", result.child_number.number_desc)
                    print(type(result.parent.decimal_val),"parent decimal value", result.parent.decimal_val)
                    print(type(result.timestamp),"child row recorded at",result.timestamp)
                else:
                    print("Not Found.")
                # _datetime_now = datetime.now(timezone.utc)
                # print(type(_datetime_now),"new timestamp",_datetime_now)
                # dt_str = datetime_to_string(_datetime_now)
                # print(type(dt_str),"new timestamp str",dt_str)
                # _new_timestamp = string_to_datetime(dt_str)
                # result.parent.decimal_val = 654.88
                # result.timestamp = _new_timestamp
                # _temp_session_for_table.commit()
            
        elif child_number is not None:
            results = _temp_session_for_table.query(ChildDm).join(ChildNumberEnumDm, ChildNumberEnumDm.id == ChildDm.child_number_enum_id).filter(ChildNumberEnumDm.id == child_number).all()
            for result in results:
                if result:
                    print("child name", result.row_name)
                    print("child id", result.id)
                    print("child parent id", result.parent.id)
                    print("child grandparent id", result.parent.parent.id)
                    print("child number", result.child_number.number_desc)
                else:
                    print("Not Found.")
        else:
            results = _temp_session_for_table.query(ChildDm).all()
            for result in results:
                if result:
                    print("child name", result.row_name)
                    print("child id", result.id)
                    print("child parent id", result.parent.id)
                    print("child grandparent id", result.parent.parent.id)
                    print("child number", result.child_number.number_desc)
                else:
                    print("Not Found.")
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
    test.fetch_child_info(parent_id=2, child_number=2)
    # test.insert_row_to_child_table()
    