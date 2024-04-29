from sqlalchemy import ForeignKey, create_engine, MetaData, Table, Column, Integer, DECIMAL, String, select, Boolean
from sqlalchemy.engine import URL
from sqlalchemy.orm import sessionmaker, relationship, declarative_base, DeclarativeBase
import os


POSTGRES_DB_ADDRESS = os.getenv("POSTGRES_DB_ADDRESS", "localhost")
POSTGRES_DB_PORT = os.getenv("POSTGRES_DB_PORT", "5432")
POSTGRES_DM_USERNAME = os.getenv("POSTGRES_DM_USERNAME")
POSTGRES_DM_PASSWORD = os.getenv("POSTGRES_DM_PASSWORD")



class Base(DeclarativeBase):
    pass


class CRUD():

    def save(self, db_session):
        if self.id == None:
            db_session.add(self)
                
        return db_session.commit()

    def destroy(self, db_session):
        db_session.delete(self)
        return db_session.commit()
        
    # def destroy_row(self, db_session, row_id):
    #     result = db_session.query(ChildDm).filter(ChildDm.id == row_id).first()
    #     if result is not None:
    #         db_session.delete(result)
    #         return db_session.commit()

class ChildDm(Base, CRUD):
    __tablename__ = "child_dm"

    id = Column(Integer, primary_key=True, index=True)
    parent_id = Column(Integer, ForeignKey("parent_dm.id"))
    child_number_enum_id = Column(Integer, ForeignKey("child_number_enum_dm.id"))
    row_name = Column(String)
    row_desc = Column(String)
    flag = Column(Boolean)
    parent = relationship("ParentDm", back_populates="childs")
    child_number = relationship("ChildNumberEnumDm", back_populates="child")

    def __init__(self, parent_id, child_number_enum_id, row_name, row_desc, flag):
        # self.id = id
        self.parent_id = parent_id
        self.child_number_enum_id = child_number_enum_id
        self.row_name = row_name
        self.row_desc = row_desc
        self.flag = flag
    

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

    child = relationship("ChildDm", back_populates="child_number")
   

class Testing:
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
        self._current_new_row = None

    def insert_row_to_child_table(self):
        self.fetch_all_from_table("child_dm")
        new_id = self.last_id + 1
        self._current_new_row = ChildDm(child_number_enum_id=3, parent_id=3, row_name=f"row_{new_id}", row_desc=f"row_{new_id} description", flag=True)
        _temp_session = sessionmaker(bind=self.engine)
        _temp_session_for_table = _temp_session()
        self._current_new_row.save(_temp_session_for_table)
        _temp_session_for_table.close()

    def delete_current_row_from_child_table(self):
        self.fetch_all_from_table("child_dm")
        _temp_session = sessionmaker(bind=self.engine)
        _temp_session_for_table = _temp_session()
        self._current_new_row.destroy(_temp_session_for_table)
        _temp_session_for_table.close()

    def fetch_all_from_table(self, table_name):
        # Reflect the existing table structure
        existing_table = Table(table_name, self.metadata, autoload_with=self.engine)

        select_statement = existing_table.select()
        print(select_statement)
        with self.engine.connect() as connection:
            result = connection.execute(select_statement)
            
            # Iterate over the result set
            for row in result:
                self.last_id = row.id if self.last_id is None or row.id > self.last_id else self.last_id
                print(row)  # Process the retrieved data here
             

if __name__=="__main__":
    test = Testing(dialect="postgresql", driver="psycopg2", username=POSTGRES_DM_USERNAME, password=POSTGRES_DM_PASSWORD, host_addr=POSTGRES_DB_ADDRESS, port=POSTGRES_DB_PORT, database_name="testing")
    test.insert_row_to_child_table()
    test.delete_current_row_from_child_table()
    test.fetch_all_from_table("child_dm")
    