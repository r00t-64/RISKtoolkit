from sqlalchemy import create_engine, text
import oracledb
from typing import Optional
import pymssql
from sqlalchemy import create_engine
import psycopg2

class OracleDBConnection:
    def __init__(self, user_lc: str = '', pass_lc: str = '', sid_lc: str = '', host:str = None, port:str = None,service:str = None):
        self.cursor = None
        self.engine = None
        self.__user_lc = user_lc
        self.__pass_lc = pass_lc
        self.__sid_lc = sid_lc
        self.__host = host
        self.__port = port
        self.__service = service
        self.__connection_string = f'oracle+oracledb://{self.__user_lc}:{self.__pass_lc}@{self.__host}:{self.__port}/?service_name={self.__service}'
        self._status_code = 'ORA-03114'
        self._status_number = 3114
        self.init_cursor()
        self.init_engine()

    def status(self):
        try:
            self.cursor.execute("SELECT 'Connected' FROM dual")
            result = self.cursor.fetchone()
            if result:
                self._status_code = 'ORA-00000'
                self._status_number = 0
            
        except oracledb.DatabaseError as e:
            error, = e.args
            self._status_code = f'ORA-{error.code}'
            self._status_number = int(error.code)  
        except Exception as e:
            self._status_code = 'ORA-50000'
            self._status_number = 5000
        
        return self._status_code

    def init_engine(self):
        try:
            self.engine = create_engine(
                self.__connection_string,
                pool_recycle=10,
                pool_size=50,
                echo=False
            )
            self._status_code = 'ORA-00000'
            self._status_number=0
            
        except oracledb.DatabaseError as e:
            error, = e.args
            self._status_code = f'ORA-{error.code}'
            self._status_number = int(error.code)
        except Exception as e:
            self._status_code = 'ORA-50000'
            self._status_number = 50000

    def init_cursor(self):
        try:
            oracledb.init_oracle_client()
            if self.__host and self.__port and self.__service:
                dsn = f"{self.__host}:{self.__port}/{self.__service}"
            else:
                dsn = self.__sid_lc

            self._connection = oracledb.connect(
                user=self.__user_lc,
                password=self.__pass_lc,
                dsn=dsn,
                mode=oracledb.AUTH_MODE_DEFAULT,
            )
            print(self._connection)
            self.cursor = self._connection.cursor()
            self._status_code = 'ORA-00000' 
            self._status_number = 0
        except oracledb.DatabaseError as e:
            error, = e.args
            print(str(e))
            self._status_code = f'ORA-{error.code}'
            self._status_number = int(error.code)

class MssqlConnection:
    def __init__(self, user_lc: str = '', pass_lc: str = '', sid_lc: str = '', server:str = None, database:str = None,port:str = None):
        self.cursor = None
        self.engine = None
        self._connection = None
        self.__user_lc = user_lc
        self.__pass_lc = pass_lc
        self.__server = server
        self.__database = database
        self.__port = port
        self._status_code = 'MSSQL-03114'  # Default status: not connected
        self._status_number = 3114
        self.__connection_string = f"mssql+pymssql://{self.__user_lc}:{self.__pass_lc}@{self.__server}/{self.__database}"
        self.init_cursor()
        self.init_engine()
 
    def status(self) -> str:
        """Check and return the current connection status."""
        try:
            if self.cursor:
                self.cursor.execute("SELECT 1")
                result = self.cursor.fetchone()
                if result:
                    self._status_code = 'MSSQL-00000'
                    self._status_number = 0
            return self._status_code
           
        except pymssql.Error as e:
            self._status_code = f'MSSQL-{e.args[0]}'
            self._status_number = e.args[0]
            return self._status_code
        except Exception as e:
            self._status_code = 'MSSQL-50000'
            self._status_number = 50000
            return self._status_code
 
    def init_engine(self):
        """Initialize the SQLAlchemy engine if needed."""
        try:
            if self._connection:
                self.engine = self._connection
                self._status_code = 'MSSQL-00000'
                self._status_number = 0
        except pymssql.Error as e:
            self._status_code = f'MSSQL-{e.args[0]}'
            self._status_number = e.args[0]
 
    def init_cursor(self):
        """Initialize the MSSQL cursor connection."""
        try:
            if self.__server and self.__port:
                server = f"{self.__server}:{self.__port}"
            else:
                server = self.__server
           
            self._connection = pymssql.connect(
                server=server,
                user=self.__user_lc,
                password=self.__pass_lc,
                database=self.__database
            )
           
            self.cursor = self._connection.cursor()
            self._status_code = 'MSSQL-00000'
            self._status_number = 0
           
        except pymssql.Error as e:
            self._status_code = f'MSSQL-{e.args[0]}'
            self._status_number = e.args[0]
        except Exception as e:
            print(str(e))
            self._status_code = 'MSSQL-50000'
            self._status_number = 50000
 
    def close(self):
        """Close the database connection."""
        try:
            if self.cursor:
                self.cursor.close()
            if self._connection:
                self._connection.close()
            self._status_code = 'MSSQL-00001'  # Closed status
            self._status_number = 1
        except Exception as e:
            self._status_code = 'MSSQL-50001'
            self._status_number = 50001
 
    def __enter__(self):
        """Support for context manager protocol."""
        return self
 
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Ensure connections are closed when exiting context."""
        self.close()
 
    def execute_query(self, query: str, params: Optional[tuple] = None):
        """Execute a SQL query and return results."""
        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
           
            # For SELECT statements
            if query.strip().upper().startswith('SELECT'):
                return self.cursor.fetchall()
            # For INSERT/UPDATE/DELETE
            else:
                self._connection.commit()
                return self.cursor.rowcount
               
        except pymssql.Error as e:
            self._connection.rollback()
            self._status_code = f'MSSQL-{e.args[0]}'
            self._status_number = e.args[0]
            raise

class PostgresDBConnection:
    def __init__(self, user: str = '', password: str = '', dbname: str = '', host: str = 'localhost', port: str = '5432'):
        self.cursor = None
        self.engine = None
        self.__user = user
        self.__password = password
        self.__dbname = dbname
        self.__host = host
        self.__port = port
        self.__connection_string = f'postgresql://{self.__user}:{self.__password}@{self.__host}:{self.__port}/{self.__dbname}'
        self._status_code = 'PG-00000'
        self._status_number = 0
        self.init_cursor()
        self.init_engine()

    def status(self):
        try:
            self.cursor.execute("SELECT 1")
            result = self.cursor.fetchone()
            if result:
                self._status_code = 'PG-00000'
                self._status_number = 0
        except psycopg2.DatabaseError as e:
            self._status_code = f'PG-{e.pgcode}'
            self._status_number = int(e.pgcode) if e.pgcode else -1
        except Exception as e:
            self._status_code = 'PG-50000'
            self._status_number = 50000
        return self._status_code

    def init_engine(self):
        try:
            self.engine = create_engine(
                self.__connection_string,
                pool_recycle=10,
                pool_size=50,
                echo=False
            )
            self._status_code = 'PG-00000'
            self._status_number = 0
        except Exception as e:
            self._status_code = 'PG-50000'
            self._status_number = 50000

    def init_cursor(self):
        try:
            self._connection = psycopg2.connect(
                user=self.__user,
                password=self.__password,
                dbname=self.__dbname,
                host=self.__host,
                port=self.__port
            )
            self.cursor = self._connection.cursor()
            self._status_code = 'PG-00000'
            self._status_number = 0
        except psycopg2.DatabaseError as e:
            self._status_code = f'PG-{e.pgcode}'
            self._status_number = int(e.pgcode) if e.pgcode else -1
        except Exception as e:
            self._status_code = 'PG-50000'
            self._status_number = 50000