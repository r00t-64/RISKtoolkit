from sqlalchemy import create_engine
import cx_Oracle

class OracleConnection:
    def __init__(self, user_lc: str = '', pass_lc: str = '', sid_lc: str = ''):
        self.cursor = None
        self.engine = None
        self.__user_lc = user_lc
        self.__pass_lc = pass_lc
        self.__sid_lc = sid_lc
        self.__connection_string = f'oracle://{self.__user_lc}:{self.__pass_lc}@{self.__sid_lc}'
        self._status_code = 'ORA-03114'
        self._status_number = 3114
        self.init_engine()
        self.init_cursor()


    def status(self):
        try:
            self.cursor.execute("SELECT 'Connected' FROM dual")
            result = self.cursor.fetchone()
            if result:
                self._status_code = 'ORA-00000'
                self._status_number=0
            
        except cx_Oracle.DatabaseError as e:
            error, = e.args
            self._status_code = f'ORA-{error.code}'
            self._status_number= int(error.code)  
        except Exception as e:
            self._status_code = 'ORA-50000'
            self._status_number = 5000
        
        return self._status_code

    def init_engine(self,):
        try:
            self.engine = create_engine(
                self.__connection_string,
                pool_recycle=10,
                pool_size=50,
                echo=False
            )
            self._status_code = 'ORA-00000'
            self._status_number=0
            
        except cx_Oracle.DatabaseError as e:
            error, = e.args
            self._status_code = f'ORA-{error.code}'
            self._status_number= int(error.code)  

    def init_cursor(self):
        try:
            self._connection = cx_Oracle.connect(
                self.__user_lc,
                self.__pass_lc,
                self.__sid_lc,
                encoding='UTF-8',
                threaded=True
            )
            self.cursor = self._connection.cursor()
            self._status_code = 'ORA-00000' 
            self._status_number=0
        except cx_Oracle.DatabaseError as e:
            error, = e.args
            self._status_code = f'ORA-{error.code}'
            self._status_number= int(error.code)  