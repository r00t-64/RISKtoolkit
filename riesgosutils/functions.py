from sqlalchemy import create_engine, text,types
from datetime import datetime, timedelta
import re,os,json
import pandas as pd
import numpy as np
from exchangelib import Credentials, Account, Message, FileAttachment,HTMLBody
import re
import oracledb
import pandas as pd


""" 
def db_select(cursor, query, ): # takes the request text and sends it to the data_frame
    response = {"is_ok": True, "error": "", "content":None}

    try:
        if query.lower().endswith(".sql"):
            __sql_str = parse_query_file(query)
        else:
            if query.lower().startswith("select"):
                __sql_str = query
            else:
                __sql_str = 'select * from ' + query
        
        cursor.execute(__sql_str)
        __col = [column[0].lower() for column in cursor.description]
        __df = pd.DataFrame.from_records(cursor, columns=__col) 
        
        if len(__df) > 0:
            response["content"] = __df
        else:
            response["content"] = None
        

    except Exception as e:
        response = {"is_ok": False, "error": str(e), "content": None}

    finally:
        return response 

    def execute_query_from_file(cursor, file_sql, parameters, output_type='default'):
        with open(file_sql, 'r', encoding='latin-1') as __file:
            __query_template = __file.read()

        __query_str = __query_template.format(**parameters)
        cursor.execute(__query_str)
        __result = cursor.fetchall()

        if __result:
            if output_type == 'dataframe':
                __col_names = [column[0].lower() for column in cursor.description]
                __df = pd.DataFrame(__result, columns=__col_names)
                return __df  
            else:
                __result_list = [value for row in __result for value in row]
                return __result_list 
        else:
            return []  

"""
def funcion_sql(cursor, query, parameters=None, output_type='dataframe'):
    response = {"is_ok": True, "error": ""}
    output = None

    try:
        # Cargar SQL desde un archivo si el nombre del archivo termina en .sql
        if query.lower().endswith(".sql"):
            with open(query, 'r', encoding='latin-1') as file:
                query_template = file.read()
            # Formatear la consulta con los parámetros proporcionados
            query_str = query_template.format(**parameters)
        else:
            # Determinar la cadena de consulta según el input
            if len(query.split()) == 1:
                # Si la consulta es una sola palabra, se asume que es el nombre de una tabla
                query_str = 'SELECT * FROM ' + query
            else:
                # Si la consulta comienza con "select", se usa directamente
                # De lo contrario, se considera una consulta válida completa
                query_str = query if query.lower().startswith("select") else query

        # Ejecutar la consulta SQL
        cursor.execute(query_str)
        result = cursor.fetchall()
        # Obtener los nombres de las columnas
        col_names = [column[0].lower() for column in cursor.description]

        # Procesar el resultado según el tipo de salida solicitado
        if output_type == 'dataframe':
            # Crear un DataFrame, incluso si no hay filas (solo columnas)
            output = pd.DataFrame(result, columns=col_names) if result else pd.DataFrame(columns=col_names)
        elif output_type == 'json':
            # Convertir el resultado a un diccionario y luego a formato JSON
            result_dict = [dict(zip(col_names, row)) for row in result]
            output = json.dumps(result_dict, ensure_ascii=False)
        else:
            # Devolver nombres de columnas más las filas como lista de listas
            output = [col_names] + [list(row) for row in result] if result else [col_names]

    except Exception as e:
        # En caso de error, actualizar el mensaje en la respuesta
        response = {"is_ok": False, "error": str(e)}

    finally:
        return output, response

def proceso_sql(engine, query, parameters=None):
    """
    Ejecuta sentencias SQL desde un archivo (.sql) o desde una cadena directa, manejando parámetros y control de errores.
    :param engine: Motor SQLAlchemy para ejecutar las consultas.
    :param query: Ruta al archivo SQL o cadena de consulta SQL directa.
    :param parameters: Diccionario de parámetros para formatear las consultas SQL.
    :param sep: Separador para dividir múltiples consultas (por defecto ';').
    :return: Diccionario con el estado de la ejecución.
    """
    __response = {"is_ok": True, "error": ""}
    
    try:
        if query.lower().endswith(".sql"):

            filename=query
            with open(filename,encoding='utf-8-sig') as file:
                statements = re.split(';', file.read(), flags=re.MULTILINE)
                for statement in statements:
                    try:
                        engine.execute(text(statement))
                    except Exception as __e:
                        print(f"Database error occurred: {str(__e)}")  
                        continue
        else:
            engine.execute(text(query))

    except Exception as __e:
        __response = {"is_ok": False, "error": str(__e)}
    
    return __response


def proceso_sql_string(engine, query_str: str ):
    __response = {"is_ok": True, "error": ""}
    try:
        engine.execute(text(query_str))
    except Exception as __e:
        __response = {"is_ok": False, "error": str(__e)}

    return __response



def test_datalake(cursor, bc_today_date: str):
    try:
        __date_obj = datetime.strptime(bc_today_date, "%d/%m/%Y")
    except ValueError as e:
        raise ValueError(f"Invalid date format for bc_today_date: {bc_today_date}. Expected format: 'dd/mm/YYYY'. Error: {e}")

    __datalake_date = __date_obj.strftime("%Y%m%d")
    
    try:
        __response = funcion_sql(cursor, f'datalake.lkta_credito_vigente_{__datalake_date}@bdbi fetch first 1 rows only' , output_type='dataframe')
    except Exception as e:
        raise RuntimeError(f"Database query execution failed: {str(e)}")

    return __response.get("is_ok", False)
    
def get_last_day_of_month(date):
    __first_day_of_current_month = date.replace(day=1)
    __last_day_of_previous_month = __first_day_of_current_month - timedelta(days=1)
    return __last_day_of_previous_month


def send_mail(subject, body, to_email, cc_email=None, attachment_paths=None, outlookacc=None, password=None, draft=False):
    try:
    # Verifica individualmente si alguno de los parámetros obligatorios es None
        if subject is None:
            raise ValueError("El parámetro 'subject' es obligatorio y no puede ser None.")
        if body is None:
            raise ValueError("El parámetro 'body' es obligatorio y no puede ser None.")
        if to_email is None:
            raise ValueError("El parámetro 'to_email' es obligatorio y no puede ser None.")
        if outlookacc is None:
            raise ValueError("El parámetro 'outlookacc' es obligatorio y no puede ser None.")
        if password is None:
            raise ValueError("El parámetro 'password' es obligatorio y no puede ser None.")
        
        
        __credentials = Credentials(username=outlookacc, password=password)
        __account = Account(primary_smtp_address=outlookacc, credentials=__credentials, autodiscover=True)

        __email = Message(
            account=__account,
            subject=subject,
            body=HTMLBody(body)
        )
        
        if attachment_paths:
            for __attachment_path in attachment_paths:
                if __attachment_path:  # Verifica que no sea None o vacío
                    with open(__attachment_path, 'rb') as __file:
                        __file_name = os.path.basename(__attachment_path)
                        __file_content = __file.read()
                        __file_attachment = FileAttachment(name=__file_name, content=__file_content)
                        __email.attach(__file_attachment)

        __email.to_recipients = to_email if to_email else []
        __email.cc_recipients = cc_email if cc_email else []

        if draft:
            __email.folder = __account.drafts
            __email.save()
            print("Correo guardado en 'bandeja de salida'")
        else:
            __email.send()
            print("Correo enviado correctamente")
    except Exception as e:
        print(f"Error al enviar correo\n", str(e))

def insert_dataframe(engine, df, table_name, schema= "", index=False, if_exists='append'):
    __response = {"is_ok": True, "error": ""}

    try:
        __column_types = {}
        for __col, __dtype in df.dtypes.items():
            if __dtype == 'int64' or __dtype == 'int32':
                __column_types[__col] = types.Integer
            elif __dtype == 'object':
                __max_length = df[__col].str.len().max()
                __column_types[__col] = types.VARCHAR(length=max(__max_length, 100))
            elif __dtype == 'float64':
                __column_types[__col] = types.Float
            elif __dtype == 'datetime64[ns]':
                __column_types[__col] = types.Date
            else:
                __column_types[__col] = types.VARCHAR(length=255)
            if df[__col].isna().all():
                __column_types[__col] = types.VARCHAR(length=255)
                
        if not index:
            df = df.reset_index(drop=True)
        df.to_sql(con = engine, schema= schema, name= table_name.lower(), dtype=__column_types, if_exists=if_exists, index=False, chunksize=1000)
    
    except Exception as __e:
        __response = {"is_ok": False, "error": str(__e)}

    return __response

def funcion_sql(cursor, query, parameters=None, output_type='dataframe'):
    response = {"is_ok": True, "error": ""}
    output = None

    try:
        if query.lower().endswith(".sql"):
            with open(query, 'r', encoding='latin-1') as file:
                query_template = file.read()
            query_str = query_template.format(**parameters)
        else:
            if len(query.split()) == 1:
                query_str = 'SELECT * FROM ' + query
            else:
                query_str = query if query.lower().startswith("select") else query

        cursor.execute(query_str)
        result = cursor.fetchall()
        col_names = [column[0].lower() for column in cursor.description]

        if output_type == 'dataframe':
            output = pd.DataFrame(result, columns=col_names) if result else pd.DataFrame(columns=col_names)
        elif output_type == 'json':
            result_dict = [dict(zip(col_names, row)) for row in result]
            output = json.dumps(result_dict, ensure_ascii=False)
        else:
            output = [col_names] + [list(row) for row in result] if result else [col_names]

    except Exception as e:
        response = {"is_ok": False, "error": str(e)}

    finally:
        return output, response


def proceso_sql(connection, query, parameters=None, sep=';'):
    """
    Ejecuta sentencias SQL incluyendo bloques PL/SQL con terminación '/'

    :param connection: Conexión activa de oracledb
    :param query: String SQL o ruta a archivo .sql
    :param parameters: Parámetros para la consulta (opcional)
    :param sep: Separador para múltiples consultas (default ';')
    :return: Dict con {is_ok, error, executed, failed}
    """
    __response = {"is_ok": True, "error": ""}
    
    def is_plsql_block(text):
        """Determina si el texto es un bloque PL/SQL"""
        text = text.strip().upper()
        return text.startswith(("BEGIN", "DECLARE", "CREATE OR REPLACE")) or text.endswith("/")

    try:
        if query.lower().endswith('.sql'):
            with open(query, encoding='utf-8-sig') as f:
                sql_content = f.read()
        else:
            sql_content = query

        sql_content = re.sub(r'^\s*/\s*$', '', sql_content, flags=re.MULTILINE)
        
        statements = []
        current = ""
        in_block = False
        
        for line in sql_content.split('\n'):
            line = line.strip()
            if not line:
                continue
                
            current += line + "\n"
            
            if not in_block and is_plsql_block(current):
                in_block = True
            
            if in_block and (current.strip().upper().endswith("END;") or 
                           current.strip().upper().endswith("END LOOP;") or
                           current.strip().upper().endswith("END IF;")):
                statements.append(current.strip())
                current = ""
                in_block = False
            elif not in_block and sep in line:
                parts = re.split(fr'{sep}(?=(?:[^\'"]|\'[^\']*\'|"[^"]*")*$)', current)
                for part in parts[:-1]:
                    if part.strip():
                        statements.append(part.strip())
                current = parts[-1] if parts[-1].strip() else ""
        
        if current.strip():
            statements.append(current.strip())

        cursor = connection.cursor()
        
        for stmt in statements:
            try:
                if is_plsql_block(stmt):
                    plsql = stmt.rstrip().rstrip('/')
                    if parameters:
                        plsql_params = {f":{k}": v for k, v in parameters.items()}
                        cursor.callproc("dbms_sql.parse", [cursor, plsql, oracledb.DB_TYPE_VARCHAR])
                        for k, v in plsql_params.items():
                            cursor.callproc("dbms_sql.bind_variable", [cursor, k, v])
                        cursor.callproc("dbms_sql.execute", [cursor])
                    else:
                        cursor.execute(plsql)
                else:
                    if parameters:
                        cursor.execute(stmt, parameters)
                    else:
                        cursor.execute(stmt)
                
            except oracledb.Error as e:
                error_msg = f"Error en: {stmt[:100]}... - {str(e)}"
                __response["error"] += error_msg + "\n"
                __response["is_ok"] = False
                continue

        if __response["is_ok"]:
            connection.commit()

    except Exception as e:
        connection.rollback()
        __response.update({
            "is_ok": False,
            "error": f"Error general: {str(e)}",
        })
    finally:
        if 'cursor' in locals():
            cursor.close()

    return __response