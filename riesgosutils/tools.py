from sqlalchemy import types
from datetime import timedelta
import os
from exchangelib import Credentials, Account, Message, FileAttachment,HTMLBody


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

