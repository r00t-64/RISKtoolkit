class StylerHTML:
    def __init__(self,):
        self.__zonales_format__ = '''
            <style>
            html {
                font-family: "Calibri", sans-serif;
                font-size: 12pt;
            }
            td,  th {
            border: 1px solid #ddd;
            padding: 6px;
            font-family: "Calibri", sans-serif;
            font-size: 12pt;
            }

            th {
            padding-top: 8px;
            padding-bottom: 8px;
            text-align: center;
            background-color: #203864;
            color: white;
            }
            </style>
        '''
        self.__consolidado_format__ = '''
            <style>
            html {
            font-family: "Arial",
            }
            td,  th {
            border: 1px solid #ddd;
            padding: 6px;
            }

            th {
            padding-top: 8px;
            padding-bottom: 8px;
            text-align: center;
            background-color: #203864;
            color: white;
            }
            </style>
        '''
        

    