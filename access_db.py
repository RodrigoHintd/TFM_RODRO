import os
import logging
import pandas as pd
from pathlib import Path
import math



# Cambiar directorio de ejecucion para busqueda de ficheros de datos
#os.chdir(os.path.dirname(os.path.realpath(__file__)))

logging.basicConfig(filename='log.log', level=logging.INFO, format='%(asctime)4s - %(levelname)-2s -> %(message)s')

class ConfiguracionConexion:
    BD_ORACLE = "Oracle"
    BD_MSSQL = "Microsoft"
    BD_DB2 = "DB2"
    BD_MYSQL = "MySQL"
    BD_POSTGRESQL = "PostgreSQL"
    BD_FIREBIRD = "Firebird"

    puertos = {
        BD_ORACLE: 1521,
        BD_MSSQL: 1422,
        BD_DB2: 50001,
        BD_MYSQL: 3306,
        BD_POSTGRESQL: 5432,
        BD_FIREBIRD: 3050
    }

    def __init__(self, configuracion=None, ruta = 'config_acceso.yaml', config_id = 'DWRAC' ):
        """
        Inicializa la configuración de un acceso a una base de datos.
        
        Entorno: admite tres tipos diferentes de bases de datos, Oracle, Microsoft SQL y DB2
        User: Nombre de usuario para conectarnos a la base de datos
        Pwd: contraseña de acceso
        Server: Servidor (IP o nombre) en el que está la base de datos
        Service: Nombre del servicio en Oracle, base de datos en IBM o instancia en MSSQL
        Database: Nombre de la base de datos a la que nos queremos conectar
        Port: puerto del servidor en el que está la base de datos.
        
        """
        self.ruta = ruta
        self.config_id = config_id
        self.conf = {'entorno': None, 
            'user': None, 
            'pwd': None, 
            'server': None, 
            'service': None,
            'port': None,
            'database': None}
        

        if configuracion != None:       
            self.get_configuracion(configuracion=configuracion)

        if self.ruta != None and os.path.exists(self.ruta):
            self.update_from_file()

        if self.conf['port'] is None and self.conf['entorno'] is not None:
            self.conf['port'] = ConfiguracionConexion.puertos[self.conf['entorno']]
    
    def lectura_yaml(self):
        import yaml
        return yaml.full_load(open(self.ruta))
    
    def get_configuraciones_disponibles(self):
        return list(self.lectura_yaml().keys())
    
    def get_configuracion(self, configuracion):
        self.configuracion = configuracion

    def update_from_file(self):
        archivo = self.lectura_yaml()
        if self.config_id is not None and self.config_id in archivo:
            self.conf.update(archivo[self.config_id])
        
    def get_connection(self):
        if self.conf['entorno'] == ConfiguracionConexion.BD_POSTGRESQL:
            import psycopg
            return psycopg.connect(dbname=self.conf['database'], user=self.conf['user'], password=self.conf['pwd'], host=self.conf['server'], port=self.conf['port'])
        if self.conf['entorno'] == ConfiguracionConexion.BD_ORACLE:
            import oracledb
            #print('Obteniendo conexión', f'{self.conf["user"]}/{self.conf["pwd"]}@{self.conf["server"]}:{self.conf["port"]}/{self.conf["service"]}')
            return oracledb.connect(user=self.conf["user"], password=self.conf["pwd"], host=self.conf["server"], port=self.conf["port"], service_name=self.conf["service"])
        if self.conf['entorno'] == ConfiguracionConexion.BD_MSSQL:
            import pymssql
            return pymssql.connect(server=self.conf['server'], user=self.conf['user'], password=self.conf['pwd'], database=self.conf['database'])
        if self.conf['entorno'] == ConfiguracionConexion.BD_DB2:
            import ibm_db_dbi
            return ibm_db_dbi.connect(f'HOSTNAME={self.conf["server"]};PORT={self.port};UID={self.conf["user"]};PWD={self.conf["pwd"]};'+
                                        f'Database={self.conf["service"]};Security=ssl', '', '')         
        if self.conf['entorno'] == ConfiguracionConexion.BD_MYSQL:
            import mysql.connector as mysql
            return mysql.connect(host=self.conf['server'], database=self.conf['database'], user=self.conf['user'], password=self.conf['pwd'])
        
        if self.conf['entorno'] == ConfiguracionConexion.BD_FIREBIRD:
            import fdb
            return fdb.connect(dsn=f"{self.conf['server']}/{self.conf['port']}:{self.conf['database']}", user=self.conf['user'], password=self.conf['pwd'], charset='WIN1252')

lista_datos_especiales = {'Microsoft' : [ 'DATE', 'DATETIME2', 'DATETIME2(6)','FLOAT', 'TINYINT', 'SMALLINT', 'INT', 'BIGINT', 'TINYINT UNSIGNED', 'DOUBLE PRECISION', 'VARCHAR(50)', 'BINARY', 'TIME', 'VARCHAR(max)'],
                            'Oracle' : ['TIMESTAMP(3)', 'NUMBER(10)', 'DATE', 'NUMBER(19)', 'NUMBER(5)', 'NUMBER(3)', 'NUMBER(1)', 'BINARY_DOUBLE', 'VARCHAR2(50)'],
                            'MySQL' : [ 'DATE', 'DATETIME(6)','FLOAT', 'TINYINT', 'TINYINT UNSIGNED', 'SMALLINT', 'INT', 'BIGINT', 'DOUBLE', 'TIME', 'DATETIME(3)', 'TIMESTAMP']}
cadenasFechas = ['VARCHAR2', 'TIMESTAMP(6)', 'VARCHAR', 'CHAR', 'NVARCHAR', 'NCHAR']
longitud_datos_especial = ['None, None']

conversion = {
                "Oracle": {
                        'VARCHAR2': 'bool',
                        'VARCHAR2': 'object',
                        'FLOAT': 'float64',
                        'NUMBER': 'int64',
                        'TIMESTAMP': 'datetime64[ns]',
                        'DATE' : 'datetime64[ns]',
                        'TIMESTAMP': 'datetime64[us]',
                        'INTEGER': 'int64',
                        'DECIMAL': 'float64',
                }
            }

conversion_tipos_base = {
    "Oracle" : {
        'int' : 'INTEGER',
        'float': 'DECIMAL(24,10)',
        'str': 'VARCHAR2',
        'bool': 'VARCHAR2',
        'datetime': 'DATE',
        'datetime64[ns]' : 'DATE',
        'float64' : 'DECIMAL(24,10)',
        'int64' : 'INTEGER',
        'object' : 'VARCHAR2',
        'datetime64[us]' : 'DATE',
    }
}

changeData = {
    'object' : str,
    'int64' : int,
    'float64' : float,
    'datetime' : pd.to_datetime,
    'datetime64[ns]' : pd.to_datetime,
    'datetime64[us]' : pd.to_datetime
}

class AccessDB:
        
    def __init__(self, configuracion):
        self.configuracion = configuracion
        self.entorno = configuracion.conf['entorno']

    def get_connection(self):
        return self.configuracion.get_connection()
    
    def execute(self, query, args='nada'):
        con = self.get_connection()
        cur = con.cursor()
        print(type(args))
        try:
            if self.entorno == ConfiguracionConexion.BD_POSTGRESQL and args != 'nada':
                if type(args) == dict:
                    args = tuple(args.values())
                cur.execute(query, args)
            elif isinstance(args, dict):
                print('dict')
                cur.execute(query, **args)
            elif isinstance(args, list):
                print('list')
                cur.execute(query, *args)
            else:
                print('nada')
                cur.execute(query)
            con.commit()
            cur.close()
            con.close()
        except Exception as e:
            con.rollback()
            con.close()
            raise
            

    def insert_one(self, query, args):
        con = self.get_connection()
        cur = con.cursor()
        if self.entorno == ConfiguracionConexion.BD_POSTGRESQL:
            args = tuple(args.values())
            cur.execute(query, args)
            data = cur.fetchall()
            data = pd.DataFrame(data)
        else:
            cur.execute(query, args)
            data = None
        con.commit()
        cur.close()
        con.close()
        return data

    def get(self, query, args):
        con = self.get_connection()
        cur = con.cursor()
        if self.entorno == ConfiguracionConexion.BD_POSTGRESQL:
            args = tuple(args.values())
        cur.arraysize = 100000
        cur.execute(query, args)
        data = cur.fetchall()
        cur.close()
        con.close()
        return data

    def get_schema_tables(self, schema):
        if self.entorno == ConfiguracionConexion.BD_ORACLE:
            return self.get_dataframe(f"SELECT TABLE_NAME FROM ALL_TAB_COLS WHERE OWNER = '{schema}'")
        else:
            return self.get_dataframe(f"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{schema}'").rename(columns=str.upper)

    def get_schema_columns(self, schema, table=None):
        if self.entorno == ConfiguracionConexion.BD_ORACLE:
            return self.get_dataframe(
                f"""
                    SELECT 
                        A.TABLE_NAME,
                        A.COLUMN_NAME,
                        A.DATA_TYPE,
                        A.DATA_LENGTH,
                        A.DATA_PRECISION,
                        A.DATA_SCALE,
                        CASE WHEN A.NULLABLE = 'Y' THEN 1 ELSE 0 END AS NULLABLE,  
                        CASE WHEN ACC.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END AS PK,
                        NULL AS INCREMENTAL
                    FROM ALL_TAB_COLS A
                    LEFT JOIN SYS.ALL_CONSTRAINTS AC
                        ON A.OWNER = AC.OWNER
                        AND A.TABLE_NAME = AC.TABLE_NAME
                        AND AC.CONSTRAINT_TYPE='P'
                    LEFT JOIN SYS.ALL_CONS_COLUMNS ACC
                        ON AC.OWNER = ACC.OWNER
                        AND AC.TABLE_NAME = ACC.TABLE_NAME
                        AND AC.CONSTRAINT_NAME = ACC.CONSTRAINT_NAME
                        AND A.COLUMN_NAME = ACC.COLUMN_NAME
                    WHERE A.OWNER = '{schema}'
                          {f"AND A.TABLE_NAME = '{table}'" if table else ""}    
                    ORDER BY A.TABLE_NAME,
                        A.COLUMN_NAME
                """
            )
        else:
            return self.get_dataframe(
                f"""
                    SELECT 
                        cols.table_name,
                        cols.column_name,
                        cols.data_type,
                        cols.character_maximum_length AS data_length,
                        cols.numeric_precision AS data_precision,
                        cols.numeric_scale AS data_scale,
                        CASE WHEN cols.is_nullable = 'YES' THEN 1 ELSE 0 END AS nullable,
                        -- CASE WHEN kcu.column_name IS NOT NULL THEN 1 ELSE 0 END AS pk,
                        case when constraint_type = 'PRIMARY KEY' then 1 else 0 end as pk,
                        cols.identity_generation AS incremental
                    FROM 
                        information_schema.columns cols
                        LEFT JOIN information_schema.key_column_usage kcu
                            ON cols.table_schema = kcu.table_schema
                            AND cols.table_name = kcu.table_name
                            AND cols.column_name = kcu.column_name
                        LEFT JOIN information_schema.table_constraints tc
                            ON kcu.constraint_name = tc.constraint_name
                            AND kcu.table_schema = tc.table_schema
                            AND tc.constraint_type = 'PRIMARY KEY'
                    WHERE cols.table_schema = 'public'
                          {f"AND cols.table_name = '{table}'" if table else ""}  
                    ORDER BY 
                        cols.table_name,
                        cols.ordinal_position;
                """
            ).rename(columns=str.upper)
        
    def get_dictionary(self, query):
        con = self.get_connection()
        cur = con.cursor()
        cur.arraysize = 100000
        cur.execute(query)
        data = cur.fetchall()
        names = [x[0] for x in cur.description]
        if len(data) < 1000 and self.entorno == 'Oracle':
            import oracledb
            data = [
                {name: (val.read() if isinstance(val, oracledb.LOB) else val)
                 for name, val in zip(names, row)}
                for row in data
            ] 
        else:
            data = [dict(zip(names, d)) for d in data]
        cur.close()
        con.close()
        return data
    
    def get_dataframe_progresivos(self, query, bloque=10000):
        pd.set_option('display.encoding', 'utf-8')
        con = self.get_connection()
        print(query)
        cur = con.cursor()
        cur.arraysize = 100000
        cur.execute(query)
        while True:
            
            data =  cur.fetchmany(bloque)
            if not data:
                cur.close()
                con.close()
                break
            names = [x[0] for x in cur.description]
            data = [dict(zip(names, d)) for d in data]
            yield pd.DataFrame(data)
        
    def get_dataframe(self, query, progresivo= False, bloque=10000):
        return pd.DataFrame(self.get_dictionary(query)) if not progresivo else self.get_dataframe_progresivos(query, bloque)
    
    def get_data_progresivo(self, query, bloque=10000, debug=True):
        if debug:
            import datetime as dtt
        data = []
        con = self.get_connection()
        cur = con.cursor()
        cur.arraysize = 100000
        if debug: 
            inicio = dtt.datetime.now()
            print(f'{inicio.isoformat()}: iniciando.')
        cur.execute(query)        
        while True:
            new_data = cur.fetchmany(bloque)
            data.extend(new_data[:])
            if not new_data or len(new_data) < bloque:
                break
            if debug: 
                fin = dtt.datetime.now()
                velocidad = len(data) / (fin - inicio).total_seconds()
                print(f'\t{fin.isoformat()}: obtenidos {bloque} registros a {velocidad} registros por segundo, acumulado: {len(data)} registros')
        cols = [x[0] for x in cur.description]
        cur.close()
        con.close()
        del cur
        del con
        return data, cols
    
    def get_dictionary_progresivo(self, query,bloque=10000, debug=True):
        data, cols = self.get_data_progresivo( query, bloque, debug)
        return [dict(zip(cols, d)) for d in data]
    
    def get_dataframe_progresivo(self, query, bloque=10000, debug=True):
        return pd.DataFrame(self.get_dictionary_progresivo(query, bloque, debug))
    
    def get_dataframe_from_stored_query(self, fichero, parameters=None, progresivo = False, bloque=10000):
        query = ''.join(open(fichero).readlines())
        if parameters:
            query = query.format(**parameters)        
        return self.get_dataframe(query, progresivo, bloque)
    
    def get_query_from_folder_files(self, ruta_carpeta, type = ['sql']):
        sql = {}
        for base, dirs, files in os.walk(ruta_carpeta):
            for file in files:
                if type == None or file.split('.')[-1] in type:
                    query = ''.join(open(Path(base, file), encoding="utf8").readlines())
                    data = query.split(';')
                    sql[file] = {f'query_{i}' : data[i] for i in range(len(data))}
        return sql
    
    def execute_many(self, query, data, table = None):
        con = self.get_connection()
        cur = con.cursor()
        try:
            if self.entorno == ConfiguracionConexion.BD_ORACLE:
                cur.prepare(query)
                cur.arraysize = 100000
                query = None
            if self.entorno == ConfiguracionConexion.BD_MSSQL:
                con.bulk_copy(table, data, check_constraints=False)
            else:
                cur.executemany(query, data)
        except Exception as e:
            con.rollback()
            cur.close()
            con.close()
            raise

        con.commit()
        cur.close()
        con.close()

    def generate_upload_command(self, destination, columns):
        if self.entorno == ConfiguracionConexion.BD_ORACLE:
            return "INSERT INTO {0} ({1}) VALUES ({2})".format(
                        destination, # Tabla destino
                        ','.join(columns), 
                        ','.join([':' + x  for x in columns]))
        elif self.entorno == ConfiguracionConexion.BD_MSSQL:
            return "INSERT INTO {0} ({1}) VALUES ({2})".format(
                    destination,
                    ','.join('"'+columns+'"'),
                    ','.join(['%s' for _ in columns])
                )
        elif self.entorno == ConfiguracionConexion.BD_MYSQL:
            return "INSERT INTO {0} ({1}) VALUES ({2})".format(
                    destination,
                    ','.join('`'+columns+'`'),
                    ','.join(['%s' for _ in columns])
                )
        elif self.entorno == ConfiguracionConexion.BD_POSTGRESQL:
            return "INSERT INTO {0} ({1}) VALUES ({2})".format(
                        destination, # Tabla destino
                        ','.join(columns), 
                        ','.join(['%(' + x + ')s' for x in columns]))
        
    def upload_data(self, data, columns, destino):        
        comando = self.generate_upload_command(destino, columns)
        if self.entorno in (ConfiguracionConexion.BD_ORACLE, ConfiguracionConexion.BD_POSTGRESQL):
            self.execute_many(comando, data)
        elif self.entorno == ConfiguracionConexion.BD_MSSQL:
            tuples = [tuple(x.values()) for x in data] if isinstance(data[0], dict) else data
            self.execute_many(comando, tuples, table=destino)
        elif self.entorno == ConfiguracionConexion.BD_MYSQL:
            lists = [list(x.values()) for x in data] if isinstance(data[0], dict) else data
            self.execute_many(comando, lists)
    
    def stubborn_upload_data(self, dataframe, destino, max_intentos=10):
        import re
        tries = 0
        while tries < max_intentos:
            tries += 1
            try:
                self.upload_data_frame(dataframe, destino)
                break
            except Exception as e:
                print('Error intentando subir datos ', str(e))
                try:
                    if "ORA-00904" in str(e):
                        print(f"Error subiendo datos a la tabla {destino}: {e}")
                        print("Intentando crear columna faltante y reintentar...")
                        columna = re.match(r".*ORA-00904: \"(.*?)\":.*", str(e)).group(1)
                        print(f"Columna problemática: {columna}")
                        self.execute(f"ALTER TABLE {destino} ADD ({columna} VARCHAR2(300 CHAR))")
                        print(f"columna creada")
                    elif 'ORA-12899' in str(e):
                        print(f"Error de longitud de datos al subir a la tabla {destino}: {e}")
                        columna =  re.match(r".*\"(.*?)\".*", str(e)).group(1)
                        print(f"Columna problemática: {columna}")
                        new_length = max([len(str(val[columna])) for val in dataframe.to_dict(orient='records') if val[columna] is not None])
                        new_length = max([100, new_length + 50])
                        print(f"ALTER TABLE {destino} MODIFY ({columna} VARCHAR2({new_length * 2} CHAR))")
                        self.execute(f"ALTER TABLE {destino} MODIFY ({columna} VARCHAR2({new_length * 2} CHAR))")
                        print(f"Columna {columna} modificada para aumentar la longitud")
                except Exception as e2:
                    print(str(e2))
                    pass
        if tries == max_intentos:
            raise Exception(f"No se ha podido subir los datos a la tabla {destino} tras {max_intentos} intentos.")
            

    def upload_data_frame(self, dataframe, destino, autoajustar=False):
        import pandas as pd
        pd.set_option('display.encoding', 'utf-8')
        df_ = dataframe.copy()
        for x in df_.columns:
            df_[x] = df_[x].astype(object)
        df_ = df_.where(pd.notnull(df_), None)
        data = df_.to_dict(orient='records')
        self.upload_data(data, dataframe.columns, destino)

    def upload_dictionaries(self, dictionaries, destino, autoajustar=False):
        if not dictionaries:
            return
        columns = dictionaries[0].keys()
        self.upload_data(dictionaries, columns, destino)

    def delete_table(self, tabla_destino, condicion_where = None):
        if condicion_where != None:
            print(f'DELETE FROM {tabla_destino} WHERE {condicion_where}')
            self.execute(f'DELETE FROM {tabla_destino} WHERE {condicion_where}')
        else:
            self.execute(f'TRUNCATE TABLE {tabla_destino}')

    def exist_table(self, table_name):
        schema = True if len(table_name.split('.')) > 1 else False

        if self.entorno == ConfiguracionConexion.BD_ORACLE:
            nombre_tablas = self.get_dataframe(f"SELECT CONCAT(CONCAT(OWNER, '.'), TABLE_NAME) AS TABLE_NAME FROM dba_tables WHERE OWNER = '{table_name.split('.')[0]}'" if schema else f'SELECT TABLE_NAME FROM dba_tables')
        elif self.entorno == ConfiguracionConexion.BD_MSSQL or self.entorno == ConfiguracionConexion.BD_MYSQL or self.entorno == ConfiguracionConexion.BD_POSTGRESQL:
            nombre_tablas = self.get_dataframe(f"SELECT CONCAT(TABLE_SCHEMA, '.', TABLE_NAME) AS TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{table_name.split('.')[0]}'" if schema else f'SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES')
        
        nombre_tablas.columns = nombre_tablas.columns.str.upper()
        
        if len(nombre_tablas) == 0:
            return False
        
        nombres_tablas = [elemento.upper() for elemento in nombre_tablas['TABLE_NAME']]
        
        return True if table_name.upper() in nombres_tablas else False
    
    def oracle_correction2mssql(self, new_data_type, longitud_dato):
        if new_data_type == 'DECIMAL' and longitud_dato in longitud_datos_especial:
            new_data_type = 'FLOAT'
        elif new_data_type == 'DECIMAL' and longitud_dato.split(', ')[1] == '0':
            if int(longitud_dato.split(',')[0]) < 3:
                new_data_type = 'TINYINT'
            elif 3 <= int(longitud_dato.split(',')[0]) < 5:
                new_data_type = 'SMALLINT'
            elif 5 <= int(longitud_dato.split(',')[0]) < 9:
                new_data_type = 'INT'
            elif 9 <= int(longitud_dato.split(',')[0]) < 19:
                new_data_type = 'BIGINT'
            else:
                longitud_dato = longitud_dato.split(',')[0]
        elif new_data_type == 'VARCHAR' and longitud_dato in longitud_datos_especial:
            longitud_dato = 'max'
        return new_data_type, longitud_dato

    def mssql_correction2oracle(self, new_data_type, longitud_dato):
        if new_data_type == 'NUMBER' and '()' not in new_data_type and longitud_dato in longitud_datos_especial:
            longitud_dato = '()'
        return new_data_type, longitud_dato

    def get_table_data_types(self, table):
        import numpy as np

        if self.entorno == ConfiguracionConexion.BD_ORACLE:
            query = f"""
            SELECT COL.COLUMN_NAME, 
                    COL.DATA_TYPE, 
                    COL.DATA_LENGTH, 
                    COL.DATA_PRECISION, 
                    COL.DATA_SCALE 
            FROM SYS.ALL_TAB_COLUMNS COL
                INNER JOIN SYS.ALL_TABLES T
                    ON COL.OWNER = T.OWNER 
                    AND COL.TABLE_NAME = T.TABLE_NAME
            WHERE (COL.OWNER || '.' || COL.TABLE_NAME) = '{table}'
            """
        elif self.entorno == ConfiguracionConexion.BD_POSTGRESQL:
            query = f"""
            SELECT 
                cols.table_schema || '.' || cols.table_name AS full_table_name,
                cols.column_name,
                cols.data_type,
                cols.character_maximum_length AS data_length,
                cols.numeric_precision AS data_precision,
                cols.numeric_scale AS data_scale
            FROM 
                information_schema.columns AS cols
                INNER JOIN  information_schema.tables AS tbl
                    ON cols.table_schema = tbl.table_schema
                       AND cols.table_name = tbl.table_name
            WHERE 
                cols.table_schema || '.' || cols.table_name = '{table}'
                AND tbl.table_type = 'BASE TABLE'
            """

            df = self.get_dataframe(query)
            df.columns = df.columns.str.upper()
            df['COLUMN_TYPE'] = df['DATA_TYPE'].apply(lambda x: x if x in ['DATE', 'TIMESTAMP(3)', 'TIMESTAMP(6)'] else np.nan)
            df['DATA_LENGTH'] = df.apply(lambda row: np.nan if row['DATA_TYPE'] in ['NUMBER', 'FLOAT'] else row['DATA_LENGTH'], axis = 1)
            df['COLUMN_TYPE'] = df['COLUMN_TYPE'].fillna(df.apply(lambda row: f"{row['DATA_TYPE']}({','.join([str(int(x)) for x in [row['DATA_LENGTH'], row['DATA_PRECISION'], row['DATA_SCALE']] if str(x) not in ['nan', 'None']])})", axis = 1))
            return dict(zip(df['COLUMN_NAME'], df['COLUMN_TYPE']))
        return

    def get_datatypes(self, df_info, conexion2):
        import json
        with open('conversion.json', 'r') as file:
            conversion = json.load(file)    

        correction = {'Oracle' : self.oracle_correction2mssql,
                    'Microsoft' : self.mssql_correction2oracle,
                    'MySQL' : self.mssql_correction2oracle}

        diccionario = {}
        for fila in df_info:
            nombre_columna = fila['COLUMN_NAME'] 
            tipo_dato = fila['DATA_TYPE'].upper() 
            null = fila['NULLABLE'].upper()        
            new_data_type = conversion[self.entorno][tipo_dato][conexion2.entorno] if self.entorno != conexion2.entorno else tipo_dato
            longitud_dato = str(fila['DATA_LENGTH']) if tipo_dato in cadenasFechas else str(fila['DATA_PRECISION'])+', '+str(fila['DATA_SCALE'])
            new_data_type, longitud_dato = correction[self.entorno](new_data_type, longitud_dato)#if self.conf['entorno'] != conexion2.conf['entorno'] else new_data_type, longitud_dato
            data = new_data_type + '(' + longitud_dato+')' if new_data_type not in lista_datos_especiales[conexion2.entorno] and '()' not in longitud_dato else new_data_type
            diccionario[nombre_columna] = data #+ ' NOT NULL' if null in ['NO', 'N'] else data
        return diccionario, df_info
    
    def crear_tabla_desde_dict(self, nombre_tabla, tipos):
        if self.entorno != ConfiguracionConexion.BD_MYSQL:
            self.execute(f"""CREATE TABLE {nombre_tabla} ({f', '.join(f'"{k}" {v}'for k, v in tipos.items())})""") 
        else:
            self.execute(f"""CREATE TABLE {nombre_tabla} ({f', '.join(f'`{k}` {v}'for k, v in tipos.items())})""")
    
    def aux(self, df, nombre_columna):
        for el in df[nombre_columna]:
            if not pd.isna(el) and str(el.time()) != '00:00:00':
                tipoData = 'TIMESTAMP'
                break
            else:
                tipoData = 'DATE'
        return tipoData

    def crear_tabla_desde_df(self, df, nombre_tabla, crear=True):
        conv = conversion_tipos_base['Oracle']

        tipos = df.dtypes.reset_index()
        tipos.columns = ['column_name', 'data_type']

        tipos['data_type'] = tipos['data_type'].apply(lambda x: conv[str(x)])

        new_types =  []
        for idx, row in tipos.iterrows():
            if row['data_type'] == 'VARCHAR2':
                newlen = max(100, df[row['column_name']].astype(str).map(len).max()*2)
                new_types.append(f'VARCHAR2({newlen} CHAR)')
            else:
                new_types.append(row['data_type'])

        tipos['data_types'] = new_types

        dict_tipos = dict(zip(tipos['column_name'], tipos['data_types']))

        if crear:
            self.crear_tabla_desde_dict(nombre_tabla, dict_tipos)

        return dict_tipos
    
    def preparar_df(self, df, mapeo, formato_fecha='ISO8601'):
        import pandas as pd
        missed_columns = []
        for k, v in mapeo.items():
            if k not in df.columns:
                print(f'La columna {k} no existe en el DataFrame')
                missed_columns.append(k)
                continue
            
            # Se comprueba si el tipo de dato es correcto
            # En caso de que sea correcto y sea VARCHAR se entra en el if para convertir nulos a ""
            if str(df.dtypes[k]) != conversion[self.entorno][v] or 'VARCHAR' in v:
                if v == 'DATE' or v == 'TIMESTAMP':
                    try:
                        df[k] = changeData[conversion[self.entorno][v]](df[k], format=formato_fecha, errors='coerce')
                    except:
                        from datetime import datetime
                        df[k] = df[k].apply(lambda fecha_str: datetime.strptime(fecha_str[:-6], "%Y-%m-%dT%H:%M:%S"))
                elif v == 'NUMBER' or v == 'FLOAT':
                    # FALLO AL CONVERTIR float64 to int -> NUMBER(p,s)
                    # Todos los valores numericos en float no da problema la subida
                    try:
                        df[k] = df[k].astype(float)
                    except:
                        import numpy as np
                        try:
                            df[k] = df[k].fillna(np.nan).replace('', np.nan) 
                            df[k] = df[k].astype(float)
                            logging.error(f'La columna {k} se ha convertido a tipo float porque solo contiene valores nulos')
                        except Exception as e:
                            logging.error(f'La columna {k} no se puede convertir a tipo float')
                            print(e)
                else:
                    df[k] = df[k].fillna("")
                    df[k] = df[k].astype(changeData[conversion[self.entorno][v]])
        for col in missed_columns:
            df = pd.concat([df, pd.DataFrame({col: []})])
        return df
    
    def get_dic_DataTypeFromDB(self, tabla):
        ######################################################################
        ## SOLO FUNCIONA SI HAY PERMISOS QUE PERMITAN ENTRAR EN ESAS TABLAS ##
        ######################################################################
        if self.entorno == ConfiguracionConexion.BD_ORACLE:
            return self.get_dictionary(f"SELECT COLUMN_NAME,\
                                            DATA_TYPE, DATA_LENGTH,\
                                            DATA_PRECISION, DATA_SCALE,\
                                            NULLABLE \
                                            FROM all_tab_columns\
                                            WHERE OWNER IN ('{tabla.split('.')[0]}') AND TABLE_NAME = '{tabla.split('.')[1]}'")
        else:
            return self.get_dictionary(f"SELECT COLUMN_NAME, DATA_TYPE, \
                    CHARACTER_MAXIMUM_LENGTH AS DATA_LENGTH, NUMERIC_PRECISION AS DATA_PRECISION,\
                    NUMERIC_SCALE AS DATA_SCALE, DATETIME_PRECISION,\
                    IS_NULLABLE AS NULLABLE\
                    FROM INFORMATION_SCHEMA.COLUMNS\
                    WHERE TABLE_NAME = '{tabla if len(tabla.split('.')) == 1 else tabla.split('.')[1]}'")

    def copiar_tabla(self, tabla_origen, conn2, tabla_destino, condicion_where=None, currentDay=False, truncateTable=True, select_query = None, escrituraPickle=False, ruta_pickle='./'):
        import numpy as np
        import pandas as pd
        import datetime as dttm
        from unidecode import unidecode

        bloque_progresivo = 100000

        df_info = self.get_dic_DataTypeFromDB(tabla_origen)
        data_types, df_info = self.get_datatypes(df_info, conn2)
        if currentDay:
            data_types['AUX_UPDATED_DATA'] = 'DATETIME2(6)' if conn2.entorno == ConfiguracionConexion.BD_MSSQL else 'TIMESTAMP(6)'
        
        logging.info('------') 
        exist = conn2.exist_table(tabla_destino) 
        if not exist: 
            try:
                if select_query != None:
                    types = {}
                    for k, v in data_types.items():
                        if k in select_query.split(', '):
                            types[k] =  v
                    data_types = types

                conn2.crear_tabla_desde_dict(tabla_destino, data_types)
                logging.info(f'TABLA CREADA: {tabla_destino}')
            except Exception as err: 
                logging.error(f"CREATE TABLE "+tabla_destino+' ('+', '.join(f'"{k}" {data_types[k]}' for k in data_types.keys())+')')
                logging.error(f'FALLO: {err}')
                return err
        else:
            logging.info(f'La tabla {tabla_destino} ya estaba creada en la base de datos.')

        
        if (condicion_where != None and condicion_where['bd_destino'] != None) or truncateTable == True:
            conn2.delete_table(tabla_destino, condicion_where=condicion_where['bd_destino'] if condicion_where != None else None)
        else:
            print('NO SE ELIMINAN DATOS DE LA TABLA DESTINO')
           
        print('La tabla de destino es: ', tabla_destino)
        if select_query == None:
            columnas_destino = conn2.get_dataframe(f"""SELECT COLUMN_NAME 
                                                        FROM INFORMATION_SCHEMA.COLUMNS 
                                                        WHERE TABLE_NAME = '{tabla_destino if len(tabla_destino.split('.')) == 1 else tabla_destino.split('.')[1]}'""" 
                                                    if conn2.entorno != ConfiguracionConexion.BD_ORACLE 
                                                    else f"""SELECT COLUMN_NAME
                                                            FROM all_tab_columns 
                                                            WHERE OWNER = '{tabla_destino.split('.')[0]}' 
                                                                AND TABLE_NAME = '{tabla_destino.split('.')[1]}'""")
            query = ', '.join(['"{}"'.format(item) if self.entorno != ConfiguracionConexion.BD_MYSQL 
                                                    else '`{}`'.format(item) for item in columnas_destino['COLUMN_NAME'] if item != 'AUX_UPDATED_DATA'])
        else:
            query = select_query
            
        final_query = f'SELECT {query} FROM {tabla_origen}'
        if condicion_where != None and condicion_where['bd_origen'] != None:
            final_query += f" WHERE {condicion_where['bd_origen']}"
        
        for i, df in enumerate(self.get_dataframe(final_query, True, bloque_progresivo)):

            if currentDay:
                df['AUX_UPDATED_DATA'] = dttm.datetime.today()       

            df = df.fillna(np.nan) 

            for elemento in df.columns:
                df[elemento] = df[elemento].astype(str)
                df[elemento] = df[elemento].apply(unidecode)

            for k, v in data_types.items():
                if v == 'TIME':
                    new_data = []
                    for elemento in df[k]:
                        if not pd.isnull(elemento):
                            new_data.append(elemento.replace('0 days ', ''))
                        else: 
                            new_data.append(elemento)
                    df[k] = new_data

            df.replace('nan', np.nan, inplace=True)
            df.replace('NaT', np.nan, inplace=True)        
            
            if conn2.entorno == ConfiguracionConexion.BD_ORACLE:
                for k, v in data_types.items():
                    if 'INT' in v and df.dtypes[k] not in ['int64', 'float64']:
                        df[k] = df[k].astype(int)
                    elif 'FLOAT' in v or 'DECIMAL' in v or 'NUMBER' in v and df.dtypes[k] not in ['int64', 'float64']:
                        df[k] = df[k].astype(float)
                    elif 'DATE' in v or 'TIMESTAMP' in v or 'DATETIME2' in v:
                        df[k] = pd.to_datetime(df[k])
            
            if escrituraPickle:
                df.to_pickle(f"{ruta_pickle + tabla_destino}_{str(i)}.pkl") 

            inicio_subida = dttm.datetime.now()
            conn2.upload_data_frame(df, tabla_destino)
            tiempo_total = dttm.datetime.now() - inicio_subida
            logging.info(f'El tiempo de subida es de: {tiempo_total}')

            logging.info(f'Filas totales copiadas en la tabla {tabla_destino}: {(i * bloque_progresivo) + len(df)}')

    def fin_ejecucion(self, subproceso, estado_fin='OK'):
        from datetime import datetime
        print(f"Buscando datos de proceso en BD para {subproceso}")
        subproceso_ejecucion = self.get(f"""
            SELECT * FROM HINTD_OPS.CTRL_PROCESO_EJECUCION EP
            WHERE SUBPROCESO = '{subproceso}'
            AND FECHA = 
                (SELECT MAX(FECHA) FROM HINTD_OPS.CTRL_PROCESO_EJECUCION WHERE SUBPROCESO = '{subproceso}')
            AND FECHA !=
                (SELECT COALESCE(MAX(FECHA), CURRENT_TIMESTAMP - 100) FROM HINTD_OPS.CTRL_PROCESO_EJECUCION WHERE SUBPROCESO = :1 AND ESTADO LIKE 'FIN%')
            """, [subproceso]
        )
        print("Subproceso ejecucion: " + str(subproceso_ejecucion))
        if len(subproceso_ejecucion)>0:
            dominio = subproceso_ejecucion[0][1]
            proceso = subproceso_ejecucion[0][2]
            ejecucion = subproceso_ejecucion[0][3]
            tarea = subproceso_ejecucion[0][4]
            print(f"Fin de ejecucion de dominio {dominio}, proceso {proceso}, ejecucion {ejecucion}, tarea {tarea}.")
            self.execute("""
                INSERT INTO HINTD_OPS.CTRL_PROCESO_EJECUCION (FECHA, DOMINIO,PROCESO,SUBPROCESO,EJECUCION,ESTADO)
                VALUES (:5, :0, :1, :2, :3, :4)
            """,
            datetime.now(), dominio, proceso, tarea, ejecucion, f'FIN-{estado_fin}'
            )
            #self.call_stored_procedure('HINTD_OPS.FIN_TAREA',(dominio,proceso,tarea,estado_fin))

    def call_stored_procedure(self,procedure, params):
        con = self.get_connection()
        cur = con.cursor()
        try:
            if self.entorno == ConfiguracionConexion.BD_POSTGRESQL:
                params_tuple = tuple(params)
                placeholders = ','.join(['%s'] * len(params_tuple))
                cur.execute(f"SELECT * from {procedure}({placeholders})", params_tuple)
            else:
                cur.callproc(procedure, params)
        except Exception as e:
            con.rollback()
            con.close()
            raise e
        
        if self.entorno == ConfiguracionConexion.BD_POSTGRESQL:
            try:
                result = cur.fetchone()[0]
            except:
                result = None
        else:
            result = None
            
        con.commit()
        cur.close()
        con.close()
        
        return result


def get(id):
    con = ConfiguracionConexion(config_id=id)
    return AccessDB(con)



def iterate_base_sequence(initial_value, final_value, delta, return_next_value):
    current_value = initial_value
    while current_value <= final_value:
        next_value = current_value + delta
        if return_next_value:
            yield (current_value, next_value)
        else:
            yield current_value
        current_value = next_value

def get_sequence(
        initial_value, 
        final_value=None,
        step_type=None,
        step_number=None, 
        step_size=1,
        return_next_value=False,
        format=None,
        offset_range=None):

    from datetime import datetime
    from collections.abc import Iterable
    
    if isinstance(initial_value, datetime):
        from dateutil.relativedelta import relativedelta
        delta = relativedelta()

        if step_type in delta.__dict__:
            delta = relativedelta(**{step_type: step_size})

        if offset_range:
            final_value = initial_value + relativedelta(**{step_type: offset_range[1] * step_size})
            initial_value += relativedelta(**{step_type: offset_range[0] * step_size})
            print(initial_value, final_value)

        if step_type == 'total':
            if return_next_value:
                yield (initial_value.strftime(format), final_value.strftime(format))
            else:
                yield initial_value.strftime(format)
            return

    elif isinstance(initial_value, (int, float)):
        delta = step_size

    if not final_value and step_number:
        final_value = initial_value + (delta * step_number)

    def apply_format(value):
        if not format:
            return value

        if isinstance(value, Iterable) and not isinstance(value, str):
            return [apply_format(x) for x in value]
        elif isinstance(value, datetime):
            return value.strftime(format)
        elif isinstance(value, str):
            return value.format(format)

        return value

    yield from map(apply_format, iterate_base_sequence(
        initial_value=initial_value,
        final_value=final_value,
        delta=delta,
        return_next_value=return_next_value
    ))

def read_some(file, some=100000):
    import csv
    f = open(file, mode='r', encoding='utf-8')
    reader = csv.DictReader(f, delimiter=',')

    while True:
        rows = []
        try:
            for _ in range(some):
                rows.append(next(reader))
            yield rows
        except StopIteration:
            if rows:
                yield rows
            break
    f.close()

"""
EJEMPLO DE USO SE READ_SOME:

for x in read_some(file_path, some=1000000):
    filas = []
    print('got chunk of size:', len(x))
    for row in x:
        row['ID_DATE'] = datetime.strptime(row['ID_DATE'], '%Y-%m-%d')
        row['AUX_TS_FICHERO'] = mod_time
        row['AUX_ORIGEN'] = file_path
        row['COD_SIZE'] = row['SIZE']
        del row['SIZE']
        filas.append(row)
    db.upload_dictionaries(filas, 'DWZAH.SRC_STOCK_TIENDA')

""" 

def escribir_log(msg, dominio, file_name, subprocess, ejecucion_id, url=None, file_log=False, tipo='error'):
    """Escribe un log en el sistema de logs centralizado y en un fichero local.
    Parámetros:
    - msg: Mensaje a escribir en el log.
    - dominio: Dominio del proceso.
    - file_name: Nombre del archivo asociado al log.
    - subprocess: Nombre del subproceso.
    - ejecucion_id: ID de la ejecución del proceso.
    - url: URL del sistema de logs centralizado (opcional).
    - file_log: Indica si se debe escribir en un fichero local (opcional).
    - tipo: Tipo de mensaje ('error', 'info', 'warn').
    """
    
    import requests
    from datetime import datetime

    if url is not None:
        try:
            requests.get(f'{url}/ctrl_log', params={'dominio': dominio, 'archivo': file_name, 'subproceso': subprocess, 'ejecucion_id': ejecucion_id, 'severidad' : 'ERROR' if tipo=='error' else 'INFO' if tipo=='info' else 'WARN', 'mensaje': f'{msg}'})
        except:
            pass
    
    if file_log:
        with open('./error_integrador.log', '+a') as file:
            if tipo == 'error':
                file.writelines(f'\n{datetime.now()} ----> Ha ocurrido un error: {msg}')
            elif msg =='' or tipo=='salto_linea':
                file.writelines(f'{msg}')
            else:
                file.writelines(f'\n{datetime.now()} ----> {msg}')

    print(f"[{tipo.upper()} - {datetime.now()}] -> {msg}")