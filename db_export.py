# -*- coding: utf-8 -*-
"""
Created on Sat Feb  9 13:12:53 2019

@author: Solis
El módulo permite:
1.	Crea una bd sqlite a partir de la estructura de una db ms Access; a partir
    de esta db se hace la migración a postgres
2.	Crea un fichero sql para recrear la estructura de la db access en postgres
3.	Exporta los datos de la db access a ficheros csv (un fichero por tabla)
4.	Exporta los datos de la db Access a postgres mediante una función upsert.
    Esta opción permite identificar de una vez problemas en la migración de
    los datos entre las 2 db.

NOTA IMPORTANTE
Para leer las foreign keys debes, en ms access:
1 Hacer visibles las tablas del sistema: opciones -> base de datos actual ->
    navegación, opciones de navegación, opciones de presentación, marcar
    mostrar objetos del sistema
2 Permitir la lectura de la tabla MSysRelationShips: opciones -> archivo ->
    información -> administrar usuarios y permisos ->
    permisos y usuarios de grupo, como administrador, tipo de objeto tabla,
    nombre del objeto MysRelationShips, marcar leer diseño y leer datos

PARA EL PROGRAMADOR
Los nombres de las tablas y las columnas de access se pasan a nombres válidos
    de postgres utilizando la función to_ascii

LIMITACION DEL MODULO EN LA CARGA DE DATOS CON LA FUNCION py_upsert
Los valores de las claves primarias y de las columnas en las claves foráneas
    se convierten en minúsculas. En la actualidad esto solo funciona bien si
    las claves foráneas implican a una sola columna
"""
import pyodbc
import sqlite3
import littleLogging as logging

SQL_FILE_HEADERS = ('SET CLIENT_ENCODING TO UTF8;',
                    'SET STANDARD_CONFORMING_STRINGS TO ON;',
                    'BEGIN;')

# molde para fichero FILE_COPYFROM metacomando de psql
copyfrom = ("\copy {} ({}) ",
            "from '{}' ",
            "with (format csv, header, delimiter ',', encoding 'LATIN1',",
            " force_null ({}))")

class Migrate():
    """
    Lee la estructura de una db access para escribir las sentencias sql
        que permiten migrarla a otro rdbs
    """
    SQL_FILE_HEADERS = ('SET CLIENT_ENCODING TO UTF8;',
                        'SET STANDARD_CONFORMING_STRINGS TO ON;')


    def __init__(self, dbaccess: str, dir_out: str):
        from os.path import isdir, isfile, join, split, splitext

        if not isfile(dbaccess):
            raise ValueError(f'No existe {dbaccess}')
        if not isdir(dir_out):
            raise ValueError(f'No existe {dir_out}')

        self.constr_access = \
        r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};' +\
        f' DBQ={dbaccess};'

        head, tail = split(dbaccess)
        name, ext = splitext(tail)
        self.constr_sqlite = join(dir_out, f'{name}_struct.db')
        self.con_a = None
        self.con_s = None
        self.dir_out = dir_out
        self.base_name = f'{name}'


    def structure_to_sqlite(self):
        """
        Lee la estructura de la db access y crea un fichero sqlite con la
            información de la estructura (no los datos)
        """
        from os import remove
        try:
            remove(self.constr_sqlite)
        except OSError:
            logging.append('No se ha podido borrar {self.constr_sqlite}')

        try:
            self.__open_connections()
            self.__create_tables()
            self.__populate_tables()
            print('Se ha creado la db sqlite; para trasladar los cambios ' +\
                  'a postgres debes ejecutar el fichero sql para crear las' +\
                  'nuevas tablas')
        except:
            from traceback import format_exc
            msg = format_exc()
            logging.append(msg)
        finally:
            self.__close_connections()


    def __open_connections(self):
        if self.con_a is None:
            self.con_a = pyodbc.connect(self.constr_access)
        if self.con_s is None:
            self.con_s = sqlite3.connect(self.constr_sqlite)


    def __close_connections(self):
        if self.con_a is not None:
            self.con_a.close()
            self.con_a = None
        if self.con_s is not None:
            self.con_s.close()
            self.con_s = None


    def __create_tables(self):
        """
        crea una db sqlite para albergar la estructura de self.db_a
        args
        con_s, conexión creada to self.db
        """
        from traceback import format_exc

        create_table1 = \
        """
        create table if not exists tables (
        name TEXT,
        table_type TEXT,
        primary_key TEXT,
        PRIMARY KEY (name))"""

        create_table2 = \
        """
        create table if not exists columns (
        table_name TEXT,
        col_name TEXT,
        type_i INTEGER,
        type_name TEXT,
        column_size INTEGER,
        pg_type_name TEXT,
        col_number INTEGER,
        PRIMARY KEY (table_name, col_name),
        FOREIGN KEY(table_name) REFERENCES tables(name))
        """

        create_table3 = \
        """
        create table if not exists relationships (
        references_table TEXT,
        references_cols TEXT,
        referenced_table TEXT,
        referenced_cols TEXT,
        PRIMARY KEY (references_table, references_cols),
        FOREIGN KEY(references_table) REFERENCES tables(name),
        FOREIGN KEY(referenced_table) REFERENCES tables(name))
        """

        sqls = (create_table1, create_table2, create_table3)
        cur = self.con_s.cursor()

        for sql in sqls:
            try:
                cur.execute(sql)
                self.con_s.commit()
            except:
                msg = format_exc()
                logging.append(f'Error al ejecutar\n{sql}\n{msg}')


    def __populate_tables(self):
        """
        lee los datos de estructura de self.db_a a través de su conexión
            con_a y los escribe en self.db_s mediante su conexión abierta
            con_s
        """
        cur = self.con_a.cursor()
        tables = [(row.table_name, row.table_type,
                  self.__primary_key_get(row.table_name, row.table_type))
                  for row in cur.tables()
                  if row.table_type in ('TABLE', 'SYSTEM TABLE')]
        self.__insert_into_tables(tables)
        self.__insert_into_columns(tables)
        self.__insert_into_relationships()


    def __primary_key_get(self, table_name, table_type):
        cur = self.con_a.cursor()
        if table_type == 'TABLE':
            pk_cols = [row[8] for row in cur.statistics(table_name)
                       if row[5] is not None and
                       row[5].upper() == 'PRIMARYKEY']
            return ', '.join(pk_cols)
        else:
            return ''


    def __insert_into_tables(self, tables):
        """
        inserta los nombres de las tablas y su tipo en la tabla tables
        """
        insert = \
        """
        insert into tables (name, table_type, primary_key) values (?, ?, ?)
        """

        update = \
        """update tables set table_type=?, primary_key=? where name=?"""

        cur = self.con_s.cursor()
        for table in tables:
            try:
                cur.execute(insert, table)
            except:
                cur.execute(update, (table[1], table[2], table[0]))
        self.con_s.commit()


    def __insert_into_columns(self, tables):
        """
        inserta las columnas de las tablas en la tabla columns
        """
        insert = \
        """
        insert into columns
        (table_name, col_name, type_i, type_name, column_size, pg_type_name,
        col_number)
        values (?, ?, ?, ?, ?, ?, ?);
        """
        update = \
        """
        update columns set type_i=?, type_name=?, column_size=?,
            pg_type_name=?, col_number=?
        where table_name=? and col_name=?;
        """
        from traceback import format_exc

        d = Migrate.__access_pg_types()
        cur = self.con_a.cursor()
        cur_s = self.con_s.cursor()
        try:
            for table in tables:
                for i, row in enumerate(cur.columns(table[0])):
                    if row.type_name in d:
                        pg_col_type = d[row.type_name]
                    else:
                        pg_col_type = ''
                    try:
                        cur_s.execute(insert, (table[0], row.column_name,
                                               row.sql_data_type, row.type_name,
                                               row.column_size, pg_col_type,
                                               i))
                    except:
                        cur_s.execute(update, (row.sql_data_type, row.type_name,
                                               row.column_size, pg_col_type, i,
                                               table[0], row.column_name))
            self.con_s.commit()
        except:
            msg = format_exc()
            msg1 = ';'.join(table)
            raise ValueError(f'\nTabla: {msg1}\n{msg}')


    def __insert_into_relationships(self):
        """
        inserta las foreign keys en relationships
        """
        select = \
        """
        SELECT szObject
        FROM MSysRelationships
        GROUP BY szObject
        ORDER BY szObject;
        """

        insert = \
        """
        insert into relationships (references_table, references_cols,
            referenced_table, referenced_cols)
        values (?, ?, ?, ?);
        """
        update = \
        """
        update relationships set referenced_table=?, referenced_cols=?
        where references_table=? and references_cols=?;
        """

        func_iter = self.__relationship_get
        cur = self.con_a.cursor()
        cur.execute(select)
        tables = [table for table in cur.fetchall()]
        cur = self.con_s.cursor()
        for table in tables:
            for items in func_iter(table):
                try:
                    cur.execute(insert, items)
                except:
                    cur.execute(update, (items[2], items[3], items[0],
                                         items[1]))
        self.con_s.commit()


    def __relationship_get(self, table: str):
        """
        lee el contenido de la tabla MSysRelationShips y lo devuelve como
            un iterator
        """
        select = \
        """
        select szRelationship
        from MSysRelationships
        where szObject=?
        group by szRelationship
        order by szRelationship
        """

        select1 = \
        """
        select szObject, szColumn, szReferencedObject, szReferencedColumn
        from MSysRelationships
        where szRelationship=?
        order by icolumn
        """

        cur = self.con_a.cursor()
        cur.execute(select, table)
        rships = [row for row in cur.fetchall()]
        for i, rship in enumerate(rships):
            cur.execute(select1, (rship[0],))
            cols = [row for row in cur.fetchall()]
            references_table = cols[0][0]
            references_cols = ', '.join([row1[1] for row1 in cols])
            referenced_table = cols[0][2]
            referenced_cols = ', '.join([row1[3] for row1 in cols])
            yield(references_table, references_cols, referenced_table,
                  referenced_cols)


    def create_tables_sql(self, schema: str):
        """
        escribe un fichero sql con las instrucciones para crear las tablas
        schema: nombre del esquema; si '' las tablas se crean es el esquema
            public
        Los nombres de las tablas se cambian a ascci en minúscula
        """

        select = \
        """
        select name, primary_key
        from tables
        where table_type = 'TABLE'
        order by name
        """

        select1 = \
        """
        select col_name, column_size, pg_type_name
        from columns
        where table_name=?
        order by col_number;
        """

        select2 = \
        """
        select references_table, references_cols, referenced_table,
            referenced_cols
        from relationships
        left join tables on relationships.references_table=tables.name
        where tables.table_type = 'TABLE'
        order by references_table;
        """

        from os.path import join

        SPECIFIC_NAME = '_create_tables.sql'
        stm = 'DROP TABLE IF EXISTS {} CASCADE;\n'
        stm1 = 'CREATE TABLE {} (\n'
        stm2 = 'PRIMARY KEY ({}));\n'
        add_foreignkey = 'alter table {} add constraint {} foreign key ({})' +\
        ' references {} ({}) on update cascade;\n'
        stm3 = 'create schema if not exists {};\n'
        stm4 = 'alter table {} set schema {};\n'
        if len(schema.strip()) == 0 or schema.lower() == 'public':
            is_schema = False
        else:
            is_schema = True

        try:
            self.__open_connections()
            fo = join(self.dir_out, f'{self.base_name}'+f'{SPECIFIC_NAME}')
            cur = self.con_s.cursor()
            cur.execute(select)
            tables = [table for table in cur.fetchall()]
            headers = '\n'.join(self.SQL_FILE_HEADERS)
            with open(fo, 'w') as f:
                f.write(f'{headers}\n')
                if is_schema:
                    stm3 = stm3.format(schema.lower())
                    f.write('\n{}\n'.format(stm3))
                for table in tables:
                    pg_table_name = self.to_ascii(table[0])
                    f.write('\n')
                    f.write(stm.format(pg_table_name))
                    f.write(stm1.format(pg_table_name))
                    cur.execute(select1, (table[0],))
                    rows = [f'{Migrate.to_ascii(row[0])} {row[2]}'
                            for row in cur.fetchall()]
                    columns = ',\n'.join(rows)
                    f.write(f'{columns}')
                    if table[1]:
                        f.write(',\n')
                        pk_columns = Migrate.pk_columns(table[1])
                        f.write(stm2.format(pk_columns))
                    else:
                        f.write('\n);\n')

                f.write('\n/* FOREIGN KEYS */\n')

                cur.execute(select2)
                for row in cur.fetchall():
                    f.write(add_foreignkey.format(Migrate.to_ascii(row[0]),
                                                  Migrate.fk_name(row[0],
                                                                  row[1]),
                                                  Migrate.pk_columns(row[1]),
                                                  Migrate.pk_columns(row[2]),
                                                  Migrate.pk_columns(row[3])))

                if is_schema:
                    f.write('\n/* move tables to schema */\n')
                    for table in tables:
                        mytable = Migrate.to_ascii(table[0])
                        f.write(stm.format(f'{schema}.{mytable}'))
                        f.write(stm4.format(mytable, schema))

                f.write('\nCOMMIT;\n')

        except:
            from traceback import format_exc
            msg = format_exc()
            logging.append(msg)
        finally:
            self.__close_connections()


    @staticmethod
    def to_ascii(name: str, replaces: tuple = ((' ', '_'), ('-', '_'))):
        """
        cambia name a un str con caracteres ascci, alguna sustitución
            adicional y minúscula
        """
        from unidecode import unidecode
        name = name.strip()
        if name[0].isdigit():
            name = 'd' + name
        for item in replaces:
            name = name.replace(item[0], item[1])
        return unidecode(unidecode(name)).lower()


    @staticmethod
    def pk_columns(pk_cols_str: str) -> str:
        """
        Sea pk_cols un string formado por una serie de columnas separadas
            por comas; la función forma una lista de columnas, les aplica la
            función to_ascii y forma un string con los nuevos nombres de
            columnas separados por comas
        """
        pk_columns = pk_cols_str.split(',')
        return ', '.join([Migrate.to_ascii(col) for col in pk_columns])


    @staticmethod
    def fk_name(table: str, cols: str) -> str:
        """
        devuelve el nombre de una foreign key
        """
        mytable = Migrate.to_ascii(table)
        columns = cols.split(',')
        columns = '_'.join([Migrate.to_ascii(col) for col in columns])
        return f'{mytable}_{columns}_fkeys'


    def export_data_to_csv(self):
        """
        exporta los datos de la db access to csv
        """
        select = \
        """
        select name from tables where table_type='TABLE' order by name;
        """

        select1 = \
        """
        select * from "{}";
        """
        import csv
        from os.path import join
        from traceback import format_exc

        try:
            self.__open_connections()
            cur = self.con_s.cursor()
            cur.execute(select)
            tables = [table[0] for table in cur.fetchall()]

            cur = self.con_a.cursor()
            for table in tables:
                fname = join(self.dir_out, f'{table}.csv')
                column_names = [row.column_name for row in cur.columns(table)]
                try:
                    cur.execute(select1.format(table))
                except:
                    msg = format_exc()
                    logging.append(f'tabla {table}\n{msg}')

                with open(fname, 'w') as csvfile:
                    writer = csv.writer(csvfile,
                                        delimiter=',',
                                        quotechar='"',
                                        quoting=csv.QUOTE_NONNUMERIC,
                                        lineterminator='\n')
                    writer.writerow(column_names)
                    for row in cur:
                        writer.writerow(row)

        except:
            from traceback import format_exc
            msg = format_exc()
            logging.append(msg)
        finally:
            self.__close_connections()


    def py_upsert(self, schema: str, file_ini: str, section: str):
        """
        Inserta nuevos registros o actualiza los existentes en la db postgres
            leyendo directamente los datos de la db access
        Las contenidos de las tablas se insertan considerando claves foráneas
            de una sola columna; si no se cumple hay que modificar la
            programación
        """
        from os.path import join
        import psycopg2
        from traceback import format_exc

        FILE = '_tablas_ordenadas_insertar.txt'

        select1 = \
        """
        select * from "{}";
        """

        insert = "insert into {} ({}) values ({});"

        upsert = \
        "insert into {} ({}) values ({}) on conflict ({}) do " +\
        "update set {};"

        upsert1 = \
        "insert into {} ({}) values ({}) on conflict ({}) do nothing;"

        try:
            con_pg = None
            mytable = ''
            params = Migrate.con_params_get(file_ini, section)
            con_pg = psycopg2.connect(**params)
            cur_pg = con_pg.cursor()

            self.__open_connections()
            cur = self.con_s.cursor()
            tables = self.tables_input_order()
            with open(join(self.dir_out, FILE), 'w') as f:
                for table in tables:
                    f.write(f'{table[0]}\n')

            cur = self.con_a.cursor()
            for table in tables:
                mytable = Migrate.to_ascii(table[0])
                print(mytable)
                if schema:
                    mytable = schema + '.' + mytable
                cols = [Migrate.to_ascii(row.column_name)
                        for row in cur.columns(table[0])]
#                ii = [i for i, row in enumerate(cur.columns(table[0])) if
#                      row.data_type == 'DATETIME']
                cols_str = ', '.join(cols)
                placeholders = ', '.join(['%s' for col in cols])
                if table[1]:
                    pk_str = Migrate.primary_key_as_pg(table[1])
                    cols_2_update_str = Migrate.cols_to_update(table[1], cols)
                    if cols_2_update_str:
                        insert0 = upsert.format(mytable, cols_str,
                                                placeholders, pk_str,
                                                cols_2_update_str)
                        on_conflict_update = True
                    else:
                        insert0 = upsert1.format(mytable, cols_str,
                                                 placeholders, pk_str)
                        on_conflict_update = False
                else:
                    insert0 = insert.format(mytable, cols_str, placeholders)
                cur.execute(select1.format(table[0]))
                for i, row in enumerate(cur.fetchall()):
                    if table[1]:
                        uvalues = Migrate.upsert_values(table[1], table[2],
                                                        cols, row,
                                                        on_conflict_update)
                    else:
                        uvalues = row
                    cur_pg.execute(insert0, uvalues)

                con_pg.commit()
        except psycopg2.Error as er:
            msg = format_exc()
            msg1 = f'{mytable}, {er.pgcode}: {er.diag.message_primary}\n{msg}'
            logging.append(msg1)
        except:
            msg = format_exc()
            logging.append(msg)
        finally:
            self.__close_connections()
            if con_pg is not None:
                con_pg.close()


    @staticmethod
    def format_dates(ii: list, row: list):
        """
        cambia los tipos fecha a yyyy-mm-dd HH:MM:SS válidos para postgres
        """
        from datetime import datetime, date
        for i in ii:
            if isinstance(row[i], datetime):
                row[i] = row[i].strftime('%Y-%m-%d %H:%M:%S')
            elif isinstance(row[i], date):
                row[i] = row[i].strftime('%Y-%m-%d 00:00:00')


    def tables_input_order(self) -> list:
        """
        devuelva las tablas en el order de carga de acuerdo a las foreign
            keys
        """
        MAXITER =25

        create_table = \
        """
        create table tables (
        name TEXT,
        primary_key TEXT,
        references_column TEXT,
        id INTEGER,
        PRIMARY KEY (name))"""

        insert = \
        "insert into tables (name, primary_key, id, references_column) " +\
        "values (?, ?, ?, ?)"

        select = \
        "select DISTINCT name, primary_key " +\
        "from tables t " +\
        "left join relationships r on r.references_table = name " +\
        "where table_type='TABLE' and r.references_table is null " +\
        "order by name;"

        select1 = \
        "select DISTINCT name, primary_key, r.references_cols " +\
        "from tables t " +\
        "left join relationships r on r.references_table = name " +\
        "where table_type='TABLE' and r.references_table is not null " +\
        "order by name;"

        select2 = \
        """select r.referenced_table
        from tables t
        left join relationships r on r.references_table = t.name
        where t.name = ?
        order by r.referenced_table;"""

        select3 = \
        "select name " +\
        "from tables t " +\
        "where name=?;"

        select4 = \
        "select name, primary_key " +\
        "from tables " +\
        "order by id;"

        con_mem = sqlite3.connect(':memory:')
        cur_mem = con_mem.cursor()
        cur_mem.execute(create_table)

        cur = self.con_s.cursor()
        cur.execute(select)
        tables = [(table[0], table[1], None, i)
                  for i, table in enumerate(cur.fetchall())]
        for table in tables:
            cur_mem.execute(insert, table)
        con_mem.commit()
        n = len(tables) - 1

        cur.execute(select1)
        tables2add = [table for table in cur.fetchall()]

        not_added_tables = []
        i = 0
        while True:
            i += 1
            if i > MAXITER:
                raise ValueError('tables_input_order, se ha alcanzado ' +\
                                 'el número máx. de iterecciones ' +\
                                 f'{MAXITER:d}')
            for t2add in tables2add:
                cur.execute(select2, (t2add[0],))
                tt = [item for item in cur.fetchall()]
                to_insert = True
                for tt1 in tt:
                    tinserted = cur_mem.execute(select3, (tt1[0],)).fetchall()
                    if not tinserted:
                        not_added_tables.append(t2add)
                        to_insert = False
                        break
                if to_insert:
                    n += 1
                    cur_mem.execute(insert, (t2add[0], t2add[1], t2add[2], n))
                    con_mem.commit()
            if not_added_tables:
                tables2add = [t1 for t1 in not_added_tables]
                not_added_tables = []
            else:
                break
        tables = [table for table in cur_mem.execute(select4).fetchall()]
        return tables


    @staticmethod
    def primary_key_as_pg(pkeys: str) -> str:
        """
        transforma la expresión access de las primary key a postgres
        """
        pk_columns = [Migrate.to_ascii(col) for col in pkeys.split(',')]
        return ', '.join(pk_columns)


    @staticmethod
    def cols_to_update(pkeys: str, col_names: list) -> str:
        """
        columnas para actualizar con parámetros
        """
        pk_columns = [Migrate.to_ascii(col) for col in pkeys.split(',')]
        ucolumns = [f'{col_name} = %s' for col_name in col_names
                    if col_name not in pk_columns]
        return ', '.join(ucolumns)


    @staticmethod
    def update_where_columns(pkeys: str, col_names: list) -> str:
        """
        condición where: columnas con parameters
        """
        pk_columns = [Migrate.to_ascii(col) for col in pkeys.split(',')]
        kcolumns = [f'{col_name} = %s' for col_name in col_names
                    if col_name in pk_columns]
        return ', '.join(kcolumns)


    @staticmethod
    def sort_row(pkeys: str, col_names: list, row: list) -> list:
        """
        reordena las columnas devueltas por la select para adecuarlas a un
            update de una tabla con primary keys
        """
        pk_columns = [Migrate.to_ascii(col) for col in pkeys.split(',')]
        kcolumns = [row[i] for i, col_name in enumerate(col_names)
                    if col_name in pk_columns]
        ucolumns = [row[i] for i, col_name in enumerate(col_names)
                    if col_name not in pk_columns]
        return ucolumns + kcolumns


    @staticmethod
    def upsert_values(pkeys: str, references_col: str, col_names: list,
                      row: list, on_conflict_update: bool) -> list:
        """
        Forma la lista de valores para la sentencia upsert
        Los valores de las claves principales y de las claves ajenas se
            convierten en minúsculas
        """
        pk_columns = [Migrate.to_ascii(col) for col in pkeys.split(',')]
        data2insert = [row[i] for i, col_name in enumerate(col_names)]
        i2lower = [i for i, col_name in enumerate(col_names)
                   if col_name in pk_columns]
        for i in i2lower:
            data2insert[i] = data2insert[i].lower()

        if references_col is not None:
            for i, col_name in enumerate(col_names):
                if col_name == references_col:
                    data2insert[i] = data2insert[i].lower()
                    break

        if not on_conflict_update:
            return data2insert
        data2update = [row[i] for i, col_name in enumerate(col_names)
                    if col_name not in pk_columns]
        return data2insert + data2update


    @staticmethod
    def __access_pg_types():
        d = {
             'LONGBINARY': 'bytea',
             'BINARY': 'bytea',
             'BIT': 'boolean',
             'BYTE': 'int2',
             'COUNTER': 'int4',
             'CURRENCY': 'numeric',
             'DATETIME': 'timestamptz',
             'GUID': 'bytea',
             'INTEGER': 'int4',
             'LONGBINARY': 'bytea',
             'LONGTEXT': 'varchar',
             'SINGLE': 'float4',
             'SMALLINT': 'int2',
             'DOUBLE': 'float8',
             'UNSIGNED BYTE': 'int2',
             'SHORT': 'int4',
             'LONG': 'int8',
             'LONGCHAR': 'varchar',
             'NUMERIC': 'numeric',
             'LONGBINARY': 'bytea',
             'REAL': 'float4',
             'VARCHAR': 'varchar',
             'VARBINARY': 'bytea'
            }
        return d


    @staticmethod
    def con_params_get(file_ini: str, section: str) -> str:
        """
        devuelve los parámetros de la conexiona una db postgres, obtenidos de
            la sección section del fichero FILE
        """
        from configparser import ConfigParser
        parser = ConfigParser()
        parser.read(file_ini)
        db = {}
        if parser.has_section(section):
            params = parser.items(section)
            for param in params:
                db[param[0]] = param[1]
        else:
            raise ValueError(f'No se encuentra section {section} en' +\
                             f' {file_ini}')
        return db


#def write_copyfrom(table_name, sfield_names, dir_out, fo):
#    """
#    writes \copy .. from psql metacommand one for each table
#    """
#    stm = ''.join(copyfrom)
#    csv_file = csv_file_name_get(dir_out, table_name)
#    stm1 = stm.format(table_name, sfield_names, csv_file, sfield_names)
#    fo.write('{}\n\n'.format(stm1))
