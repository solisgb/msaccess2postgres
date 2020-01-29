# -*- coding: utf-8 -*-
"""
Created on Sat Feb  9 13:12:53 2019

@author: Solis
El módulo permite:
1 Crear una sb sqlite con la estructura de una db ms access
2 Crear ficheros sql para recrear la estructura de la db access en otro
    rdbs
3 Exporta los datos de la db access a ficheros csv

NOTA IMPORTANTE
Para leer las foreign keys debes, en ms access:
1 Hacer visibles las tablas del sistema: opciones -> base de datos actual ->
    navegación, opciones de navegación, opciones de presentación, marcar
    mostrar objetos del sistema
2 Permitir la lectura de la tabla MSysRelationShips: opciones -> archivo ->
    información -> administrar usuarios y permisos ->
    permisos y usuarios de grupo, como administrador, tipo de objeto tabla,
    nombre del objeto MysRelationShips, marcar leer diseño y leer datos
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

class MsAccess_migrate():
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
        try:
            self.__open_connections()
            self.__create_tables()
            self.__populate_tables()
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
            self.con_a = None


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
        referenced_table TEXT,
        referenced_cols TEXT,
        parent_table TEXT,
        parent_cols TEXT,
        my_rship_name TEXT,
        PRIMARY KEY (referenced_table, referenced_cols),
        FOREIGN KEY(referenced_table) REFERENCES tables(name),
        FOREIGN KEY(parent_table) REFERENCES tables(name))
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

        d = self.__access_pg_types()
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
        SELECT szReferencedObject
        FROM MSysRelationships
        GROUP BY szReferencedObject
        ORDER BY szReferencedObject;
        """

        insert = \
        """
        insert into relationships (referenced_table, referenced_cols,
            parent_table, parent_cols, my_rship_name)
        values (?, ?, ?, ?, ?);
        """
        update = \
        """
        update relationships set parent_table=?, parent_cols=?,
            my_rship_name=?
        where referenced_table=? and referenced_cols=?;
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
                    cur.execute(update, (items[2], items[3], items[4],
                                         items[0], items[1]))
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
        where szReferencedObject=?
        group by szRelationship
        order by szRelationship
        """

        select1 = \
        """
        select szReferencedObject, szReferencedColumn, szObject, szColumn
        from MSysRelationships
        where szRelationship=?
        order by icolumn
        """

        cur = self.con_a.cursor()
        cur.execute(select, table)
        rships = [row for row in cur.fetchall()]
        for i, rship in enumerate(rships):
            cur.execute(select1, (rship[0],))
            cols = [(row[0], row[1], row[2], row[3])
                    for row in cur.fetchall()]
            referenced_table = cols[0][0]
            referenced_cols = [fila[1] for fila in cols]
            parent_table = cols[0][2]
            parent_cols = [fila[3] for fila in cols]
            referenced_cols = ', '.join(referenced_cols)
            parent_cols = ', '.join(parent_cols)
            rship_name = f'fk_{referenced_table}_{parent_table}_{i:d}'
            yield(referenced_table, referenced_cols, parent_table,
                  parent_cols, rship_name)


    def create_tables_sql(self):
        """
        escribe un fichero sql con las instrucciones para crear las tablas
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
        select * from relationships
        left join tables on relationships.referenced_table=tables.name
        where tables.table_type = 'TABLE'
        order by referenced_table;
        """

        from os.path import join

        SPECIFIC_NAME = '_create_tables.sql'
        STM = 'DROP TABLE IF EXISTS {} CASCADE;\n\n'
        STM1 = 'CREATE TABLE {} (\n'
        STM2 = 'PRIMARY KEY ({}))\n'
        add_foreignkey = 'alter table {} add constraint {} foreign key ({})' +\
        ' references {} ({}) on update cascade;'

        try:
            self.__open_connections()
            fo = join(self.dir_out, f'{self.base_name}'+f'{SPECIFIC_NAME}')
            cur = self.con_s.cursor()
            cur.execute(select)
            tables = [table for table in cur.fetchall()]
            headers = '\n'.join(self.SQL_FILE_HEADERS)
            with open(fo, 'w') as f:
                f.write(f'{headers}\n')
                for table in tables:
                    f.write('\n')
                    f.write(STM.format(table[0].lower()))
                    f.write(STM1.format(table[0].lower()))
                    cur.execute(select1, (table[0],))
                    rows = [f'{row[0].lower()} {row[2]}'
                            for row in cur.fetchall()]
                    columns = ',\n'.join(rows)
                    f.write(f'{columns}')
                    if table[1]:
                        f.write(',\n')
                        f.write(STM2.format(table[1]))
                    else:
                        f.write('\n)\n')

                f.write('\n/* FOREIGN KEYS */\n')

                cur.execute(select2)
                for row in cur.fetchall():
                    f.write(add_foreignkey.format(row[0], row[4], row[1],
                                                  row[2], row[3]))
                    f.write('\n')
                f.write('COMMIT;\n')
        except:
            from traceback import format_exc
            msg = format_exc()
            logging.append(msg)
        finally:
            self.__close_connections()


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


    def upsert_sql(self):
        """
        Crea los ficheros sql con la sentencia upsert
        la tabla no tiene valores autonuméricos por lo que se inserta el valor
            de la primary key
        """

        select = \
        """
        select name, primary_key from tables
        where table_type='TABLE'
        order by name;
        """

        select1 = \
        """
        select * from "{}";
        """

        upsert = \
        """
        insert into {} ({})
        values ({}))
        on conflict ({})
        do
        update set {};
        """

        from os.path import join
        from traceback import format_exc

        try:
            self.__open_connections()
            cur = self.con_s.cursor()
            cur.execute(select)
            tables = [table[0] for table in cur.fetchall()]
            headers = '\n'.join(self.SQL_FILE_HEADERS)

            cur = self.con_a.cursor()
            for table in tables:
                fname = join(self.dir_out, f'{table[0]}_upsert.sql')
                cols = [(row.column_name, row.type_name)
                        for row in cur.columns(table[0])]
                col_names = [row[0] for row in cols]
                col_names = ', '.join(col_names)
                try:
                    cur.execute(select1.format(table[0]))
                except:
                    msg = format_exc()
                    logging.append(f'tabla {table[0]}\n{msg}')

                with open(fname, 'w') as f:
                    f.write(f'{headers}\n')

                    for row in cur:
                        insert_values = self.__insert_values_get(row)
                        upsert_row = upsert.format(table[0], col_names )

        except:
            from traceback import format_exc
            msg = format_exc()
            logging.append(msg)
        finally:
            self.__close_connections()


    @staticmethod
    def __insert_values_get(row: list) -> str:
        """
        Forma una cadena de texto con los valores a insertar en una sentencia
            insert
        """
        for item in row:
            pass


    @staticmethod
    def __access_pg_types():
        d = {
             'LONGBINARY': 'bytea',
             'BINARY': 'bytea',
             'BIT': 'int2',
             'BYTE': 'bytea',
             'COUNTER': 'int4',
             'CURRENCY': 'numeric',
             'DATETIME': 'timestamp',
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

#def write_copyfrom(table_name, sfield_names, dir_out, fo):
#    """
#    writes \copy .. from psql metacommand one for each table
#    """
#    stm = ''.join(copyfrom)
#    csv_file = csv_file_name_get(dir_out, table_name)
#    stm1 = stm.format(table_name, sfield_names, csv_file, sfield_names)
#    fo.write('{}\n\n'.format(stm1))
