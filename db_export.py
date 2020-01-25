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
import littleLogging as logging

header_sql_file = ('SET CLIENT_ENCODING TO UTF8;',
                   'SET STANDARD_CONFORMING_STRINGS TO ON;')

# número de líneas separadoras en los ficheros output
NHYPHEN = 70

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

    def __init__(self, dbaccess: str, recreate_tables=True):
        from os.path import isfile, join, split, splitext

        if not isfile(dbaccess):
            raise ValueError(f'No existe {dbaccess}')

        self.constr_access = \
        r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};' +\
        f' DBQ={dbaccess};'

        head, tail = split(dbaccess)
        name, ext = splitext(tail)
        dbs = join(head, f'{name}_struct.db')
        self.constr_sqlite = dbs
        self.con_a = None
        self.con_s = None
        self.recreate_tables = recreate_tables


    def structure_to_sqlite(self):
        """
        Lee la estructura de la db access y crea un fichero sqlite con la
            información de la estructura (no los datos)
        """
        import pyodbc
        import sqlite3
        try:
            self.con_a = pyodbc.connect(self.constr_access)
            self.con_s = sqlite3.connect(self.constr_sqlite)
            self.__create_tables()
            self.__populate_tables()
        except:
            from traceback import format_exc
            msg = format_exc()
            logging.append(msg)
        finally:
            if self.con_a:
                self.con_a.close()
            if self.con_s:
                self.con_s.close()


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
        type TEXT,
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
        PRIMARY KEY (table_name, col_name))"""

        create_table3 = \
        """
        create table if not exists relationships (
        name TEXT,
        table_name TEXT,
        col_name TEXT,
        ncolumn INTEGER,
        icolumn INTEGER,
        parent_table TEXT,
        parent_column TEXT,
        PRIMARY KEY (name))"""

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
        """insert into tables (name, type, primary_key) values (?, ?, ?)"""
        update = \
        """update tables set type=?, primary_key=? where name=?"""

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
        (table_name, col_name, type_i, type_name, column_size, pg_type_name)
        values (?, ?, ?, ?, ?, ?);
        """
        update = \
        """
        update columns set type_i=?, type_name=?, column_size=?, pg_type_name=?
        where table_name=? and col_name=?;
        """
        d = MsAccess_migrate.__access_pg_types()
        cur = self.con_a.cursor()
        cur_s = self.con_s.cursor()
        for table in tables:
            for row in cur.columns(table[0]):
                if row.type_name in d:
                    pg_col_type = d[row.type_name]
                else:
                    pg_col_type = ''
                try:
                    cur_s.execute(insert, (table[0], row.column_name,
                                           row.sql_data_type, row.type_name,
                                           row.column_size, pg_col_type))
                except:
                    cur_s.execute(update, (row.sql_data_type, row.type_name,
                                           row.column_size, pg_col_type,
                                           table[0], row.column_name))
            self.con_s.commit()


    def __insert_into_relationships(self, tables):
        """
        inserta las foreign keys en relationships
        """
        select = \
        """
        select szRelationship, szReferencedObject, szReferencedColumn,
            ccolumn, icolumn, szObject, szColumn
        from MSysRelationships
        where szReferencedObject=?
        order by ccolumn, icolumn
        """
        insert = \
        """
        insert into relationships (name, table_name, col_name, ncolumn,
        icolumn, parent_table, parent_column)
        values (?, ?, ?, ?, ?, ?, ?)"""
        update = \
        """update relationships set table_name=?, col_name=?, ncolumn=?,
        icolumn=?, parent_table=?, parent_column=? where name=?"""

        cur = self.con_s.cursor()
        cur1 = self.con_a.cursor()
        for table in tables:
            try:
                cur1.execute(select, (table[0],))
            except:
                raise ValueError(f'error al ejecutar\n{select}\n' +\
                                 f'con la tabla {table[0]}')
            rows = [row for row in cur1.fetchall()]

            try:
                cur.execute(insert, table)
            except:
                cur.execute(update, (table[1], table[2], table[0]))
        self.con_s.commit()


    def _relationship_get(self, tables):
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
        where szReferencedObject=?
        order by icolumn
        """

        cur = self.con_a.cursor()
        cur1 = self.con_a.cursor()
        for table in tables:
            cur.execute(select, (table[0],))
            for i, rship in enumerate(cur.fetchall()):
                cur1.execute(select1, (rship,))
                cols = [(row[0], row[1], row[2], row[3])
                        for row in cur1.fetchall()]
                referenced_table = cols[0][0]
                referenced_cols = [fila[1] for fila in cols]
                parent_table = cols[0][2]
                parent_cols = [fila[3] for fila in cols]
                referenced_cols = ', '.join(referenced_cols)
                parent_cols = ', '.join(parent_cols)
                rship_name = f'{referenced_table}_{parent_table}_{i:d}'
                yield(table[0], referenced_cols, )


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
             'VARBINARY': 'varchar'
            }
        return d


def ms_access_structure_get():
    """
    extracts tables column names and call function thar write each
    table structure
    input:
        db (str): access data base file (must exist)
        dir_out (str): output directory (must exists)
    """
    FILE_TABLES_NAMES = '_TABLES_NAMES.txt'
    FILE_CREATE_TABLES = '_CREATE_TABLES.sql'
    FILE_RELATIONSHIPS = '_CREATE_FOREIGNKEYS.sql'
    FILE_COPYFROM = '_COPYFROM.txt'

    from os.path import join
    import pyodbc
    from db_export_parameters import db, dir_out, wstruct, wdata
    from db_export_parameters import trelationships

    cns = constring_get(db)
    con = pyodbc.connect(cns)
    cur = con.cursor()
    tables = [table[2] for table in cur.tables() if table[3] == 'TABLE']
    print('{0:d} tables found'.format(len(tables)))

    if wstruct == 1:
        fo = open(join(dir_out, FILE_CREATE_TABLES), 'w')
        fo.write('{}'.format('\n'.join(header_sql_file)))
        focopyfrom = open(join(dir_out, FILE_COPYFROM), 'w')

    if wdata == 1:
        focopyfrom = open(join(dir_out, FILE_COPYFROM), 'w')

    table_names = []
    for i, table_name in enumerate(tables):
        print('{0:d}. {1}'.format(i+1, table_name))
        table_names.append(table_name)
        # columnas
        columns = [[row.ordinal_position, row.column_name, row.type_name,
                    row.column_size, row.nullable, row.remarks]
                   for row in cur.columns(table=table_name)]
        # primary keys
        pk_cols = [[row[7], row[8]]
                   for row in cur.statistics(table_name)
                   if row[5] is not None and
                   row[5].upper() == 'PRIMARYKEY']

        if wstruct == 1:
            write_table_struct(fo, table_name, columns, pk_cols)

        if wdata == 1:
            field_names = [column[1] for column in columns]
            write_data(table_name, field_names, con, dir_out)
            sfield_names = ','.join(field_names)
            write_copyfrom(table_name, sfield_names, dir_out, focopyfrom)

    if wdata == 1:
        focopyfrom.close()

    if wstruct == 1:
        fo.close()
        fo = open(join(dir_out, FILE_RELATIONSHIPS), 'w')
        fo.write('beguin;\n')
        cur.execute('SELECT * FROM {};'.format(trelationships))
        fks = [row for row in cur.fetchall()]
        for row in fks:
            fo.write('\n')
            fo.write('alter table if exists {} '.
                     format(row.szObject))
            fo.write('drop constraint if exists {};\n'.
                     format(row.szRelationship))

            fo.write('alter table if exists {}\n'.
                     format(row.szObject))
            fo.write('add constraint {} '.
                     format(row.szRelationship))
            fo.write('foreign key ({}) '.
                     format(row.szColumn))
            fo.write('references {0} ({1});'.
                     format(row.szReferencedObject, row.szReferencedColumn))
            fo.write(2*'\n')
            fo.write(NHYPHEN*'-' + '\n')
        fo.write('commit;\n')
        fo.close()

    table_names_str = '\n'.join(table_names)
    fo = open(join(dir_out, FILE_TABLES_NAMES), 'w')
    fo.write('{}'.format(table_names_str))
    fo.close()


def translate_msa(atype, length):
    """
    translates ms access types to postgis types
    """
    ttypes = {'TEXT': 'varchar', 'VARCHAR': 'varchar',
              'MEMO': 'varchar', 'LONGCHAR': 'varchar',
              'BYTE': 'smallint', 'INTEGER': 'integer', 'LONG': 'bigint',
              'SMALLINT': 'smallint',
              'SINGLE': 'real', 'DOUBLE': 'double precision',
              'REAL': 'double precision',
              'CURRENCY': 'money', 'AUTONUMBER': 'serial',
              'COUNTER': 'serial',
              'DATETIME': 'timestamp',
              'YES/NO': 'smallint'}
    if atype in('TEXT', 'VARCHAR', 'MEMO', 'LONGCHAR') and length > 0:
        return '{}({})'.format(ttypes[atype], length)
    else:
        return ttypes[atype]


def write_table_struct(fo, table_name, columns, pk_cols):
    """
    writes table structure in text file fo
    fo (object file): text file (must be open)
    table_name (str)
    columns [[]]: columns definition (defined in ms_access_structure_get)
    """
    fo.write(2*'\n')
    fo.write('drop table if exists {}\nbeguin;\n'.format(table_name))
    fo.write('create table if not exists ' + table_name + '(\n')
    wcolumns = ['\t{} {}'.format(column[1],
                translate_msa(column[2], column[3])) for column in columns]
    fo.write(',\n'.join(wcolumns))
    fo.write('\n')
    pk_columns = [row[1] for row in pk_cols]
    if len(pk_columns) > 0:
        pk_cstr = ','.join(pk_columns)
        fo.write('\tconstraint {0} primary key ({1})\n'.format(table_name,
                 pk_cstr))
    fo.write(');\n'+'commit;\n')
    fo.write(NHYPHEN*'-' + '\n')


def csv_file_name_get(dir_out, table_name):
    from os.path import join
    return join(dir_out, table_name + '.csv')


def write_data(table_name, field_names, con, dir_out):
    """
    writes all table data
    in
    table_name (str)
    field_names ([str,...])
    con (object connexion to an access data base)
    dir_out (str): directory to write data (must exists)
    """
    import csv
    csv_file = csv_file_name_get(dir_out, table_name)

    cur = con.cursor()
    cur.execute('select * from [' + table_name + '];')

    with open(csv_file, 'w') as csvfile:
        writer = csv.writer(csvfile,
                            delimiter=',',
                            quotechar='"',
                            quoting=csv.QUOTE_NONNUMERIC,
                            lineterminator='\n')
        writer.writerow(field_names)
        for row in cur:
            writer.writerow(row)


def write_copyfrom(table_name, sfield_names, dir_out, fo):
    """
    writes \copy .. from psql metacommand one for each table
    """
    stm = ''.join(copyfrom)
    csv_file = csv_file_name_get(dir_out, table_name)
    stm1 = stm.format(table_name, sfield_names, csv_file, sfield_names)
    fo.write('{}\n\n'.format(stm1))
