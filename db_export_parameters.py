# -*- coding: utf-8 -*-
"""
módulo con la asignación de los parámetros del programa
también se explica el significado de cada parámetro
@solis
"""
# _________________DATOS_______________________

# ruta de la base de datos Access a migrar
db: str = r'C:\Users\solis\Documents\DB\Ipasub97.mdb'

# directorio de resultados (debe existir)
dir_out: str = r'C:\Users\solis\Documents\DB\bak'

# ______________VARIABLES QUE CONTROLAN LAS ACCIONES DEL SCRIPT_______________
# create_db_structure
# la app crea primero un fichero sqlite con la estructura
# de la db access; si True la crea por primera vez o sobreescribe la
# existente
create_db_structure: bool = False

# write_sql
# escribe la estructura de las tablas en un fichero sql apto para ser ejecutado
# desde psql
write_sql: bool = False

# schema_name
# nombre del esquema donde se crearán las tablas si '' -> public
schema_name: str = 'ipa'

# write_data_to_csv
# copia los datos de cada tabla en un fichero csv
write_data_to_csv: bool = False

# py_upsert
# inserta los datos de la dc access en la db postgres con python
# file_ini fichero ini con los datos de la conexión a la db postgres
# section nombre de la sección del fichero ini con los datos de la conexión
py_upsert: bool = False
file_ini = 'pgdb.ini'
section = 'h2ogeo'
