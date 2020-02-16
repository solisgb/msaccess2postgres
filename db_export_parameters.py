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

# schema_name
# nombre del esquema donde se crearán las tablas si '' -> public
schema_name: str = 'ipa'

# datos de la conexión a postgres
# file_ini fichero ini con los datos de la conexión a la db postgres
# section nombre de la sección del fichero ini con los datos de la conexión
file_ini = 'pgdb.ini'
section = 'h2ogeo'

