# -*- coding: utf-8 -*-
"""
módulo con la asignación de los parámetros del programa
también se explica el significado de cada parámetro

@solis
"""
# _________________DATOS_______________________

# ruta de la base de datos Access a migrar
db: str = r'H:\off\db\analiticas_masub\PH_09_15_y_15_21\BD_FIC_AGBAR_2009_2013\PERIODO_AGBAR_FEDERICO_para_exportar.mdb'

# directorio de resultados (debe existir)
dir_out: str = r'H:\off\db\analiticas_masub\PH_09_15_y_15_21\BD_FIC_AGBAR_2009_2013\export2psql'

# schema_name
# nombre del esquema donde se crearán las tablas si '' -> public
schema_name: str = 'tmp'

# datos de la conexión a postgres
# file_ini fichero ini con los datos de la conexión a la db postgres
# section nombre de la sección del fichero ini con los datos de la conexión
file_ini = 'pgdb.ini'
section = 'ipa'

