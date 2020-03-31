# -*- coding: utf-8 -*-
"""
módulo con la asignación de los parámetros del programa
también se explica el significado de cada parámetro

@solis
"""
# _________________DATOS_______________________

# ruta de la base de datos Access a migrar
db: str = r'H:\off\db\analíticas_MASubt_07\SEGUIMIENTO_2018\MASb_Calidad\OPH_Subterraneas_2018.mdb'

# directorio de resultados (debe existir)
dir_out: str = r'H:\off\db\analíticas_MASubt_07\SEGUIMIENTO_2018\MASb_Calidad\topostgres'

# schema_name
# nombre del esquema donde se crearán las tablas si '' -> public
schema_name: str = ''

# datos de la conexión a postgres
# file_ini fichero ini con los datos de la conexión a la db postgres
# section nombre de la sección del fichero ini con los datos de la conexión
file_ini = 'pgdb.ini'
section = 'ipa'

