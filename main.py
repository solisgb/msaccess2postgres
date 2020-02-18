# -*- coding: utf-8 -*-
"""
@solis

driver del módulo eb_export; permite migrar una db access a postgres;

IMPORTANTE. Los parámetros de la app se menajen desde db_export_parameters
"""
import littleLogging as logging

# _________VARIABLES QUE CONTROLAN LAS ACCIONES DE UNA EJECUCIÓN_______________
# create_db_structure
# la app crea primero un fichero sqlite con la estructura
# de la db access; si True la crea por primera vez o sobreescribe la
# existente
create_db_structure: bool = False

# write_sql
# si True escribe la estructura de las tablas en 2 ficheros sql que deben ser
# ejecutados
write_sql: bool = True

# write_data_to_csv
# si True graba los datos de cada tabla en un fichero csv
write_data_to_csv: bool = False

# py_upsert
# Si Truw ejecuta un upsert de los datos en la db access en la db postgres
upsert_py: bool = False

# keys2lower. Convierte las contenidos de las columnas implicadas en las
# claves primarias o ajenas en minúsculas (2 opciones):
# keys2lower_py si True la conversón se hece directamente con python
# keys2lower_sql si True se escribe un fichero sql
keys2lower_py: bool = False
keys2lower_sql: bool = False


if __name__ == "__main__":

    try:
        from time import time
        import db_export_parameters as par
        from db_export import Migrate as msa

        startTime = time()

        migrate = msa(par.db, par.dir_out, par.file_ini, par.section)

        if create_db_structure:
            migrate.structure_to_sqlite()

        if write_sql:
            migrate.structure_to_sql(par.schema_name)

        if write_data_to_csv:
            migrate.export_data_to_csv()

        if upsert_py:
            migrate.upsert(upsert_py)

        if keys2lower_py or keys2lower_sql:
            migrate.column_contents_2lowercase(keys2lower_py, keys2lower_sql)


        xtime = time() - startTime
        print(f'The script took {xtime:.2f} secs')

    except Exception as e:
        import traceback
        print(e)
        msg = '\n{}'.format(traceback.format_exc())
        logging.append(msg)
    finally:
        logging.dump()
        print('Se ha generado el fichero app.log')
