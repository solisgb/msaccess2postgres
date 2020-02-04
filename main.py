# -*- coding: utf-8 -*-
"""
@solis

driver del módulo eb_export; permite migrar una db access a postgres;
    ver la documentación del módulo

Las opciones de la app se menajen desde el módulo db_parameter_parameters
"""
import littleLogging as logging

if __name__ == "__main__":

    try:
        from time import time
        import db_export_parameters as par
        from db_export import Migrate as msa

        startTime = time()

        migrate = msa(par.db, par.dir_out)

        if par.create_db_structure:
            migrate.structure_to_sqlite()

        if par.write_sql:
            migrate.create_tables_sql(par.schema_name)

        if par.write_data_to_csv:
            migrate.export_data_to_csv()

        if par.py_upsert:
            migrate.py_upsert(par.schema_name, par.file_ini, par.section)

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
