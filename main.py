# -*- coding: utf-8 -*-
"""
dbexport_main; script para:
    1 muestra los tipos exitentes en una BDD access
      y su correspondencia con los tipos postgres
    2 escribe las sentencias psql para crear cada una de las tablas de
      la base de datos Access
      en la actualidad no soporta primary keys o foreign relations
      formadas por varios campos
    3 Hacer una copia de todos los datos de cada tabla en ficheros csv

Los datos del script y las opciones al ejecutar el script se definen
    por el usuario en el módulo db_export_parameters.py
Las opciones de ejecución son:
    get_types = 1 (1)
    wstruct = 1 (2)
    wdata = 1 (3)
    wstruct = 1 y wdata = 1 (2 y 3)

El controlador de Acces no permite a pyodbc obtener las foreign keys de la
BDD; tampoco me permite hacer una select a la tabla MsysRelationShips

Para solventarlo es necesario hacer una copia del fichero interno se Access
MsysRelationShips (select * from msysrelationships) y guardarlo en una
tabla con otro nombre. De esta tabla ya se puede hacer una select para obtener
las foreign keys.

Warning. Actualizar la tabla de relaciones antes de ejecutarlo el script
para asegurarse que entran todas

@solis
"""
import littleLogging as logging

if __name__ == "__main__":

    try:
        from time import time
        import db_export_parameters as par
        from db_export import MsAccess_migrate as msa

        startTime = time()

        migrate = msa(par.db)
        migrate.structure_to_sqlite()

        xtime = time() - startTime
        print(f'The script took {xtime:.2f} secs')

    except Exception as e:
        import traceback
        print(e)
        msg = '\n{}'.format(traceback.format_exc())
        logging.append(msg)
    finally:
        logging.dump()
        print('Se ha generado el fichero log.txt')
