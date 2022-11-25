from asyncio.log import logger
from connection import Connection
from logger_base import logger

class CursorDelPool:
    def __init__(self):
        self.__conn = None,
        self.__cursor = None
        
    #Inicio de with
    def __enter__(self):
        logger.debug('Inicio de with método __enter__')
        self.__conn = Connection.obtenerConexion()
        self.__cursor = self.__conn.cursor()
        return self.__cursor
    
    #Fin del bloque with
    def __exit__(self, exception_type, exception_value, exception_traceback):
        logger.debug('Se ejecuta método __exit__()')
        #if exception_value is not None:
        
        if exception_value:
            self.__conn.rollback()
            logger.debug(f'Ocurrió una excepción: {exception_value}')
        else:
            self.__conn.commit()
            logger.debug('Commit de la transacción')
        #Cerramos el cursor
        self.__cursor.close()
        #Regresar la conexión al pool
        Connection.liberarConexion(self.__conn)
        
if __name__ == '__main__':
    #Obtenemos un cursor a partir de la conexión del pool
    #with se ejecuta __enter__ y termina con __exit__
    with CursorDelPool() as cursor:
        cursor.execute('SELECT * FROM persona')
        logger.debug('Listado de personas')
        logger.debug(cursor.fetchall())