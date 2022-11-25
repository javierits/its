from logger_base import logger
from psycopg2 import pool
import sys

class Connection:
    __DATABASE = 'test_db'
    __USERNAME = 'postgres'
    __PASSWORD = 'javier2021#'
    __DB_PORT = '5432'
    __HOST = '127.0.0.1'
    __MIN_CON = 1
    __MAX_CON = 5
    __pool = None 
    
    @classmethod
    def obtenerPool(cls):
        if cls.__pool is None:
            try:
                cls.__pool = pool.SimpleConnectionPool(
                    cls.__MIN_CON,
                    cls.__MAX_CON,
                    host=cls.__HOST,
                    user=cls.__USERNAME,
                    password=cls.__PASSWORD,
                    port=cls.__DB_PORT,
                    database=cls.__DATABASE)
                logger.debug(f'Creación pool exitosa: {cls.__pool}')
                return cls.__pool
            except Exception as e:
                logger.error(f'Error al crear el pool de conexiones: {e}')
                sys.exit()
        else:
            return cls.__pool

    @classmethod
    def obtenerConexion(cls):
        #Obtener una conexion del pool
        
        connection = cls.obtenerPool().getconn()
        logger.debug(f'Conexión obtenida del pool: {connection}')
        return connection
    
    @classmethod
    def liberarConexion(cls, connection):
        #Regresar el objeto conexion al pool
        
        cls.obtenerPool().putconn(connection)
        logger.debug(f'Regresamos la conexión al pool: {connection}')
        logger.debug(f'Estado del pool: {cls.__pool}')
        
    @classmethod
    def cerrarConexiones(cls):
        #Cerrar el pool y todas sus conexiones
        
        cls.obtenerPool().closeall()
        logger.debug(f'Cerramos todas las conexiones del pool: {cls.__pool}')
        
if __name__=='__main__':
    #Obtener una conexión a partir del pool
    connection1 = Connection.obtenerConexion()
    connection2 = Connection.obtenerConexion()
    
    #Regresamos las conexiones al pool
    Connection.liberarConexion(connection1)
    Connection.liberarConexion(connection2)
    
    #Cerramos el pool
    Connection.cerrarConexiones()
    
    #Si intentamos pedir una conexión de un pool cerrado manda un error