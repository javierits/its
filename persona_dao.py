from persona import Persona
from cursor_del_pool import CursorDelPool
from logger_base import logger

class PersonaDao:
    '''
    DAO (Data Access Object)
    CRUD: Create-Read-Update_Delete entidad Persona
    
    '''
    
    __SELECCIONAR = 'SELECT * FROM persona ORDER BY id_persona'
    __INSERTAR = 'INSERT INTO persona(nombre, apellido, email) VALUES(%s,%s,%s)'
    __ACTUALIZAR = 'UPDATE persona SET nombre=%s, apellido=%s, email=%s WHERE id_persona=%s'
    __ELIMINAR = 'DELETE FROM persona WHERE id_persona = %s'
    
    @classmethod
    def seleccionar(cls):
        with CursorDelPool() as cursor:
            logger.debug(cursor.modify(cls.__SELECCIONAR))
            cursor.execute(cls.__SELECCIONAR)
            registros = cursor.fetchall()
            personas = []
            for registro in registros:
                persona = Persona(registro[0], registro[1], registro[2], registro[3])
                personas.append(persona)
            return personas
        
    @classmethod
    def insertar(cls, persona):
        with CursorDelPool() as cursor:
            logger.debug(cursor.modify(cls.__INSERTAR))
            logger.debug(f'Persona a insertar: {persona}')
            valores = (persona.get_nombre(), persona.get_apellido(), persona.get_email())
            cursor.execute(cls.__INSERTAR, valores)
            return cursor.rowcount
        
    @classmethod
    def actualizar(cls, persona):
        with CursorDelPool()as cursor:
            logger.debug(cursor.modify(cls.__ACTUALIZAR))
            logger.debug(f'Persona a actualizar: {persona}')
            valores = (persona.get_nombre(), persona.get_apellido(), persona.get_email(), persona.get_id_persona())
            cursor.execute(cls.__ACTUALIZAR, valores)
            return cursor.rowcount
        
    @classmethod
    def eliminar(cls, persona):
        with CursorDelPool() as cursor:
            logger.debug(cursor.modify(cls.__ELIMINAR))
            logger.debug(f'Persona a eliminar: {persona}')
            valores = (persona.get_id_persona(),)
            cursor.execute(cls.__ELIMINAR, valores)
            return cursor.rowcount
        
if __name__ == '__main__':
    #Listado de personas
    
    personas = PersonaDao.seleccionar()
    for persona in personas:
        logger.debug(persona)
        
    #Insertamos un nuevo registro
    
    persona = Persona(nombre= 'Pedro', apellido='Najera', email='pnajera@gmail.com')
    personas_insertadas = PersonaDao.insertar(persona)
    logger.debug(f'Personas insertadas: {personas_insertadas}')

    #Actualizar un registro existente
    persona = Persona(1,'Juan', 'Perez2', 'jperez3@gmail.com')
    personas_actualizadas = PersonaDao.actualizar(persona)
    logger.debug(f'Personas actualizadas: {personas_actualizadas}') 
    
    #eliminar un registro existente
    persona = Persona(id_persona = 11)
    personas_eliminadas = PersonaDao.eliminar(persona)
    logger.debug(f'Personas eliminadas: {personas_eliminadas}')   
        
