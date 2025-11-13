from database import create_db_and_tables, get_db
from models.clients import Cliente, SmsIncoming
from sqlalchemy.orm import Session
from setupLog import setup_logging, logging
setup_logging()
log = logging.getLogger(__name__)

def inicializar_aplicacion():
    """
    Función que se ejecuta al inicio para preparar la base de datos.
    """
    log.info("Iniciando la base de datos...")
    # Llama a la función para crear las tablas si no existen
    create_db_and_tables()
    log.info("base de datos lista.")

def ejemplo_de_uso():
    """
    Una función de demostración para añadir y consultar datos.
    """
    log.info("\n--- Ejecutando ejemplo de uso ---")
    
    # Obtenemos una sesión de la base de datos
    # En FastAPI, esto lo haría la dependencia get_db() automáticamente
    db: Session = next(get_db())

    try:
        # 1. Crear un nuevo cliente
        log.info("Creando un nuevo cliente...")
        nuevo_cliente = Cliente(
            sender="+15551234567",
            email_cliente="cliente.ejemplo@email.com",
            ftp_directorio="/home/ftp/cliente_ejemplo"
        )
        print(nuevo_cliente)
        db.add(nuevo_cliente)
        db.commit() # Guardar cambios en la base de datos
        db.refresh(nuevo_cliente) # Refrescar el objeto con los datos de la BD
        log.info(f"Cliente creado: {nuevo_cliente}")

        # 2. Añadir un SMS para ese cliente
        log.info("\nAñadiendo un nuevo SMS...")
        nuevo_sms = SmsIncoming(
            message_id="MSG_ID_001",
            sender=nuevo_cliente.sender,
            content="Este es un mensaje de prueba.",
            # Los campos denormalizados se toman del cliente
            email_cliente=nuevo_cliente.email_cliente,
            ftp_directorio=nuevo_cliente.ftp_directorio
        )
        db.add(nuevo_sms)
        db.commit()
        db.refresh(nuevo_sms)
        log.info(f"SMS añadido: {nuevo_sms}")

        # 3. Consultar datos
        log.info("\nConsultando el cliente y sus SMS...")
        cliente_consultado = db.query(Cliente).filter(Cliente.sender == "+15551234567").first()
        if cliente_consultado:
            log.info(f"Cliente encontrado: {cliente_consultado.email_cliente}")
            # Gracias a la relación, podemos acceder a los SMS fácilmente
            for sms in cliente_consultado.sms_entrantes:
                log.info(f"  -> SMS ID: {sms.message_id}, Estado: {sms.status}, Contenido: '{sms.content[:20]}...'")

    except Exception as e:
        log.info(f"Ha ocurrido un error: {e}")
        db.rollback() # Revertir cambios si algo sale mal
    finally:
        db.close() # Asegurarse de cerrar la sesión
        log.info("\n--- Ejemplo de uso finalizado ---")


if __name__ == "__main__":
    # Esta parte se ejecuta solo cuando corres 'python main.py'
    inicializar_aplicacion()
    ejemplo_de_uso()