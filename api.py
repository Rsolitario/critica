import logging
import uuid
from datetime import datetime
from fastapi import FastAPI, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy.orm import Session

from database import get_db
# Importaciones de la configuración de la base de datos y modelos del paso anterior
# (Asegúrate de que este archivo esté en el mismo directorio o sea accesible)
from database import (
    SessionLocal,
    engine,
    Base,
    create_db_and_tables as init_db,
)
from models import Cliente, SmsIncoming
from setupLog import setup_logging
from productorRabbitmq import (
    connect_to_rabbitmq,
    close_rabbitmq_connection,
    publish_to_resend_queue,
)

# --- Configuración de Logs ---
setup_logging()
logger = logging.getLogger(__name__)

# --- Inicialización de la Base de Datos ---
# Crea las tablas si no existen al iniciar la aplicación
try:
    init_db()
    logger.info("Base de datos verificada e inicializada.")
except Exception as e:
    logger.error(f"No se pudo inicializar la base de datos: {e}")

# --- Creación de la Aplicación FastAPI ---
app = FastAPI(
    title="SMS Processing Service",
    description="Servicio para recibir y procesar mensajes SMS.",
    on_startup=[connect_to_rabbitmq],
    on_shutdown=[close_rabbitmq_connection],
)


# --- Modelo Pydantic para la Validación de Entrada ---
class SmsInput(BaseModel):
    """
    Modelo de datos para la validación de la carga útil del POST.
    """
    
    sender: str
    receiver: str
    content: str
    timestamp: datetime

# --- Endpoint para Recibir SMS ---
@app.post("/receive_sms", status_code=status.HTTP_202_ACCEPTED)
async def receive_sms(sms_data: SmsInput, db: Session = Depends(get_db)):
    """
    Endpoint para recibir un SMS de un proveedor externo.

    - Valida los datos de entrada.
    - Busca la información del cliente (`sender`).
    - Registra el mensaje entrante con estado 'pending'.
    - Devuelve una respuesta inmediata para desacoplar el procesamiento posterior.
    """
    message_id = str(uuid.uuid4().hex)  # Generar un ID único para el mensaje
    logger.info(
        f"Recibido nuevo SMS de '{sms_data.sender}' con message_id: {message_id}"
    )

    # 1. Consultar la tabla clientes para obtener los datos asociados al sender
    logger.info(f"Buscando cliente con sender: {sms_data.sender}")
    cliente = (
        db.query(Cliente).filter(Cliente.sender == sms_data.sender).first()
    )

    if not cliente:
        logger.warning(
            f"Cliente no encontrado para el sender: {sms_data.sender}"
        )
        # Si el cliente no existe, no se puede procesar el mensaje.
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Cliente con sender '{sms_data.sender}' no encontrado.",
        )

    logger.info(
        f"Cliente encontrado: Email='{cliente.email_cliente}', FTP='{cliente.ftp_directorio}'"
    )

    # 2. Registrar inmediatamente el mensaje en sms_incoming con status "pending"
    nuevo_sms = SmsIncoming(
        message_id=message_id,
        sender=sms_data.sender,
        receiver=sms_data.receiver,
        content=sms_data.content,
        timestamp_received=sms_data.timestamp,
        status="pending",  # Estado inicial
        email_cliente=cliente.email_cliente,
        ftp_directorio=cliente.ftp_directorio,
    )

    try:
        db.add(nuevo_sms)
        db.commit()
        db.refresh(nuevo_sms)
        logger.info(
            f"SMS con message_id '{message_id}' registrado en la BBDD con estado 'pending'."
        )
    except Exception as e:
        db.rollback()
        logger.error(f"Error al registrar el SMS en la base de datos: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error interno al guardar el mensaje.",
        )

    # 3. Productor RabbitMQ
    try:
        publish_to_resend_queue(
            message_id=nuevo_sms.message_id,
            email_cliente=cliente.email_cliente,
            ftp_directorio=cliente.ftp_directorio,
        )
    except HTTPException as e:
        logger.critical(f"FALLO CRITICO: SMS con ID {nuevo_sms.message_id} guardado pero no encolado.")

    # 4. Devolver una respuesta HTTP 202 inmediata al proveedor
    return {
        "status": "success",
        "message": "SMS recibido y en cola para procesamiento.",
    }


# --- Para probar, puedes añadir datos de ejemplo a la BBDD ---
def add_example_client(db: Session):
    """Añade un cliente de ejemplo si no existe."""
    example_sender = "+15551234567"
    client = db.query(Cliente).filter(Cliente.sender == example_sender).first()
    if not client:
        new_client = Cliente(
            sender=example_sender,
            email_cliente="cliente@ejemplo.com",
            ftp_directorio="/ftp/path/for/client123",
        )
        db.add(new_client)
        db.commit()
        logger.info(
            f"Cliente de ejemplo '{example_sender}' añadido a la base de datos."
        )


if __name__ == "__main__":
    # Este bloque es útil para desarrollo y pruebas.
    # No se ejecutará cuando se use uvicorn para iniciar la aplicación.
    with SessionLocal() as db:
        add_example_client(db)

    # Para ejecutar: uvicorn nombre_del_archivo:app --reload
    # Ejemplo: uvicorn main:app --reload
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
