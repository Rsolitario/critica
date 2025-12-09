import os
import logging
import uuid
import pika
import json
from datetime import datetime
import requests
from fastapi import FastAPI, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session
from contextlib import asynccontextmanager
from typing import Dict, Any, Optional
from dotenv import load_dotenv

from database import get_db

# Importaciones de la configuración de la base de datos y modelos del paso anterior
# (Asegúrate de que este archivo esté en el mismo directorio o sea accesible)
from database import (
    SessionLocal,
    engine,
    Base,
    create_db_and_tables as init_db,
)
from models.clients import Cliente, SmsIncoming
from setupLog import setup_logging
from productorRabbitmq import (
    connect_to_rabbitmq,
    close_rabbitmq_connection,
    publish_to_resend_queue,
    RABBITMQ_HOST,
    rabbitmq_channel,
)

# Importaciones relacionadas con la autenticación y usuarios
from models.users import User, UserRole
import utils.auth as auth
from utils import crud
from schemas.user import UserCreate
from controllers.users import router as router_users
from controllers.clients import router as router_clients

# productor worker pdf
from worker_resend import CERTIFICACION_PDF_QUEUE

load_dotenv()
# --- Configuración de Logs ---
setup_logging()
logger = logging.getLogger(__name__)


# --- Inicialización de la Base de Datos ---
# Crea las tablas si no existen al iniciar la aplicación
@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        init_db()
        logger.info("Base de datos verificada e inicializada.")
    except Exception as e:
        logger.error(f"No se pudo inicializar la base de datos: {e}")

    # --- creando un usuario admin si no existe ---
    db = SessionLocal()
    try:
        admin_user = crud.get_user_by_username(db, username="admin")
        if not admin_user:
            admin_user = UserCreate(
                username=os.getenv('ADMIN_USERNAME'), password=os.getenv('ADMIN_PASSWORD'), role=UserRole.ADMIN
            )
            crud.create_user(db, admin_user)
            logger.info("Usuario administrador creado con username 'admin'.")
        add_example_client(db)
    except Exception as e:
        logger.error(f"Error al crear el usuario administrador: {e}")
    finally:
        db.close()
    yield


# --- Creación de la Aplicación FastAPI ---
app = FastAPI(
    title="SMS Processing Service",
    description="Servicio para recibir y procesar mensajes SMS.",
    # on_startup=[connect_to_rabbitmq],
    # on_shutdown=[close_rabbitmq_connection],
    lifespan=lifespan,  # ejecución al inicio del programa
)

app.include_router(router_users)
app.include_router(router_clients)


# --- Modelo Pydantic para la Validación de Entrada ---
class SmsInput(BaseModel):
    """
    Modelo de datos para la validación de la carga útil del POST.
    """

    sender_id: str
    recipients: str
    message: str


# --- Modelo para la respuesta dlr ---
class DLRWebhookPayload(BaseModel):
    msgId: str = Field(
        ...,
        description="ID del mensaje del proveedor. Clave para la conciliación.",
    )
    event: str = Field(
        ..., description="Estado de entrega (ej: DELIVERED, UNDELIVERED)"
    )
    errorCode: Optional[int] = Field(
        None, description="Código de error, 0 si no hay error."
    )
    errorMessage: Optional[str] = Field(
        None, description="Mensaje de error asociado, si aplica."
    )
    numParts: int
    partNum: int


def publish_to_pdf_queue(db_message_id: int):
    """
    Publica un mensaje en la cola Certificacion_PDF.
    """
    if not rabbitmq_channel or not rabbitmq_channel.is_open:
        logger.error(
            "El canal de RabbitMQ no está disponible para publicar en la cola PDF."
        )
        # Esto es un error crítico, el estado se actualizó pero la siguiente tarea no se encoló.
        # Requiere monitoreo y posible intervención manual o un proceso de conciliación.
        return

    message_body = {"db_message_id": db_message_id}
    try:
        rabbitmq_channel.basic_publish(
            exchange="",
            routing_key=CERTIFICACION_PDF_QUEUE,
            body=json.dumps(message_body).encode("utf-8"),
            properties=pika.BasicProperties(
                delivery_mode=2
            ),  # Mensaje persistente
        )
        logger.info(
            f"Tarea para db_message_id '{db_message_id}' encolada en '{CERTIFICACION_PDF_QUEUE}'."
        )
    except Exception as e:
        logger.error(
            f"Error al publicar en la cola PDF para db_message_id '{db_message_id}': {e}"
        )


# --- Endpoint para Recibir SMS ---
@app.get("/receive_sms", status_code=status.HTTP_202_ACCEPTED)
async def receive_sms(
    sender_id: str,
    recipients: str,
    message: str,
    action: str,
    sub_account: str,
    sub_account_pass: str,
    db: Session = Depends(get_db)
):
    """
    Endpoint para recibir un SMS de un proveedor externo.

    - Valida los datos de entrada.
    - Busca la información del cliente (`sender`).
    - Registra el mensaje entrante con estado 'pending'.
    - Devuelve una respuesta inmediata para desacoplar el procesamiento posterior.
    """
    message_id = str(uuid.uuid4())  # Generar un ID único para el mensaje
    logger.info(
        f"Recibido nuevo SMS de '{sender_id}' con message_id: {message_id}"
    )

    # 1. Consultar la tabla clientes para obtener los datos asociados al sender
    logger.info(f"Buscando cliente con sender: {sender_id}")
    cliente = (
        db.query(Cliente).filter(Cliente.sender == sender_id).first()
    )

    if not cliente:
        logger.warning(
            f"Cliente no encontrado para el sender: {sender_id}"
        )
        # Si el cliente no existe, no se puede procesar el mensaje.
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Cliente con sender '{sender_id}' no encontrado.",
        )

    logger.info(
        f"Cliente encontrado: Email='{cliente.email_cliente}', FTP='{cliente.ftp_directorio}'"
    )

    # 2. Registrar inmediatamente el mensaje en sms_incoming con status "pending"
    nuevo_sms = SmsIncoming(
        message_id=message_id,
        sender=sender_id,
        receiver=recipients,
        content=message,
        timestamp_received=datetime.now(),
        status="pending",  # Estado inicial
        email_cliente=cliente.email_cliente,
        ftp_directorio=cliente.ftp_directorio,
        action=action,
        sub_account=sub_account,
        sub_account_pass=sub_account_pass
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
        logger.critical(
            f"FALLO CRITICO: SMS con ID {nuevo_sms.message_id} guardado pero no encolado."
        )

    response_data = {
        'errorCode': 0,
        'errorDescription': 'Ok',
        'sms': [[
            {
                'errorCode': 0,
                'id': nuevo_sms.message_id,
                'originatingAddress': nuevo_sms.sender,
                'destinationAddress': nuevo_sms.receiver,
            }
        ]],
        'messageCount': 1,
        'messageParts': 1
    }
    # 4. Devolver una respuesta
    return response_data


def respond_dlr_success(message_to_update: SmsIncoming, event: str):
    status_dlr_server = {
        "Delivered": 2,
        "Expired": 3,
        "Deleted": 4,
        "Undeliverd": 5,
        "Accepted": 6,
        "Invalid": 7,
        "Rejected": 8
    }
    status_dlr_response = {
        0: "Ok",
        1: "Invalid Credentials",
        3: "Invalid Query String Parameters",
        10: "Internal Server Error"
    }

    dlr_params = {
        'username': message_to_update.sub_account,
        'password': message_to_update.sub_account_pass,
        'sender': message_to_update.sender,
        'destination': message_to_update.receiver,
        'messageId': message_to_update.message_id,
        # Se usa un espacio normal
        'dateReceived': datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
        'description': event,
        # Puede ser un número o un string '2'
        'deliveryStatus': status_dlr_server.get(event.lower().capitalize())
    }

    try:
        # Respuesta actual Error! {"errorCode":1,"errorDescriptio>
        print(dlr_params)
        response = requests.get(
            "http://195.191.165.16:32006/HTTP/api/Vendor/DLRListener", params=dlr_params
        )
        if response.status_code != 200:
            logger.error(
                f"Error al enviar la respuesta DLR: {response.status_code} - {response.text}")
        else:
            json_response = response.json()
            logger.info(
                f"Respuesta DLR enviada exitosamente para message_id: {message_to_update.message_id}")
            if json_response.get("errorCode") == 0:
                logger.info(f"DLR aceptado por el servidor: {json_response}")
            else:
                logger.error(f"DLR rechazado por el servidor: {status_dlr_response.get(json_response.get("erroCode"))} {json_response.get("errorDescription")}")
                raise ValueError(f"DLR rechazado por el servidor: {status_dlr_response.get(json_response.get("erroCode"))} {json_response.get("errorDescription")}")
    except requests.RequestException as e:
        logger.error(f"Excepción al enviar la respuesta DLR: {e}")
        raise


@app.post(
    "/sms_es_connector/webhook/dlr",
    status_code=status.HTTP_200_OK,
    summary="Recibe Reportes de Entrega (DLR)",
    tags=["Webhooks"],
)
def receive_dlr_webhook(
    payload: DLRWebhookPayload, db: Session = Depends(get_db)
):
    """
    Este endpoint procesa los reportes de entrega (DLR) enviados por el proveedor de SMS.

    - **Procesamiento**: Recibe la solicitud POST y decodifica el cuerpo JSON.
    - **Conciliación**: Busca el mensaje en la base de datos usando el `msgId`.
    - **Actualización**: Actualiza el campo `status` del mensaje con el nuevo estado `event`.
    """
    print(payload)
    logger.info(f"DLR Webhook recibido para msgId: {payload.msgId}")

    # --- Lógica Mínima de la IA ---

    # 1. Conciliación: Buscar el mensaje original en la BD usando el msgId del DLR.
    #    Asumimos que guardaste el ID del proveedor en el campo `provider_id`.
    message_to_update = (
        db.query(SmsIncoming)
        .filter(SmsIncoming.provider_id == payload.msgId)
        .first()
    )

    # Si no se encuentra el mensaje, es un error.
    if not message_to_update:
        logger.warning(
            f"No se encontró ningún mensaje con provider_id (msgId): {payload.msgId}"
        )
        # Es importante lanzar un error para que el proveedor sepa que algo falló.
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Message with msgId '{payload.msgId}' not found.",
        )

    # 2. Actualización: Cambiar el estado del mensaje con el valor del campo 'event'.
    previous_status = message_to_update.status
    logger.info(
        f"Actualizando estado del mensaje {message_to_update.message_id} de '{message_to_update.status}' a '{payload.event}'"
    )
    message_to_update.status = payload.event

    # DLR al que envia el sms
    respond_dlr_success(message_to_update, payload.event)

    # Guardar los cambios en la base de datos.
    db.commit()
    db.refresh(message_to_update)

    if (
        message_to_update.status == "DELIVERED"
        and previous_status != "DELIVERED"
    ):
        logger.info(
            f"Mensaje {message_to_update.message_id} entregado exitosamente al destinatario, agregando a la cola de generación de PDF."
        )

        # agregar a la cola de mensaje para generar pdf
        logger.info("Iniciando publicación a la cola de RabbitMQ para PDF...")
        global rabbitmq_channel
        connection_params = pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            heartbeat=600,
            blocked_connection_timeout=300,
        )
        rabbitmq_connection = pika.BlockingConnection(connection_params)
        rabbitmq_channel = rabbitmq_connection.channel()
        rabbitmq_channel.queue_declare(
            queue=CERTIFICACION_PDF_QUEUE, durable=True
        )
        publish_to_pdf_queue(db_message_id=message_to_update.message_id)
        if rabbitmq_connection and rabbitmq_connection.is_open:
            rabbitmq_connection.close()

    # --- Respuesta Esperada del Servidor ---
    # Se devuelve una confirmación simple. Un status 200 es suficiente para que
    # el proveedor sepa que recibimos el DLR correctamente.
    return {
        "status": "success",
        "message": "DLR processed successfully",
        "processed_msgId": payload.msgId,
        "new_status": payload.event,
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


# if __name__ == "__main__":
    # Este bloque es útil para desarrollo y pruebas.
    # No se ejecutará cuando se use uvicorn para iniciar la aplicación.

    # Para ejecutar: uvicorn nombre_del_archivo:app --reload
    # Ejemplo: uvicorn main:app --reload
    # import uvicorn

    # uvicorn.run(app, host="0.0.0.0", port=7000)
