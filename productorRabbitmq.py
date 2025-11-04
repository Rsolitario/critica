import pika
import logging
import json

from fastapi import HTTPException

from setupLog import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

# --- Configuración de RabbitMQ ---
RABBITMQ_HOST = "localhost"
SMS_RESEND_QUEUE = "sms_resend_queue"

# --- Variables globales para la Conexión de RabbitMQ ---
rabbitmq_connection = None
rabbitmq_channel = None


# --- Funciones del Ciclo de Vida de FastAPI para RabbitMQ ---
def connect_to_rabbitmq():
    """Establece la conexión con RabbitMQ y declara la cola."""
    global rabbitmq_connection, rabbitmq_channel
    try:
        logger.info(f"Conectando a RabbitMQ en {RABBITMQ_HOST}...")
        parameters = pika.ConnectionParameters(host=RABBITMQ_HOST)
        rabbitmq_connection = pika.BlockingConnection(parameters)
        rabbitmq_channel = rabbitmq_connection.channel()

        # Declarar la cola como 'durable' para que sobreviva a reinicios del broker
        rabbitmq_channel.queue_declare(queue=SMS_RESEND_QUEUE, durable=True)
        logger.info(
            f"Conexión a RabbitMQ exitosa. Cola '{SMS_RESEND_QUEUE}' asegurada."
        )
    except pika.exceptions.AMQPConnectionError as e:
        logger.error(f"Error al conectar con RabbitMQ: {e}")
        # En un escenario real, se podrían implementar reintentos de conexión
        raise


def close_rabbitmq_connection():
    """Cierra la conexión con RabbitMQ de forma segura."""
    global rabbitmq_connection
    if rabbitmq_connection and rabbitmq_connection.is_open:
        logger.info("Cerrando la conexión con RabbitMQ...")
        rabbitmq_connection.close()
        logger.info("Conexión con RabbitMQ cerrada.")


# --- Productor de RabbitMQ ---
def publish_to_resend_queue(
    message_id: int, email_cliente: str, ftp_directorio: str
):
    """
    Publica un mensaje en la cola SMS_Resend.
    """
    if not rabbitmq_channel or not rabbitmq_channel.is_open:
        logger.error(
            "No se puede publicar el mensaje: el canal de RabbitMQ no está disponible."
        )
        # Aquí se podría lanzar una excepción o implementar una política de reintentos
        raise HTTPException(
            status_code=503, detail="Servicio de mensajería no disponible."
        )

    message_body = {
        "db_message_id": message_id,  # Usamos el ID de la BBDD como identificador único interno
        "email_cliente": email_cliente,
        "ftp_directorio": ftp_directorio,
    }

    try:
        rabbitmq_channel.basic_publish(
            exchange="",
            routing_key=SMS_RESEND_QUEUE,
            body=json.dumps(message_body).encode("utf-8"),
            properties=pika.BasicProperties(
                delivery_mode=2,  # Hacer el mensaje persistente
            ),
        )
        logger.info(
            f"Mensaje para db_message_id '{message_id}' enviado a la cola '{SMS_RESEND_QUEUE}'."
        )
    except Exception as e:
        logger.error(
            f"Error al publicar mensaje en RabbitMQ para db_message_id '{message_id}': {e}"
        )
        # Este es un punto crítico: el SMS está en BBDD pero no en la cola.
        # Se requiere una estrategia de recuperación (ej. un proceso batch que revise 'pending' sin encolar).
        raise HTTPException(
            status_code=500,
            detail="Error al encolar la tarea de procesamiento.",
        )
