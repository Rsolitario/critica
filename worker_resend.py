import pika
import json
import logging
import time
import os

import requests
from sqlalchemy.orm import Session
from dotenv import load_dotenv
load_dotenv()

# Asegúrate de que este worker pueda acceder a estos archivos.
from models.clients import SmsIncoming

# --- Configuración de Logs ---
from setupLog import setup_logging

# --- Configuración ---
from productorRabbitmq import (
    RABBITMQ_HOST,
    SMS_RESEND_QUEUE,
    rabbitmq_connection,
    rabbitmq_channel,
)

from database import get_db

setup_logging()
logger = logging.getLogger(__name__)

# --- Configuración para sms api ---
SMS_API_CONFIG = {
    "api_url": os.getenv("API_URL_SMS_API"),  # Reemplazar con la URL real
    "username": os.getenv("USERNAME_SMS_API"),  # Reemplazar
    "password": os.getenv("PASSWORD_SMS_API"),  # Reemplazar
    "dlr_mask": 19,
    "dlr_url": os.getenv("DLR_URL") + "/sms_es_connector/webhook/dlr",
    "dcs": "gsm",
    "use_flash": False,
    "use_validate_period": True,
    "validate_period_minutes": 1440,
}

RC_THROTTLING_ERROR = 105  # Código de error específico de la API

CERTIFICACION_PDF_QUEUE = (
    "Certificacion_PDF"  # Cola a la que se publican las nuevas tareas
)

class StandaloneSmsEsClient:
    """
    Cliente de API para interactuar con el servicio de SMS.es.
    Esta versión está adaptada para ser usada fuera de Odoo.
    """
    def __init__(self, config):
        """
        Inicializa el cliente con un diccionario de configuración.
        :param config: Diccionario con los parámetros de la API.
        """
        self.api_url = config.get("api_url")
        self.username = config.get("username")
        self.password = config.get("password")
        self.dlr_mask = config.get("dlr_mask", 19)
        self.dlr_url = config.get("dlr_url")
        self.dcs = config.get("dcs", "gsm")
        self.use_flash = config.get("use_flash", False)
        self.use_validate_period = config.get("use_validate_period", True)
        self.validate_period_minutes = config.get("validate_period_minutes", 1440)

        if not all([self.api_url, self.username, self.password]):
            raise ValueError("La configuración de la API de SMS (URL, usuario, contraseña) no está completa.")

    def _build_payload(self, message_data):
        """Construye el payload para la API de SMS."""
        payload = {
            "type": "text",
            "auth": {"username": self.username, "password": self.password},
            "sender": message_data["sender"],
            "receiver": message_data["receiver"].lstrip("+"),
            "text": message_data["text"],
            "custom": {"db_message_id": message_data["db_message_id"]},
        }

        if self.dlr_mask and self.dlr_url:
            payload["dlrMask"] = self.dlr_mask
            payload["dlrUrl"] = self.dlr_url
        if self.use_flash:
            payload["flash"] = True
        if self.use_validate_period:
            payload["validatePeriodMinutes"] = self.validate_period_minutes
        
        payload["dcs"] = self.dcs
        return payload

    def send_sms(self, message_data, max_retries=3):
        """
        Envía un SMS, gestionando la construcción del payload y la lógica de reintentos.
        """
        try:
            payload = self._build_payload(message_data)
            payload_for_log = payload.copy()
            # Ocultar contraseña en logs
            # if payload_for_log.get("auth"):
            #     payload_for_log['auth']['password'] = '********'
            logger.info("Enviando SMS. Payload: %s", json.dumps(payload_for_log))
        except Exception as e:
            logger.error("Error construyendo el payload del SMS: %s", e)
            return {"status": "failed", "error": {"code": -1, "message": f"Error de payload: {e}"}}

        attempts = 0
        while attempts < max_retries:
            attempts += 1
            try:
                headers = {"Content-Type": "application/json; charset=utf-8"}
                response = requests.post(
                    self.api_url,
                    data=json.dumps(payload).encode("utf-8"),
                    headers=headers,
                    timeout=20,
                )

                if response.status_code == 202:
                    logger.info("SMS aceptado por la API. Respuesta: %s", response.text)
                    return {"status": "success", "data": response.json()} #{"status": "success", "data": {"msgid": "9856", "numParts": 1}}
                
                elif response.status_code == 420:
                    error_data = response.json().get("error", {})
                    if error_data.get("code") == RC_THROTTLING_ERROR:
                        logger.info("Error de throttling (105). Reintentando en 1 segundo...")
                        time.sleep(1)
                        continue
                    else:
                        logger.warning("SMS rechazado (420) por la API: %s", response.text)
                        return {"status": "failed", "error": error_data}
                
                elif 500 <= response.status_code < 600:
                    logger.error(f"Error del servidor de la API ({response.status_code}). Reintentando en 1 minuto...")
                    time.sleep(60)
                    continue
                
                else:
                    logger.error(f"Respuesta inesperada de la API. Código: {response.status_code}, Respuesta: {response.text}")
                    return {"status": "failed", "error": {"code": response.status_code, "message": response.text}}

            except requests.exceptions.RequestException as e:
                logger.error(f"Error de conexión con la API de SMS: {e}. Reintentando en 1 minuto...")
                if attempts < max_retries:
                    time.sleep(60)
                continue
        
        logger.error(f"El envío del SMS ha fallado después de {max_retries} intentos.")
        return {"status": "failed", "error": {"code": -1, "message": f"Falló después de {max_retries} reintentos."}}


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


def callback(ch, method, properties, body):
    """
    Función que se ejecuta por cada mensaje consumido.
    """
    logger.info(f"Mensaje recibido: {body.decode()}")
    db: Session = next(get_db())
    task_data = json.loads(body)
    db_message_id = task_data.get("db_message_id")

    if not db_message_id:
        logger.error("Mensaje inválido, falta 'db_message_id'. Descartando.")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    try:
        # Buscar el SMS en la base de datos
        sms = (
            db.query(SmsIncoming)
            .filter(SmsIncoming.message_id == db_message_id)
            .first()
        )

        if not sms:
            logger.error(
                f"No se encontró el SMS con id '{db_message_id}' en la BBDD. Descartando mensaje."
            )
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        logger.info(
            f"Procesando reenvío para SMS id '{db_message_id}' al sender '{sms.sender}'."
        )

        # 1. Utilizar 'requests' para reenviar el mensaje
        sms_client = StandaloneSmsEsClient(SMS_API_CONFIG)
        message_data = {
            "sender": sms.sender,
            "receiver": sms.receiver,
            "text": sms.content,
            "db_message_id": sms.message_id,
        }
        result = sms_client.send_sms(message_data)
        print(result)
        response_data = result.get("data", {}) \
            if result["status"] == "success" else {}
        
        logger.info(f"Respuesta del endpoint externo: {response_data}")
        
        # 2. Si la respuesta indica "DELIVERED", actualizar BBDD
        if result["status"] == "success":
            logger.info(
                f"El mensaje '{db_message_id}' fue aceptado por la API para su entrega"
            )
            sms.status = "SENDING"
            sms.provider_id = response_data.get("msgid")
            sms.num_parts = response_data.get("numParts")
            db.commit()
            logger.info(
                f"Estado del mensaje '{db_message_id}' actualizado 'SENDING' en la BBDD."
            )

            # 3. Encolar nueva tarea en "Certificación_PDF"
            #publish_to_pdf_queue(db_message_id=sms.message_id)

            # Confirmar que el mensaje fue procesado exitosamente
            ch.basic_ack(delivery_tag=method.delivery_tag)
        else:
            error_info = result.get("error", {})
            logger.warning(
                f"La entrega falló para el mensaje '{db_message_id}'. Razón: {error_info}. Marcando como fallido."
            )
            sms.status = "sent_failed"
            db.commit()
            # Se hace ACK porque el fallo fue una respuesta controlada del endpoint, no un error del sistema.
            # No se debe reintentar indefinidamente. Para reintentos, se necesitaría un sistema de 'dead-letter queue'.
            ch.basic_ack(delivery_tag=method.delivery_tag)

    except requests.exceptions.RequestException as e:
        logger.error(
            f"Error de red al contactar el endpoint externo para SMS id '{db_message_id}': {e}"
        )
        # NO hacemos ACK. El mensaje será re-entregado por RabbitMQ para un nuevo intento.
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
    except Exception as e:
        logger.error(
            f"Error inesperado procesando el mensaje id '{db_message_id}': {e}"
        )
        db.rollback()
        # NO hacemos ACK, pero evitamos re-encolarlo para prevenir bucles de envenenamiento.
        # Idealmente, esto iría a una 'dead-letter-queue'.
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    finally:
        db.close()


def main():
    """
    Función principal que inicia la conexión y el consumo de mensajes.
    """
    global rabbitmq_connection, rabbitmq_channel
    while True:
        try:
            logger.info("Iniciando worker de reenvío de SMS...")
            connection_params = pika.ConnectionParameters(
                host=RABBITMQ_HOST,
                heartbeat=600,
                blocked_connection_timeout=300,
            )
            rabbitmq_connection = pika.BlockingConnection(connection_params)
            rabbitmq_channel = rabbitmq_connection.channel()

            # Asegurar que ambas colas existen y son durables
            rabbitmq_channel.queue_declare(
                queue=SMS_RESEND_QUEUE, durable=True
            )
            rabbitmq_channel.queue_declare(
                queue=CERTIFICACION_PDF_QUEUE, durable=True
            )

            # Procesar un mensaje a la vez
            rabbitmq_channel.basic_qos(prefetch_count=1)
            rabbitmq_channel.basic_consume(
                queue=SMS_RESEND_QUEUE, on_message_callback=callback
            )

            logger.info(
                f"[*] Esperando mensajes en la cola '{SMS_RESEND_QUEUE}'. Para salir presione CTRL+C"
            )
            rabbitmq_channel.start_consuming()

        except pika.exceptions.AMQPConnectionError as e:
            logger.error(
                f"Error de conexión con RabbitMQ: {e}. Reintentando en 5 segundos..."
            )
            time.sleep(5)
        except KeyboardInterrupt:
            logger.info("Deteniendo el worker...")
            if rabbitmq_connection and rabbitmq_connection.is_open:
                rabbitmq_connection.close()
            break
        except Exception as e:
            logger.error(
                f"Ocurrió un error inesperado en el worker: {e}. Reiniciando..."
            )
            time.sleep(5)


if __name__ == "__main__":
    main()
