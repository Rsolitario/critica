# worker_distribution.py (versión con python-dotenv)

import pika
import json
import logging
import time
import os
from dotenv import load_dotenv  # <-- 1. IMPORTAR LA LIBRERÍA

# --- Módulos de Distribución ---
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.header import Header
import ftplib
try:
    import pysftp
    SFTP_AVAILABLE = True
except ImportError:
    SFTP_AVAILABLE = False

# --- Cargar variables de entorno desde el archivo .env ---
load_dotenv()  # <-- 2. LLAMAR A LA FUNCIÓN PARA CARGAR EL ARCHIVO

# --- Configuración de Logs ---
from setupLog import setup_logging
setup_logging()
logger = logging.getLogger(__name__)

# --- Configuración del Worker (ahora se lee desde .env) ---
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST")
DISTRIBUCION_PDF_QUEUE = "Distribucion_PDF"

# --- Configuración de Email (SMTP) ---
SMTP_HOST = os.getenv("SMTP_HOST")
SMTP_PORT = int(os.getenv("SMTP_PORT", 587))
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASS = os.getenv("SMTP_PASS")
SMTP_SENDER = os.getenv("SMTP_SENDER")

# --- Configuración de Almacenamiento Remoto (FTP/SFTP) ---
REMOTE_STORAGE_TYPE = os.getenv("REMOTE_STORAGE_TYPE", "SFTP").upper()
REMOTE_HOST = os.getenv("REMOTE_HOST")
REMOTE_USER = os.getenv("REMOTE_USER")
REMOTE_PASS = os.getenv("REMOTE_PASS")
REMOTE_PORT = int(os.getenv("REMOTE_PORT", 22))

# --- Funciones de Distribución (Sin cambios en su lógica interna) ---

def send_email_with_attachment(recipient_email: str, file_path: str):
    # (El código de esta función es idéntico al anterior)
    if not all([SMTP_HOST, SMTP_USER, SMTP_PASS]):
        logger.warning("Variables de entorno de SMTP no configuradas. Saltando envío de correo.")
        return
    logger.info(f"Preparando correo para {recipient_email}...")
    try:
        msg = MIMEMultipart()
        msg["From"] = SMTP_SENDER
        msg["To"] = recipient_email
        msg["Subject"] = Header("Certificado de Entrega de SMS", "utf-8")
        msg.attach(MIMEText("Adjunto encontrará el certificado de entrega de su comunicación reciente.", "plain", "utf-8"))
        with open(file_path, "rb") as attachment:
            part = MIMEApplication(attachment.read(), Name=os.path.basename(file_path))
        part['Content-Disposition'] = f'attachment; filename="{os.path.basename(file_path)}"'
        msg.attach(part)
        server = smtplib.SMTP(SMTP_HOST, SMTP_PORT)
        server.starttls()
        server.login(SMTP_USER, SMTP_PASS)
        server.send_message(msg)
        server.quit()
        logger.info(f"Correo enviado exitosamente a {recipient_email}.")
    except Exception as e:
        logger.error(f"Fallo al enviar correo a {recipient_email}: {e}")
        raise

def upload_file_to_remote(local_path: str, remote_dir: str):
    # (El código de esta función es idéntico al anterior)
    if not all([REMOTE_HOST, REMOTE_USER, REMOTE_PASS]):
        logger.warning("Variables de entorno de almacenamiento remoto no configuradas. Saltando subida de archivo.")
        return
    if REMOTE_STORAGE_TYPE == "SFTP":
        if not SFTP_AVAILABLE:
            logger.error("Se configuró SFTP pero 'pysftp' no está instalado.")
            raise ImportError("pysftp no está instalado.")
        upload_sftp(local_path, remote_dir)
    elif REMOTE_STORAGE_TYPE == "FTP":
        upload_ftp(local_path, remote_dir)
    else:
        logger.error(f"Tipo de almacenamiento '{REMOTE_STORAGE_TYPE}' no soportado.")

def upload_ftp(local_path, remote_dir):
    # (El código de esta función es idéntico al anterior)
    logger.info(f"Iniciando subida FTP a {REMOTE_HOST}...")
    try:
        with ftplib.FTP(REMOTE_HOST, REMOTE_USER, REMOTE_PASS) as ftp:
            for part in remote_dir.strip('/').split('/'):
                try: ftp.cwd(part)
                except ftplib.error_perm: ftp.mkd(part); ftp.cwd(part)
            with open(local_path, 'rb') as f:
                ftp.storbinary(f'STOR {os.path.basename(local_path)}', f)
        logger.info("Archivo subido exitosamente vía FTP.")
    except Exception as e:
        logger.error(f"Fallo en la subida FTP: {e}")
        raise

def upload_sftp(local_path, remote_dir):
    # (El código de esta función es idéntico al anterior)
    logger.info(f"Iniciando subida SFTP a {REMOTE_HOST}:{REMOTE_PORT}...")
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None # ¡No usar en producción!
    try:
        with pysftp.Connection(host=REMOTE_HOST, username=REMOTE_USER, password=REMOTE_PASS, port=REMOTE_PORT, cnopts=cnopts) as sftp:
            sftp.makedirs(remote_dir)
            remote_path = os.path.join(remote_dir, os.path.basename(local_path))
            sftp.put(local_path, remote_path)
        logger.info("Archivo subido exitosamente vía SFTP.")
    except Exception as e:
        logger.error(f"Fallo en la subida SFTP: {e}")
        raise

# --- Función Principal del Worker y `main` (Sin cambios en su lógica interna) ---

def callback(ch, method, properties, body):
    # (El código de esta función es idéntico al anterior)
    logger.info(f"Mensaje de distribución recibido: {body.decode()}")
    task_data = json.loads(body)
    final_pdf_path = task_data.get("final_pdf_path")
    recipient_email = task_data.get("recipient_email")
    remote_dir = task_data.get("remote_dir")
    sms_id = task_data.get("sms_id")
    if not all([final_pdf_path, recipient_email, remote_dir]):
        logger.error(f"Mensaje inválido recibido: {task_data}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return
    if not os.path.exists(final_pdf_path):
        logger.error(f"El archivo PDF '{final_pdf_path}' no fue encontrado.")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return
    try:
        send_email_with_attachment(recipient_email, final_pdf_path)
        upload_file_to_remote(final_pdf_path, remote_dir)
        logger.info(f"Distribución para SMS id '{sms_id}' completada.")
    except Exception as delivery_error:
        logger.critical(f"FALLO DE ENTREGA para certificado '{final_pdf_path}': {delivery_error}")
    finally:
        ch.basic_ack(delivery_tag=method.delivery_tag)

def main():
    while True:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            channel = connection.channel()
            channel.queue_declare(queue=DISTRIBUCION_PDF_QUEUE, durable=True)
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(queue=DISTRIBUCION_PDF_QUEUE, on_message_callback=callback)
            logger.info(f"[*] Worker de Distribución esperando mensajes en '{DISTRIBUCION_PDF_QUEUE}'.")
            channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            logger.error(f"Error de conexión con RabbitMQ: {e}. Reintentando en 5 segundos...")
            time.sleep(5)
        except KeyboardInterrupt:
            logger.info("Deteniendo el worker.")
            break
        except Exception as e:
            logger.critical(f"Error inesperado: {e}")
            break

if __name__ == "__main__":
    main()