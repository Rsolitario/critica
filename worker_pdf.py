import pika
import json
import logging
import time
import os
import hashlib
from datetime import datetime
from sqlalchemy.orm import Session
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import Paragraph

# --- Nuevas importaciones para el sellado de tiempo ---
from pyhanko.pdf_utils.writer import PdfFileWriter
from pyhanko.pdf_utils.reader import PdfFileReader
from pyhanko.sign import signers, PdfSignatureMetadata
from pyhanko.sign.timestamps import HTTPTimeStamper
from pyhanko.pdf_utils.writer import PdfFileWriter
from pyhanko.pdf_utils.incremental_writer import IncrementalPdfFileWriter
from pyhanko.sign.fields import (
    append_signature_field,
    SigFieldSpec,
    VisibleSigSettings,
)

# Importaciones de la configuración de la base de datos y modelos
from database import get_db
from models import SmsIncoming

# --- Configuración de Logs ---
from setupLog import setup_logging

# --- Configuración del Worker ---
from productorRabbitmq import (
    RABBITMQ_HOST,
)

CERTIFICACION_PDF_QUEUE = "Certificacion_PDF"
DISTRIBUCION_PDF_QUEUE = "Distribucion_PDF"
PDF_OUTPUT_DIR = "certificados/tmp"  # Directorio para guardar los PDFs
# Directorio para guardar los PDFs sellados
PDF_FINAL_DIR = "certificados/sellados"
LOGO_FILE = "logo.png"  # Asegúrate de que este archivo exista

# --- Configuración del Certificado Digital ---
# Se asume un certificado en formato PKCS#12 (.p12 o .pfx)
PKCS12_PATH = (
    "paquete.p12"  # ¡IMPORTANTE! Coloca aquí la ruta a tu certificado
)
PKCS12_PASSPHRASE = "1234"  # ¡IMPORTANTE! La contraseña de tu certificado

# URL de la autoridad de sellado de tiempo (TSA)
TSA_URL = "http://timestamp.digicert.com"

setup_logging()
logger = logging.getLogger(__name__)

# Crear el directorio de salida si no existe
os.makedirs(PDF_OUTPUT_DIR, exist_ok=True)
os.makedirs(PDF_FINAL_DIR, exist_ok=True)


def create_certification_pdf(sms_data: SmsIncoming):
    """
    Genera un PDF de certificación utilizando reportlab.
    """
    file_path = os.path.join(
        PDF_OUTPUT_DIR, f"certificado_{sms_data.message_id}.pdf"
    )
    logger.info(
        f"Creando PDF para el mensaje id '{sms_data.message_id}' en: {file_path}"
    )

    try:
        c = canvas.Canvas(file_path, pagesize=letter)
        width, height = letter

        # --- Cabecera y Logo ---
        if os.path.exists(LOGO_FILE):
            c.drawImage(
                LOGO_FILE,
                x=inch,
                y=height - 1.5 * inch,
                width=1.5 * inch,
                height=1 * inch,
                preserveAspectRatio=True,
                mask="auto",
            )
        else:
            logger.warning(f"Archivo de logo '{LOGO_FILE}' no encontrado.")
            c.drawString(inch, height - inch, "[Logo de la Empresa]")

        c.setFont("Helvetica-Bold", 18)
        c.drawCentredString(
            width / 2.0, height - 1.25 * inch, "Certificado de Entrega de SMS"
        )

        # --- Línea de separación ---
        c.line(inch, height - 1.8 * inch, width - inch, height - 1.8 * inch)

        # --- Contenido del Certificado ---
        c.setFont("Helvetica", 12)
        text_y_position = height - 2.5 * inch

        styles = getSampleStyleSheet()
        style_body = styles["BodyText"]
        style_body.leading = 18  # Espacio entre líneas

        def draw_detail(label, value, y_pos):
            p_label = Paragraph(f"<b>{label}:</b>", style_body)
            p_value = Paragraph(str(value), style_body)
            p_label.wrapOn(c, 2 * inch, inch)
            p_label.drawOn(c, inch, y_pos)
            p_value.wrapOn(c, width - 4.5 * inch, inch)
            p_value.drawOn(c, inch + 2.2 * inch, y_pos)
            return y_pos - (p_value.height + 10)  # Retorna la nueva posición Y

        # Detalles del mensaje
        text_y_position = draw_detail(
            "ID de Certificación (Interno)",
            sms_data.message_id,
            text_y_position,
        )
        text_y_position = draw_detail(
            "ID del Proveedor", sms_data.provider_id, text_y_position
        )
        text_y_position = draw_detail(
            "Número de Partes", sms_data.num_parts, text_y_position
        )
        text_y_position = draw_detail(
            "Remitente (Sender)", sms_data.sender, text_y_position
        )
        text_y_position = draw_detail(
            "Fecha de Recepción",
            sms_data.timestamp_received.strftime("%Y-%m-%d %H:%M:%S UTC"),
            text_y_position,
        )
        text_y_position = draw_detail(
            "Fecha de Certificación",
            datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
            text_y_position,
        )
        text_y_position = draw_detail(
            "Estado Confirmado", sms_data.status.upper(), text_y_position
        )

        # Contenido del mensaje (maneja texto largo con Paragraph)
        c.drawString(inch, text_y_position, "Contenido del Mensaje:")
        text_y_position -= 20
        content_paragraph = Paragraph(sms_data.content, style_body)
        # Ancho máximo, altura máxima
        content_paragraph.wrapOn(c, width - 2 * inch, height)
        content_paragraph.drawOn(
            c, inch, text_y_position - content_paragraph.height
        )

        c.save()
        logger.info(f"PDF guardado exitosamente en '{file_path}'.")
        return file_path

    except Exception as e:
        logger.error(
            f"No se pudo generar el PDF para el mensaje id '{sms_data.message_id}': {e}"
        )
        raise


def sign_and_store_pdf(temp_pdf_path: str, sms_id: int) -> str:
    """
    Aplica una firma digital PAdES-B-LT y guarda el PDF en la ubicación final.
    """
    logger.info(f"Iniciando firma digital para '{temp_pdf_path}'.")

    # 1. Cargar el certificado y la clave privada del firmante
    try:
        signer = signers.SimpleSigner.load_pkcs12(
            pfx_file=PKCS12_PATH,
            passphrase=(
                PKCS12_PASSPHRASE.encode("utf-8")
                if PKCS12_PASSPHRASE
                else None
            ),
        )
    except FileNotFoundError:
        logger.error(
            f"¡ERROR CRÍTICO! No se encontró el archivo del certificado en: {PKCS12_PATH}"
        )
        raise
    except Exception as e:
        logger.error(
            f"¡ERROR CRÍTICO! No se pudo cargar el certificado. Revisa la ruta y la contraseña. Error: {e}"
        )
        raise

    # 2. Preparar el firmante PAdES con el sello de tiempo (PAdES-B-LT)
    pdf_signer = signers.PdfSigner(
        PdfSignatureMetadata(
            field_name="FirmaEmpresa",
            reason="Certificación de entrega de comunicación",
            location="Servidor Central",
        ),
        signer=signer,
        # Incrusta el sello de tiempo en la firma
        timestamper=HTTPTimeStamper(url=TSA_URL),
    )

    # 3. Aplicar la firma al documento
    final_pdf_path = os.path.join(
        PDF_FINAL_DIR, f"certificado_final_{sms_id}.pdf"
    )
    with open(temp_pdf_path, "rb+") as doc_in:
        writer = IncrementalPdfFileWriter(doc_in)

        sig_field_spec = SigFieldSpec(
            "FirmaEmpresa",
            box=(50, 50, 200, 100),
            visible_sig_settings=VisibleSigSettings(rotate_with_page=True),
        )
        append_signature_field(writer, sig_field_spec)
        writer.write_in_place()

        doc_in.seek(0)

        with open(final_pdf_path, "wb") as doc_out:
            # PyHanko se encarga de todo el proceso de firma y sellado
            pdf_signer.sign_pdf(writer, doc_out)

    logger.info(
        f"PDF firmado y sellado. Guardado permanentemente en: {final_pdf_path}"
    )
    return final_pdf_path


def callback(ch, method, properties, body):
    """
    Función que se ejecuta por cada mensaje consumido de la
    cola de certificación.
    """
    logger.info(f"Mensaje de certificación recibido: {body.decode()}")
    db: Session = next(get_db())
    task_data = json.loads(body)
    db_message_id = task_data.get("db_message_id")
    temp_pdf_path = None

    # conexión con rabbitmq y procesamiento del mensaje
    connection_pub = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST)
    )
    channel_pub = connection_pub.channel()
    channel_pub.queue_declare(queue=DISTRIBUCION_PDF_QUEUE, durable=True)

    if not db_message_id:
        logger.error("Mensaje inválido, falta 'db_message_id'. Descartando.")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    try:
        sms = (
            db.query(SmsIncoming)
            .filter(
                SmsIncoming.message_id == db_message_id,
                SmsIncoming.status == "sent",
            )
            .first()
        )
        if not sms:
            logger.warning(
                f"No se encontró SMS 'delivered' con id '{db_message_id}'. Descartando."
            )
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        # Paso 1: Generar el PDF base
        temp_pdf_path = create_certification_pdf(sms)

        # Paso 2: firmar el pdf con sello de tiempo integrado
        stamped_pdf_path = sign_and_store_pdf(temp_pdf_path, sms.message_id)

        # Publicar tarea de distribución
        distribution_task = {
            "sms_id": sms.message_id,
            "final_pdf_path": stamped_pdf_path,
            "recipient_email": sms.email_cliente,
            "remote_dir": sms.ftp_directorio,
        }

        channel_pub.basic_publish(
            exchange="",
            routing_key=DISTRIBUCION_PDF_QUEUE,
            body=json.dumps(distribution_task),
            properties=pika.BasicProperties(
                delivery_mode=2,  # make message persistent
            ),
        )

        logger.info(
            f"Tarea de distribución para '{stamped_pdf_path}' enviada a la cola '{DISTRIBUCION_PDF_QUEUE}'."
        )

        # --- Lógica de éxito ---
        # (Opcional) Aquí se podría actualizar la BBDD con la ruta del PDF sellado
        sms.pdf_path = stamped_pdf_path
        db.commit()

        ch.basic_ack(delivery_tag=method.delivery_tag)
        logger.info(
            f"Proceso de certificación y sellado para SMS id '{db_message_id}' completado."
        )

    except Exception as e:
        logger.error(
            f"Fallo CRÍTICO en el flujo de certificación para el mensaje id '{db_message_id}': {e}"
        )
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    finally:
        # Limpieza: eliminar el archivo PDF temporal después del proceso
        if temp_pdf_path and os.path.exists(temp_pdf_path):
            # os.remove(temp_pdf_path)
            logger.info(f"Archivo temporal '{temp_pdf_path}' eliminado.")
        db.close()
        if connection_pub and connection_pub.is_open:
            connection_pub.close()


def main():
    """
    Función principal que inicia la conexión y el consumo de mensajes.
    """
    connection = None
    while True:
        try:
            logger.info("Iniciando worker de certificación PDF...")
            connection_params = pika.ConnectionParameters(
                host=RABBITMQ_HOST,
                heartbeat=600,
                blocked_connection_timeout=300,
            )
            connection = pika.BlockingConnection(connection_params)
            channel = connection.channel()

            channel.queue_declare(queue=CERTIFICACION_PDF_QUEUE, durable=True)
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(
                queue=CERTIFICACION_PDF_QUEUE, on_message_callback=callback
            )

            logger.info(
                f"[*] Esperando mensajes en la cola '{CERTIFICACION_PDF_QUEUE}'. Para salir presione CTRL+C"
            )
            channel.start_consuming()

        except pika.exceptions.AMQPConnectionError as e:
            logger.error(
                f"Error de conexión con RabbitMQ: {e}. Reintentando en 5 segundos..."
            )
            time.sleep(5)
        except KeyboardInterrupt:
            logger.info("Deteniendo el worker...")
            if connection and connection.is_open:
                connection.close()
            break
        except Exception as e:
            logger.error(
                f"Ocurrió un error inesperado en el worker: {e}. Reiniciando..."
            )
            time.sleep(5)


if __name__ == "__main__":
    # ¡IMPORTANTE! Antes de ejecutar, asegúrate de que el certificado exista.
    if not os.path.exists(PKCS12_PATH):
        logger.error(
            f"El archivo del certificado '{PKCS12_PATH}' no se encuentra. El worker no puede iniciar."
        )
    else:
        main()
