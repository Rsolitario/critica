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
from reportlab.lib.units import inch, cm
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import Paragraph
from reportlab.lib.enums import TA_LEFT, TA_CENTER
from reportlab.lib.colors import HexColor

# --- Nuevas importaciones para el sellado de tiempo ---
from pyhanko.pdf_utils.writer import PdfFileWriter
from pyhanko.pdf_utils.reader import PdfFileReader
from pyhanko.sign import signers, PdfSignatureMetadata, fields
from pyhanko.sign.timestamps import HTTPTimeStamper
from pyhanko.pdf_utils.writer import PdfFileWriter
from pyhanko.pdf_utils.incremental_writer import IncrementalPdfFileWriter
from pyhanko.sign.fields import (
    append_signature_field,
    SigFieldSpec,
    VisibleSigSettings,
)

from dotenv import load_dotenv

# Importaciones de la configuración de la base de datos y modelos
from database import get_db
from models.clients import SmsIncoming

# --- Configuración de Logs ---
from setupLog import setup_logging

# --- Configuración del Worker ---
from productorRabbitmq import (
    RABBITMQ_HOST,
)

load_dotenv()
# Configuración de TSA con autenticación si es necesario
tsa_username_firmaprofesional = os.getenv("TSA_USERNAME_FIRMAPROFESIONAL")
tsa_password_firmaprofesional = os.getenv("TSA_PASSWORD_FIRMAPROFESIONAL")

# --- Configuración para pdf diseño personalizado ---
LOGO_G729 = os.getenv("LOGO_G729")
LOGO_SMS_ES = os.getenv("LOGO_SMS_ES")
LOGO_CNMC = os.getenv("LOGO_CNMC")
LOGO_FIRMAPROFESIONAL = os.getenv("LOGO_FIRMAPROFESIONAL")
# Datos estáticos de la empresa
COMPANY_NAME = os.getenv("COMPANY_NAME")
COMPANY_WEBSITE = os.getenv("COMPANY_WEBSITE")
COMPANY_CIF = os.getenv("COMPANY_CIF")
COMPANY_ADDRESS = os.getenv("COMPANY_ADDRESS")

# --- Configuración de RabbitMQ ---
CERTIFICACION_PDF_QUEUE = "Certificacion_PDF"
DISTRIBUCION_PDF_QUEUE = "Distribucion_PDF"
PDF_OUTPUT_DIR = "certificados/tmp"  # Directorio para guardar los PDFs
# Directorio para guardar los PDFs sellados
PDF_FINAL_DIR = "certificados/sellados"
LOGO_FILE = "logo.png"  # Asegúrate de que este archivo exista

# --- Configuración del Certificado Digital ---
# Se asume un certificado en formato PKCS#12 (.p12 o .pfx)
PKCS12_PATH = (
    os.getenv("CERT_DIGITAL_PATH")  # ¡IMPORTANTE! Coloca aquí la ruta a tu certificado
)
PKCS12_PASSPHRASE = os.getenv("CERT_DIGITAL_PASS")  # ¡IMPORTANTE! La contraseña de tu certificado

# URL de la autoridad de sellado de tiempo (TSA)
TSA_WHITE = ["http://timestamp.digicert.com", "http://tsa.firmaprofesional.com"]
TSA_URL = os.getenv("TSA_URL", "http://timestamp.digicert.com")
# verificación de TSA con autenticación
if TSA_URL not in TSA_WHITE  and (
    tsa_username_firmaprofesional is None
    or tsa_password_firmaprofesional is None
):
    raise ValueError(
        f"La TSA configurada ({TSA_URL}) requiere un usuario y contraseña. "
        "Por favor, defina TSA_USERNAME_FIRMAPROFESIONAL y TSA_PASSWORD_FIRMAPROFESIONAL en su archivo .env"
    )

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

def create_certification_pdf_custom(sms_data: SmsIncoming):
    """
    Genera un PDF de certificación con un diseño específico para sms
    """
    # Formateo de datos que usaremos en el PDF
    now = datetime.utcnow()
    # Formato de fecha dd/mm/yyyy - HH:MM (Timezone)
    fecha_envio_str = sms_data.timestamp_received.strftime(
        "%d/%m/%Y - %H:%M (GMT+1)"
    )
    fecha_emision_str = now.strftime("%d/%m/%Y")
    # Creando identificadores únicos basados en el message_id
    identificador_certificado = (
        f"S-{now.strftime('%Y%m%d')}-{sms_data.message_id[:6]}"
    )
    codigo_validacion = (
        f"LOG-{now.strftime('%Y%m%d')}-{sms_data.message_id[6:16]}"
    )

    file_path = os.path.join(
        PDF_OUTPUT_DIR, f"certificado_{sms_data.message_id}.pdf"
    )
    logger.info(
        f"Creando PDF para el mensaje id '{sms_data.message_id}' en: {file_path}"
    )

    try:
        c = canvas.Canvas(file_path, pagesize=letter)
        width, height = letter

        # --- Definir Estilos de Párrafo ---
        styles = getSampleStyleSheet()
        style_body = ParagraphStyle(
            name="Body",
            parent=styles["Normal"],
            fontName="Helvetica",
            fontSize=10.5,
            leading=16,
            alignment=TA_LEFT,
        )
        style_title = ParagraphStyle(
            name="Title",
            parent=styles["h1"],
            fontName="Helvetica-Bold",
            fontSize=14,
            alignment=TA_CENTER,
            textColor=HexColor("#333333"),
        )
        style_subtitle = ParagraphStyle(
            name="SubTitle", parent=style_title, fontSize=11, spaceAfter=12
        )

        # --- 1. Cabecera (Logos y Títulos) ---
        # Logo izquierdo
        if os.path.exists(LOGO_G729):
            c.drawImage(
                LOGO_G729,
                1 * inch,
                height - 1.2 * inch,
                width=1.5 * inch,
                preserveAspectRatio=True,
                mask="auto",
            )
        # Logo derecho
        if os.path.exists(LOGO_SMS_ES):
            c.drawImage(
                LOGO_SMS_ES,
                width - 2.5 * inch,
                height - 1.2 * inch,
                width=1.5 * inch,
                preserveAspectRatio=True,
                mask="auto",
            )

        # Títulos
        p_title = Paragraph(
            "CERTIFICADO DE COMUNICACIÓN ELECTRÓNICA", style_title
        )
        p_title.wrapOn(c, width - 2 * inch, height)
        p_title.drawOn(c, 1 * inch, height - 1.8 * inch)

        p_subtitle = Paragraph("SMS CERTIFICADO", style_subtitle)
        p_subtitle.wrapOn(c, width - 2 * inch, height)
        p_subtitle.drawOn(c, 1 * inch, height - 2.0 * inch)

        # Línea horizontal
        c.setStrokeColorRGB(0.8, 0.8, 0.8)
        c.line(
            1 * inch,
            height - 2.3 * inch,
            width - 1 * inch,
            height - 2.3 * inch,
        )

        # --- 2. Texto Lateral Vertical ---
        c.saveState()
        c.translate(0.5 * inch, 3 * inch)  # Mover el origen
        c.rotate(90)  # Rotar 90 grados
        c.setFont("Helvetica", 8)
        c.setFillColorRGB(0.5, 0.5, 0.5)
        c.drawString(
            0, 0, f"{COMPANY_NAME} C.I.F: {COMPANY_CIF} {COMPANY_ADDRESS}"
        )
        c.restoreState()

        # --- 3. Cuerpo del Documento (usando una función para manejar la posición Y) ---
        y_pos = height - 2.8 * inch
        margin_left = 1 * inch
        content_width = width - 2 * margin_left

        def draw_flowable(flowable, y):
            """Dibuja un objeto Flowable (como un Paragraph) y retorna la nueva posición Y."""
            w, h = flowable.wrapOn(c, content_width, height)
            flowable.drawOn(c, margin_left, y - h)
            return y - h - 0.25 * inch  # Espacio después del párrafo

        # Contenido del certificado
        p = Paragraph(
            f"<b>Identificador del certificado:</b> {identificador_certificado}",
            style_body,
        )
        y_pos = draw_flowable(p, y_pos)

        p = Paragraph(
            f"El operador de comunicaciones electrónicas {COMPANY_NAME} ({COMPANY_WEBSITE}), "
            "en calidad de tercero de confianza, certifica que los datos consignados en el "
            "presente documento son los que constan en sus registros de comunicaciones electrónicas.",
            style_body,
        )
        y_pos = draw_flowable(p, y_pos)

        # Usamos espacios adicionales para separar
        y_pos -= 0.1 * inch

        p = Paragraph(f"<b>Remitente:</b> sms.es", style_body)
        y_pos = draw_flowable(p, y_pos)

        destinatario = getattr(sms_data, "receiver", "N/A")
        p = Paragraph(f"<b>Destinatario:</b> {destinatario}", style_body)
        y_pos = draw_flowable(p, y_pos)

        p = Paragraph(
            f'<b>Contenido del mensaje:</b> "{sms_data.content}"', style_body
        )
        y_pos = draw_flowable(p, y_pos)

        p = Paragraph(
            f"<b>Fecha y hora de envío:</b> {fecha_envio_str}", style_body
        )
        y_pos = draw_flowable(p, y_pos)

        # La fecha de entrega puede ser la misma que la de envío en este contexto
        p = Paragraph(
            f"<b>Fecha y hora de entrega:</b> {fecha_envio_str}", style_body
        )
        y_pos = draw_flowable(p, y_pos)

        y_pos -= 0.1 * inch

        p = Paragraph(
            "<b>Sello de tiempo emitido por Firmaprofesional S.A.</b>",
            style_body,
        )
        y_pos = draw_flowable(p, y_pos)

        p = Paragraph(
            f"<b>Código de validación:</b> {codigo_validacion}", style_body
        )
        y_pos = draw_flowable(p, y_pos)

        y_pos -= 0.1 * inch

        p = Paragraph(
            f"La autenticidad de este documento puede verificarse en https://{COMPANY_WEBSITE}/certified-sms/ "
            "introduciendo el código de validación indicado.",
            style_body,
        )
        y_pos = draw_flowable(p, y_pos)

        y_pos -= 0.1 * inch

        p = Paragraph(
            f"<b>Emitido por:</b> {COMPANY_NAME} ({COMPANY_WEBSITE}) – Operador de comunicaciones electrónicas y tercero de confianza digital.",
            style_body,
        )
        y_pos = draw_flowable(p, y_pos)

        p = Paragraph(
            f"<b>Fecha de emisión:</b> {fecha_emision_str}", style_body
        )
        y_pos = draw_flowable(p, y_pos)

        # --- 4. Pie de Página (Logos) ---
        footer_y = 0.2 * inch
        if os.path.exists(LOGO_CNMC):
            c.drawImage(
                LOGO_CNMC,
                margin_left,
                footer_y,
                height=0.5 * inch,
                preserveAspectRatio=True,
                mask="auto",
            )
        if os.path.exists(LOGO_FIRMAPROFESIONAL):
            c.drawImage(
                LOGO_FIRMAPROFESIONAL,
                width - margin_left - (2.5 * inch),
                footer_y,
                height=0.5 * inch,
                preserveAspectRatio=True,
                mask="auto",
            )

        # --- Finalizar y Guardar ---
        c.save()
        logger.info(f"PDF guardado exitosamente en '{file_path}'.")
        return file_path

    except Exception as e:
        logger.error(
            f"No se pudo generar el PDF para el mensaje id '{sms_data.message_id}': {e}"
        )
        raise


def sign_and_store_pdf(temp_pdf_path: str, sms_id: str) -> str:
    """
    Aplica una firma digital PAdES-B-LT y guarda el PDF en la ubicación final.
    """
    logger.info(f"Iniciando firma digital para '{temp_pdf_path}'.")

    # 1. Cargar el certificado
    try:
        signer = signers.SimpleSigner.load_pkcs12(
            pfx_file=PKCS12_PATH,
            passphrase=(
                PKCS12_PASSPHRASE.encode("utf-8")
                if PKCS12_PASSPHRASE
                else None
            ),
        )
    except Exception as e:
        logger.error(f"¡ERROR CRÍTICO! No se pudo cargar el certificado: {e}")
        raise

    # 2. Definir los metadatos de la firma que usaremos
    meta = signers.PdfSignatureMetadata(
        field_name=os.getenv("CERT_NAME", "FirmaEmpresa"),
        reason=os.getenv("CERT_REASON", "Certificación de entrega de comunicación"),
        location=os.getenv("CERT_LOCATION", "Servidor Central"),
    )

    # 3. Preparar el archivo de salida
    final_pdf_path = os.path.join(
        PDF_FINAL_DIR, f"certificado_final_{sms_id}.pdf"
    )

    with open(temp_pdf_path, "rb") as doc_in, open(
        final_pdf_path, "wb"
    ) as doc_out:
        # Abrimos el PDF original y lo preparamos para una modificación incremental
        w = IncrementalPdfFileWriter(doc_in)

        # Añadimos un campo de firma vacío, que es necesario para que la firma sea visible
        fields.append_signature_field(
            w,
            sig_field_spec=fields.SigFieldSpec(
                sig_field_name="FirmaEmpresa",
                box=(
                    72.25,
                    50,
                    250,
                    100,
                ),  # (x1, y1, x2, y2) desde la esquina inferior izquierda
            ),
        )

        # Creamos el objeto firmante, combinando los metadatos, el certificado y el sello de tiempo
        timestamper = (
            HTTPTimeStamper(url=TSA_URL)
            if TSA_URL == "http://timestamp.digicert.com" or TSA_URL == "http://tsa.firmaprofesional.com"
            else HTTPTimeStamper(
                url=TSA_URL,
                auth=(
                    tsa_username_firmaprofesional,
                    tsa_password_firmaprofesional,
                ),
            )
        )

        pdf_signer = signers.PdfSigner(meta, signer, timestamper=timestamper)

        # Finalmente, firmamos el PDF modificado (w) y escribimos el resultado en el archivo de salida (doc_out)
        pdf_signer.sign_pdf(w, output=doc_out)

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
                SmsIncoming.status == "DELIVERED",
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
        temp_pdf_path = create_certification_pdf_custom(sms)

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
            os.remove(temp_pdf_path)
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
