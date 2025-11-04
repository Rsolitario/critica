from sqlalchemy import Column, String, Text, DateTime, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func # Para obtener el timestamp por defecto

# Importamos la Base declarativa desde nuestro archivo de configuración
from database import Base

class Cliente(Base):
    """
    Modelo SQLAlchemy para la tabla 'clientes'.
    Almacena la información de cada cliente identificado por su 'sender'.
    """
    __tablename__ = "clientes"

    # Columnas
    sender = Column(String(50), primary_key=True, index=True, comment="Identificador único del remitente (ej. número de teléfono)")
    email_cliente = Column(String(100), unique=True, nullable=False, index=True, comment="Correo electrónico del cliente")
    ftp_directorio = Column(String(255), nullable=False, comment="Ruta de almacenamiento FTP para el cliente")

    # Relación: Un cliente puede tener muchos SMS entrantes.
    # 'back_populates' establece la relación bidireccional con el modelo SmsIncoming.
    # 'cascade' asegura que si se borra un cliente, sus SMS también se borren.
    sms_entrantes = relationship("SmsIncoming", back_populates="cliente", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<Cliente(sender='{self.sender}', email='{self.email_cliente}')>"


class SmsIncoming(Base):
    """
    Modelo SQLAlchemy para la tabla 'sms_incoming'.
    Almacena los mensajes SMS entrantes de los clientes.
    """
    __tablename__ = "sms_incoming"

    # Columnas
    message_id = Column(String(100), primary_key=True, index=True, comment="ID único del mensaje")
    sender = Column(String(50), ForeignKey("clientes.sender"), nullable=False, index=True, comment="Remitente que envió el SMS")
    receiver = Column(String(50), nullable=False, index=True, comment="Número receptor del SMS")
    content = Column(Text, nullable=False, comment="Contenido del mensaje SMS")
    provider_id = Column(String(100), nullable=True, comment="ID del mensaje asignado por el proveedor externo")
    num_parts = Column(String(10), nullable=True, comment="Número de partes en que se dividió el SMS")
    timestamp_received = Column(DateTime(timezone=True), server_default=func.now(), comment="Fecha y hora de recepción")
    status = Column(String(20), nullable=False, default="pending", index=True, comment="Estado del procesamiento del SMS")
    pdf_path = Column(String(255), nullable=True, comment="Ruta al PDF generado para este SMS")
    
    # Estos campos se denormalizan (duplican) para un acceso más rápido,
    # evitando tener que hacer un JOIN a la tabla de clientes cada vez
    # que se procesa un mensaje. Es una decisión de diseño común.
    email_cliente = Column(String(100), nullable=False, comment="Email del cliente (denormalizado)")
    ftp_directorio = Column(String(255), nullable=False, comment="Ruta FTP del cliente (denormalizado)")

    # Relación: Un SMS pertenece a un único cliente.
    cliente = relationship("Cliente", back_populates="sms_entrantes")

    def __repr__(self):
        return f"<SmsIncoming(id='{self.message_id}', sender='{self.sender}', status='{self.status}')>"
