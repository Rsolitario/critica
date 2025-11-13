from sqlalchemy.orm import Session
from models.clients import Cliente
from schemas.client import Client as ClientSchema

def create_client(db: Session, client: ClientSchema):
    db_client = Cliente(
        sender=client.sender,
        email_cliente=client.email_cliente,
        ftp_directorio=client.ftp_directorio
    )
    db.add(db_client)
    db.commit()
    db.refresh(db_client)
    return db_client

def get_client_by_sender(db: Session, sender: str):
    return db.query(Cliente).filter(Cliente.sender == sender).first()

def read_clients(db: Session, skip: int = 0, limit: int = 100):
    return db.query(Cliente).offset(skip).limit(limit).all()

def delete_client(db: Session, sender: str):
    client = db.query(Cliente).filter(Cliente.sender == sender).first()
    if client:
        db.delete(client)
        db.commit()
    return client

def update_client(db: Session, sender: str, client_update: ClientSchema):
    client = db.query(Cliente).filter(Cliente.sender == sender).first()
    if client:
        client.email_cliente = client_update.email_cliente
        client.ftp_directorio = client_update.ftp_directorio
        db.commit()
        db.refresh(client)
    return client