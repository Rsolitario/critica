from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from schemas.client import Client as ClientSchema
from database import get_db
from models import users
from utils import auth
from models.clients import Cliente
from utils import client_crud

router = APIRouter(
    prefix="/clients",
    tags=["Clients"],
)

@router.post("/create_client/", 
             response_model=ClientSchema)
def create_new_client(
    client: ClientSchema,
    db: Session = Depends(get_db),
    current_user: users.User = Depends(auth.get_current_active_user),
):
    if current_user.role != users.UserRole.ADMIN:
        raise HTTPException(status_code=403, detail="Not enough permissions")
    db_client = db.query(Cliente).filter(Cliente.sender == client.sender).first()
    if db_client:
        raise HTTPException(
            status_code=400, detail="Client with this sender already registered"
        )
    return client_crud.create_client(db=db, client=client)

@router.get("/clients/", 
            response_model=list[ClientSchema])
def read_clients(
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db),
    current_user: users.User = Depends(auth.get_current_active_user),
):
    if current_user.role != users.UserRole.ADMIN:
        raise HTTPException(status_code=403, detail="Not enough permissions")
    clients = client_crud.read_clients(db, skip=skip, limit=limit)
    return clients

@router.delete("/delete_client/{sender}", 
               response_model=ClientSchema)
def delete_client(
    sender: str,
    db: Session = Depends(get_db),
    current_user: users.User = Depends(auth.get_current_active_user),
):
    if current_user.role != users.UserRole.ADMIN:
        raise HTTPException(status_code=403, detail="Not enough permissions")
    client = client_crud.delete_client(db, sender=sender)
    if not client:
        raise HTTPException(
            status_code=404, detail="Client not found"
        )
    return client

@router.put("/update_client/{sender}", 
            response_model=ClientSchema)
def update_client(
    sender: str,
    client_update: ClientSchema,
    db: Session = Depends(get_db),
    current_user: users.User = Depends(auth.get_current_active_user),
):
    if current_user.role != users.UserRole.ADMIN:
        raise HTTPException(status_code=403, detail="Not enough permissions")
    client = client_crud.update_client(db, sender=sender, client_update=client_update)
    if not client:
        raise HTTPException(
            status_code=404, detail="Client not found"
        )
    return client