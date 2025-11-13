from pydantic import BaseModel

class Client(BaseModel):
    sender: str
    email_cliente: str
    ftp_directorio: str