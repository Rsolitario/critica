# app/schemas.py
from pydantic import BaseModel
from models.users import UserRole

class UserCreate(BaseModel):
    username: str
    password: str
    role: UserRole

class UserInDB(BaseModel):
    username: str
    role: UserRole

    class Config:
        from_attributes = True

class Token(BaseModel):
    access_token: str
    token_type: str