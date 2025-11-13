from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from passlib.context import CryptContext
from datetime import datetime, timedelta
from models import users
from schemas import  user as user_schemas
from sqlalchemy.orm import Session
from database import get_db
import os
from dotenv import load_dotenv

from models.users import User, UserRole
from utils import crud

load_dotenv()  # loader .env

SECRET_KEY = os.getenv("SECRET_KEY")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/token")

def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)

def get_password_hash(password):
    return pwd_context.hash(password)

def create_access_token(data: dict):
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

# --- Dependencias de FastAPI para Autenticación ---

def get_current_user(
    db: Session = Depends(get_db), token: str = Depends(oauth2_scheme)
) -> User:
    """
    Dependencia para decodificar el token JWT y obtener el usuario actual.
    Esta es la función base que se usa en otras dependencias.
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="No se pudieron validar las credenciales",
        headers={"WWW-Authenticate": "Bearer"},
    )
    
    try:
        # Decodifica el token
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
            
    except JWTError:
        # Si el token es inválido o ha expirado
        raise credentials_exception
    
    # Busca el usuario en la base de datos
    user = crud.get_user_by_username(db, username=username)
    if user is None:
        # Si el usuario extraído del token ya no existe
        raise credentials_exception
        
    return user


def get_current_active_user(current_user: User = Depends(get_current_user)) -> User:
    """
    Dependencia para obtener el usuario actual y activo.
    En este ejemplo, no tenemos un campo 'is_active', por lo que simplemente
    devuelve el usuario obtenido. Se puede extender fácilmente.
    
    Esta es la función que debes usar para proteger tus endpoints.
    """
    # Si tuvieras un campo `is_active` en el modelo User, lo comprobarías aquí:
    # if not current_user.is_active:
    #     raise HTTPException(status_code=400, detail="Usuario inactivo")
    return current_user


# --- Dependencias de FastAPI para Autorización por Roles ---

def get_current_admin_user(current_user: User = Depends(get_current_active_user)) -> User:
    """
    Dependencia que verifica que el usuario actual no solo esté autenticado,
    sino que también tenga el rol de 'ADMIN'.
    """
    if current_user.role != UserRole.ADMIN:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="La operación requiere privilegios de administrador",
        )
    return current_user