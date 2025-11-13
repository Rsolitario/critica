from sqlalchemy.orm import Session
from models.users import User, UserRole
from schemas import user as user_schema
from utils import auth

def get_user_by_username(db: Session, username: str):
    return db.query(User).filter(User.username == username).first()

def create_user(db: Session, user: user_schema.UserCreate):
    hashed_password = auth.get_password_hash(user.password)
    db_user = User(
        username=user.username,
        hashed_password=hashed_password,
        role=user.role
    )
    db.add(db_user)
    db.commit()
    db.refresh(db_user)
    return db_user

def read_user(db: Session, user_id: int):
    return db.query(User).filter(User.id == user_id).first()

def read_users(db: Session, skip: int = 0, limit: int = 100):
    return db.query(User).offset(skip).limit(limit).all()

def delete_user(db: Session, user_id: int):
    user = db.query(User).filter(User.id == user_id).first()
    if user:
        db.delete(user)
        db.commit()
    return user

def update_user(db: Session, user_id: int, user_update: user_schema.UserCreate):
    user = db.query(User).filter(User.id == user_id).first()
    if user:
        user.username = user_update.username
        user.hashed_password = auth.get_password_hash(user_update.password)
        user.role = user_update.role
        db.commit()
        db.refresh(user)
        user.hashed_password = "*" * len(user_update.password) 
    return user