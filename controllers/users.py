from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy.orm import Session

from schemas import user as schemas
from utils import crud
from database import get_db
from utils import auth
from models import users


router = APIRouter(
    prefix="/auth",
    tags=["Authentication and Users"],
)


@router.post("/token", response_model=schemas.Token)
def login_for_access_token(
    db: Session = Depends(get_db),
    form_data: OAuth2PasswordRequestForm = Depends(),
):
    user = crud.get_user_by_username(db, username=form_data.username)
    if not user or not auth.verify_password(
        form_data.password, user.hashed_password
    ):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token = auth.create_access_token(
        data={"sub": user.username, "role": user.role}
    )
    return {"access_token": access_token, "token_type": "bearer"}


@router.post("/create_user/", response_model=schemas.UserInDB)
def create_new_user(
    user: schemas.UserCreate,
    db: Session = Depends(get_db),
    current_user: users.User = Depends(auth.get_current_active_user),
):
    if current_user.role != users.UserRole.ADMIN:
        raise HTTPException(status_code=403, detail="Not enough permissions")
    db_user = crud.get_user_by_username(db, username=user.username)
    if db_user:
        raise HTTPException(
            status_code=400, detail="Username already registered"
        )
    return crud.create_user(db=db, user=user)

@router.get("/user/{user_id}", response_model=schemas.UserInDB)
def read_user(
    user_id: int,
    db: Session = Depends(get_db),
    current_user: users.User = Depends(auth.get_current_active_user),
):
    if current_user.role != users.UserRole.ADMIN:
        raise HTTPException(status_code=403, detail="Not enough permissions")
    db_user = crud.read_user(db, user_id=user_id)
    if db_user is None:
        raise HTTPException(status_code=404, detail="User not found")
    return db_user

@router.get("/users/", response_model=list[schemas.UserInDB])
def read_users(
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db),
    current_user: users.User = Depends(auth.get_current_active_user),
):
    if current_user.role != users.UserRole.ADMIN:
        raise HTTPException(status_code=403, detail="Not enough permissions")
    users_list = crud.read_users(db, skip=skip, limit=limit)
    return users_list

@router.delete("/delete_user/{user_id}", response_model=schemas.UserInDB)
def delete_user(
    user_id: int,
    db: Session = Depends(get_db),
    current_user: users.User = Depends(auth.get_current_active_user),
):
    if current_user.role != users.UserRole.ADMIN:
        raise HTTPException(status_code=403, detail="Not enough permissions")
    user = crud.delete_user(db, user_id=user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return user

@router.put("/update_user/{user_id}", response_model=schemas.UserInDB)
def update_user(
    user_id: int,
    user_update: schemas.UserCreate,
    db: Session = Depends(get_db),
    current_user: users.User = Depends(auth.get_current_active_user),
):
    if current_user.role != users.UserRole.ADMIN:
        raise HTTPException(status_code=403, detail="Not enough permissions")
    user = crud.update_user(db, user_id=user_id, user_update=user_update)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return user