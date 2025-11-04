from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from setupLog import setup_logging, logging

setup_logging()
log = logging.getLogger(__name__)

# 1. Configuración de la conexión a la base de datos
# Usaremos SQLite para este ejemplo por su simplicidad.
# El archivo 'sms_database.db' se creará en el mismo directorio.
# Para PostgreSQL sería: "postgresql://user:password@postgresserver/db"
SQLALCHEMY_DATABASE_URL = "sqlite:///./sms_database.db"

# 2. Creación del motor (engine) de SQLAlchemy
# El argumento 'connect_args' es necesario solo para SQLite para permitir
# que múltiples hilos (como los de una petición web) interactúen con la BD.
engine = create_engine(
    SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False}
)

# 3. Creación de una fábrica de sesiones (Session factory)
# Cada instancia de SessionLocal será una sesión de base de datos.
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# 4. Creación de una Base declarativa
# Usaremos esta clase base para crear nuestros modelos ORM.
Base = declarative_base()


# 5. Lógica para inicializar la base de datos
def create_db_and_tables():
    """
    Crea todas las tablas en la base de datos que heredan de Base.
    Esta función se llama una sola vez al iniciar la aplicación.
    """
    log.info("Creando tablas en la base de datos...")
    Base.metadata.create_all(bind=engine)
    log.info("Tablas creadas exitosamente.")


# 6. Requisito: Obtener una sesión segura para FastAPI
def get_db():
    """
    Función de dependencia para FastAPI.
    Crea una nueva sesión de base de datos para cada petición, la proporciona
    a la ruta y se asegura de que se cierre correctamente al final.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()