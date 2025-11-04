import logging
import sys

def setup_logging():
    """
    Configura un logger formateado para mostrar módulo, función y línea.
    """
    # 1. Elige un formato detallado
    # Formato: HORA - NOMBRE_LOGGER - NIVEL - MODULO:FUNCIÓN:LÍNEA - MENSAJE
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s"

    # 2. Crea un Formatter con el formato que definiste
    formatter = logging.Formatter(log_format)

    # 3. Crea un manejador (Handler) para dirigir la salida (ej. a la consola)
    # StreamHandler envía los logs a la salida estándar (consola)
    handler = logging.StreamHandler(sys.stdout)
    
    # 4. Asigna el formatter al handler
    handler.setFormatter(formatter)

    # 5. Obtiene el logger raíz y le añade el handler
    # Configurar el logger raíz afecta a todos los loggers del proyecto
    # a menos que se configuren específicamente.
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO) # Establece el nivel mínimo de log a mostrar
    
    # Evitar añadir handlers duplicados si esta función se llama más de una vez
    if not root_logger.handlers:
        root_logger.addHandler(handler)