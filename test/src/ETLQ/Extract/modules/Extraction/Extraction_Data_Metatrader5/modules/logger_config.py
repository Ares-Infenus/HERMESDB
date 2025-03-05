# ----------------------------
# Descripcion
# ----------------------------

"""Este código define la función setup_logger, que configura y retorna un logger para
registrar eventos en un archivo de texto. Crea un directorio de logs si no existe y guarda
los registros en un archivo con una marca de tiempo en su nombre."""

# ----------------------------
# Conexiones
# ----------------------------

# ----------------------------
# librerias y dependencias
# ----------------------------

import os
import logging
from datetime import datetime


# ----------------------------
# Codigo
# ----------------------------
def setup_logger(log_directory: str) -> logging.Logger:
    """
    Configura y retorna un logger que escribe en un archivo ubicado en log_directory.
    """
    os.makedirs(log_directory, exist_ok=True)
    log_file = os.path.join(
        log_directory, f"download_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    )
    logger = logging.getLogger("DownloaderLogger")
    logger.setLevel(logging.INFO)
    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    fh.setFormatter(formatter)
    if not logger.handlers:
        logger.addHandler(fh)
    return logger
