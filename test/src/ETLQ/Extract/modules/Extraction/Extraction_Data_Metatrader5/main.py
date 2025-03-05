# ----------------------------
# Descripcion
# ----------------------------

# ----------------------------
# Tareas pendientes
# ----------------------------
# ** Al parecer no se inicializa si primero no se abre la cuenta y anexa el servidor
# ** en LA INTERFAZ grafica de metatrader 5
# ? si quieres ejecutar este modulo tendras que cambiar en la seccion de conexiones
# ? from .modules.historical_data_downloader import HistoricalDataDownloader por
# ? from modules.historical_data_downloader import HistoricalDataDownloader
# TODO: [FEATURE] Automatizar la eliminación de datos descargados del broker y la caché de MetaTr ader (ubicada en AppData)
# TODO: [FEATURE] Se debe implementar el monitorio de recursos para diagnosticar problemas de rendimiento.
# TODO: [FEATURE] Se debe optimizar las funciones y los modulos.
# TODO: [FEATURE] Se debe terminar la documentacion una vez terminado la implementacion del monitoreo de recursos y la optimizacion de modulos y submodulos

# ----------------------------
# librerias y dependencias
# ----------------------------

import multiprocessing as mp
from datetime import datetime
import pandas as pd

# ----------------------------
# Conexiones
# ----------------------------
from modules.historical_data_downloader import HistoricalDataDownloader

# ----------------------------
# Codigo
# ----------------------------

if __name__ == "__main__":
    # Configuración de ejemplo (reemplazar con datos reales)
    data = {
        "Tipo": ["user", "password", "Investor", "Server"],
        # "Dukascopy": [1717024561, "/BUPf3]`", float("nan"), "Dukascopy-demo-mt5-1"],
        "OANDA": [6369670, "GetBun72+", float("nan"), "OANDA-Demo-1"],
    }
    credentials_df = pd.DataFrame(data)
    LOG_FOLDER = r"C:\Users\spinz\OneDrive\Documentos\Portafolio oficial\HERMESDB\HERMESDB\test\data\logs"
    DATA_FOLDER = r"C:\Users\spinz\OneDrive\Documentos\Portafolio oficial\HERMESDB\HERMESDB\test\data\backup"

    start_date = datetime(2000, 1, 1)
    end_date = datetime.now()

    # En Windows es importante usar el método "spawn"
    mp.set_start_method("spawn", force=True)

    downloader = HistoricalDataDownloader(
        authentication_df=credentials_df,
        logs_path=LOG_FOLDER,
        data_storage=DATA_FOLDER,
        start_timestamp=start_date,
        end_timestamp=end_date,
    )

    try:
        downloader.process_all_brokers()
    except Exception as e:  # pylint: disable=broad-exception-caught
        print(f"Se produjo un error durante la descarga: {e}")
