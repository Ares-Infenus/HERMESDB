# ----------------------------
# Descripcion
# ----------------------------

# ----------------------------
# Tareas pendientes
# ----------------------------
# ** Al parecer no se inicializa si primero no se abre la cuenta y an exa el servidor
# ** en LA INTERFAZ grafica de metatrader 5
# ? si quieres ejecutar este modulo tendras que cambiar en la seccion de conexiones
# ? from .modules.historical_data_downloader import HistoricalDataDownloader por
# ? from modules.historical_data_downloader import HistoricalDataDownloader
# TODO: [FEATURE] Eliminar el exportado de log de ExtractMT5 no es util
# TODO: [FEATURE] Automatizar la eliminación de datos descargados del broker y la caché de MetaTr ader (ubicada en AppData)
# TODO: [FEATURE] Se debe implementar el monitorio de recursos para diagnosticar problemas de rendimiento.
# TODO: [FEATURE] Se debe optimizar las funciones y los modulos.
# TODO: [FEATURE] Se debe terminar la documentacion una vez terminado la implementacion del monitoreo de recursos y la optimizacion de modulos y submodulos
# TODO: [FEATURE] Recuerda agregarle las columnas run_id y id a System_monitor.csv

# ----------------------------
# librerias y dependencias
# ----------------------------
import os
import multiprocessing as mp
from datetime import datetime
import pandas as pd

# ----------------------------
# Conexiones
# ----------------------------
from modules.historical_data_downloader import HistoricalDataDownloader
from performance.system_monitor import SystemMonitor

# ----------------------------
# Codigo
# ----------------------------

if __name__ == "__main__":
    # Instanciamos el monitor, especificando la ruta del CSV y el intervalo deseado
    monitor = SystemMonitor(
        csv_path=r"C:\Users\spinz\OneDrive\Documentos\Portafolio oficial\HERMESDB\HERMESDB\test\data\logs\Extract_Data_metatrader5\System_monitor.csv",
        interval=1.0,
    )
    monitor.start()  # Inicia la monitorización en un hilo paralelo

    # Configuración de ejemplo (reemplazar con datos reales)
    data = {
        "Tipo": ["user", "password", "Investor", "Server"],
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
    except Exception as e:  # Capturamos cualquier error durante la descarga
        print(f"Se produjo un error durante la descarga: {e}")
    finally:
        # Detenemos el monitor y exportamos las métricas al CSV
        monitor.stop()
