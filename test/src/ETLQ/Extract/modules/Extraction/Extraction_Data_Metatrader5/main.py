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
# TODO: [FEATURE] Recuerda agregarle las columnas run_id y id a system_monitor.csv
# TODO: [FEATURE] Recuerda agregarle las columnas run_id y id a funtion_profiles.csv

# ----------------------------
# librerias y dependencias
# ----------------------------

# Standard library imports
import cProfile
import pstats
import multiprocessing as mp
from datetime import datetime

# Third-party imports
import pandas as pd

# ----------------------------
# Conexiones
# ----------------------------

from modules.historical_data_downloader import HistoricalDataDownloader
from performance.system_monitor import SystemMonitor
from performance.function_profiles import ProfileExporter
from profiling_utils import mem_profile, memory_usage_dict


# ----------------------------
# Codigo
# ----------------------------


def run_downloader():
    """
    Ejecuta la descarga de datos históricos mientras se monitoriza el sistema.
    Separa la responsabilidad de la lógica de negocio y la monitorización.
    """
    # Inicializa y arranca el monitor del sistema
    monitor = SystemMonitor(
        csv_path=r"C:\Users\spinz\OneDrive\Documentos\Portafolio oficial\HERMESDB\HERMESDB\test\data\logs\Extract_Data_metatrader5\system_metrics.csv",
        interval=1.0,
    )
    monitor.start()

    # Configuración de credenciales y rutas
    data = {
        "Tipo": ["user", "password", "Investor", "Server"],
        "OANDA": [6369670, "GetBun72+", float("nan"), "OANDA-Demo-1"],
    }
    credentials_df = pd.DataFrame(data)
    LOG_FOLDER = r"C:\Users\spinz\OneDrive\Documentos\Portafolio oficial\HERMESDB\HERMESDB\test\data\logs"
    DATA_FOLDER = r"C:\Users\spinz\OneDrive\Documentos\Portafolio oficial\HERMESDB\HERMESDB\test\data\backup"

    start_date = datetime(2000, 1, 1)
    end_date = datetime.now()

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
    except Exception as e:
        print(f"Error durante la descarga: {e}")
    finally:
        monitor.stop()


@mem_profile
def main():
    """
    Función principal decorada para monitorear el consumo de memoria.
    Cumple el principio de inversión de dependencias (DIP) al depender de abstracciones (funciones y clases).
    """
    run_downloader()


if __name__ == "__main__":
    # Inicia el perfilador de rendimiento
    profiler = cProfile.Profile()
    profiler.enable()

    main()

    profiler.disable()

    # Exporta el perfil a un archivo de texto para análisis
    with open(
        r"C:\Users\spinz\OneDrive\Documentos\Portafolio oficial\HERMESDB\HERMESDB\test\data\logs\Extract_Data_metatrader5\function_profiles.txt",
        "w",
        encoding="utf-8",
    ) as f:
        stats_obj = pstats.Stats(profiler, stream=f)
        stats_obj.sort_stats(pstats.SortKey.TIME)
        stats_obj.print_stats()

    # Exporta el perfil a CSV utilizando la clase ProfileExporter
    exporter = ProfileExporter(profiler, memory_usage_dict)
    exporter.export_to_csv(
        r"C:\Users\spinz\OneDrive\Documentos\Portafolio oficial\HERMESDB\HERMESDB\test\data\logs\Extract_Data_metatrader5\function_profiles.csv"
    )
