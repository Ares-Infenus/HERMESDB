"""
Módulo para la descarga de datos históricos desde MetaTrader5 utilizando multiprocessing.

Este módulo configura el logger, gestiona la conexión a MetaTrader5, y descarga los datos
por rangos mensuales para cada símbolo disponible, guardándolos en archivos CSV.
"""

# ----------------------------
# Tareas pendientes
# ----------------------------

# TODO: [FEATURE] Automatizar la eliminación de datos descargados del broker y la caché de MetaTrader (ubicada en AppData)

# ----------------------------
# librerias y dependencias
# ----------------------------
import os
import logging
import multiprocessing as mp
from datetime import datetime
from tqdm import tqdm
import pandas as pd
import MetaTrader5 as mt5


# ----------------------------
# Configuración y utilidades
# ----------------------------


def generate_month_ranges(initial_date: datetime, final_date: datetime) -> list:
    """
    Genera una lista de tuplas (inicio, fin) para cada mes entre initial_date y final_date.
    """
    ranges = []
    current_start = initial_date
    while current_start < final_date:
        if current_start.month == 12:
            next_month = datetime(current_start.year + 1, 1, 1)
        else:
            next_month = datetime(current_start.year, current_start.month + 1, 1)
        current_end = min(next_month, final_date)
        ranges.append((current_start, current_end))
        current_start = next_month
    return ranges


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


# ----------------------------
# Conexión a MetaTrader5
# ----------------------------


class MT5Connection:
    """
    Encapsula la inicialización y desconexión de MetaTrader5.
    """

    def __init__(self, credentials: dict):
        """
        Inicializa la conexión con las credenciales requeridas.

        Args:
            credentials (dict): Credenciales con claves 'server', 'login', 'password'
                                y opcionalmente 'investor_password'.
        """
        self.credentials = credentials

    def initialize(self):
        """
        Inicializa la conexión a MetaTrader5 con las credenciales proporcionadas.

        Raises:
            RuntimeError: Si falla la inicialización, se lanza una excepción con el error.
        """
        init_args = {
            "server": self.credentials["server"],
            "login": self.credentials["login"],
            "password": self.credentials["password"],
        }
        if "investor_password" in self.credentials:
            init_args["investor_password"] = self.credentials["investor_password"]

        if not mt5.initialize(**init_args):  # pylint: disable=no-member
            error_msg = f"Falló la inicialización de MT5: {mt5.last_error()}"  # pylint: disable=no-member
            raise RuntimeError(error_msg)

    @staticmethod
    def shutdown():
        """
        Cierra la conexión a MetaTrader5.
        """
        mt5.shutdown()  # pylint: disable=no-member


# ----------------------------
# Worker para descarga de datos
# ----------------------------


class WorkerState:
    """Gestiona el estado global de los workers."""

    credentials = None
    data_directory = None
    date_range = (None, None)


def worker_initializer(credentials: dict, data_directory: str, date_range: tuple):
    """
    Inicializa las variables necesarias en cada proceso worker.
    """
    WorkerState.credentials = credentials
    WorkerState.data_directory = data_directory
    WorkerState.date_range = date_range

    connection = MT5Connection(WorkerState.credentials)
    connection.initialize()


def download_symbol_data(symbol: str) -> dict:
    """
    Descarga los datos históricos para el símbolo dado en diferentes temporalidades.
    Los datos se descargan por rangos mensuales, se procesan y se guardan en un archivo CSV.
    """
    try:
        timeframes = {
            "H1": mt5.TIMEFRAME_H1,
            "H4": mt5.TIMEFRAME_H4,
            "D1": mt5.TIMEFRAME_D1,
            "W1": mt5.TIMEFRAME_W1,
            "MN1": mt5.TIMEFRAME_MN1,
        }
        collected_dfs = []
        timeframe_log = {}

        begin_date, fin_date = WorkerState.date_range
        month_ranges = generate_month_ranges(begin_date, fin_date)

        for tf_label, tf_value in timeframes.items():
            dfs = []
            for month_start, month_end in month_ranges:
                rates = mt5.copy_rates_range(  # pylint: disable=no-member
                    symbol, tf_value, month_start, month_end
                )
                if rates is not None and len(rates) > 0:
                    df_temp = pd.DataFrame(rates)
                    dfs.append(df_temp)
            if dfs:
                df_tf = pd.concat(dfs, ignore_index=True)
                df_tf["time"] = pd.to_datetime(df_tf["time"], unit="s")
                df_tf["timeframe"] = tf_label
                timeframe_log[tf_label] = {
                    "rows": len(df_tf),
                    "first_date": str(df_tf["time"].min()),
                    "last_date": str(df_tf["time"].max()),
                }
                collected_dfs.append(df_tf)
            else:
                timeframe_log[tf_label] = {
                    "rows": 0,
                    "first_date": None,
                    "last_date": None,
                }

        if collected_dfs:
            final_df = pd.concat(collected_dfs, ignore_index=True)
            final_df.drop_duplicates(subset=["time", "timeframe"], inplace=True)
        else:
            final_df = pd.DataFrame()

        safe_symbol = symbol.replace("/", "_")
        output_file = os.path.join(WorkerState.data_directory, f"{safe_symbol}.csv")
        final_df.to_csv(output_file, index=False)

        return {
            "symbol": symbol,
            "log": timeframe_log,
            "file": output_file,
            "error": None,
        }

    except (FileNotFoundError, PermissionError) as e:
        return {
            "symbol": symbol,
            "log": {},
            "file": None,
            "error": f"Error de archivo: {e}",
        }

    except (ValueError, TypeError) as e:
        return {
            "symbol": symbol,
            "log": {},
            "file": None,
            "error": f"Error de datos: {e}",
        }

    except ConnectionError as e:
        return {
            "symbol": symbol,
            "log": {},
            "file": None,
            "error": f"Error de conexión: {e}",
        }

    except OSError as e:
        return {
            "symbol": symbol,
            "log": {},
            "file": None,
            "error": f"Error del sistema: {e}",
        }


# ----------------------------
# Descargador de datos históricos
# ----------------------------


class HistoricalDataDownloader:
    """
    Gestiona la extracción de credenciales, la conexión a los brokers y la descarga
    de datos históricos utilizando multiprocessing.
    """

    def __init__(
        self,
        authentication_df: pd.DataFrame,
        logs_path: str,
        data_storage: str,
        start_timestamp: datetime = None,
        end_timestamp: datetime = None,
    ):
        self.authentication_df = authentication_df
        self.logs_path = logs_path
        self.data_storage = data_storage  # Se asigna a self.data_storage
        self.logger = setup_logger(logs_path)

        os.makedirs(self.data_storage, exist_ok=True)

        self.start_date = start_timestamp or datetime(2000, 1, 1)
        self.end_date = end_timestamp or datetime.now()

    def extract_credentials(self, broker: str) -> dict:
        """
        Extrae las credenciales del DataFrame para un broker dado.
        """
        df = self.authentication_df.set_index("Tipo")
        credentials = {
            "login": int(df.at["user", broker]),
            "password": df.at["password", broker],
            "server": df.at["Server", broker],
        }
        investor = df.at["Investor", broker]
        if pd.notna(investor):
            credentials["investor_password"] = investor
        return credentials

    def process_broker(self, broker: str):
        """
        Procesa un broker: se conecta, obtiene la lista de símbolos y descarga
        los datos históricos de cada uno utilizando multiprocessing.
        """
        self.logger.info("Procesando broker: %s", broker)
        credentials = self.extract_credentials(broker)
        connection = MT5Connection(credentials)
        connection.initialize()

        symbols = mt5.symbols_get()  # pylint: disable=no-member
        if symbols is None:
            error_msg = f"No se pudieron obtener símbolos para broker {broker}"
            self.logger.error(error_msg)
            connection.shutdown()
            raise RuntimeError(error_msg)

        symbol_list = [s.name for s in symbols]
        connection.shutdown()
        self.logger.info(
            "Broker %s: %s símbolos encontrados.", broker, len(symbol_list)
        )

        broker_data_folder = os.path.join(self.data_storage, broker)
        os.makedirs(broker_data_folder, exist_ok=True)

        pool = mp.Pool(
            processes=mp.cpu_count(),
            initializer=worker_initializer,
            initargs=(
                credentials,
                broker_data_folder,
                (self.start_date, self.end_date),
            ),
        )

        results = []
        for result in tqdm(
            pool.imap_unordered(download_symbol_data, symbol_list),
            total=len(symbol_list),
            desc=f"Broker {broker}",
        ):
            results.append(result)
            if result.get("error"):
                pool.terminate()
                error_info = f"Error en activo {result['symbol']}: {result['error']}"
                self.logger.error(error_info)
                raise RuntimeError(error_info)
            self.logger.info(
                "Activo %s descargado. Detalles: %s. Archivo: %s",
                result["symbol"],
                result["log"],
                result["file"],
            )

        pool.close()
        pool.join()
        return results

    def process_all_brokers(self):
        """
        Procesa todos los brokers disponibles en el DataFrame.
        """
        broker_columns = [
            col for col in self.authentication_df.columns if col != "Tipo"
        ]
        total_files_downloaded = 0

        for broker in broker_columns:
            try:
                results = self.process_broker(broker)
                total_files_downloaded += len(results)
            except Exception as e:
                self.logger.error("Excepción al procesar broker %s: %s", broker, str(e))
                raise e

        self.logger.info("Total de archivos descargados: %s", total_files_downloaded)


# ----------------------------
# Ejemplo de uso
# ----------------------------

if __name__ == "__main__":
    # Configuración de ejemplo (reemplazar con datos reales)
    data = {
        "Tipo": ["user", "password", "Investor", "Server"],
        "Dukascopy": [561753946, "=UcA9-:c", float("nan"), "Dukascopy-demo-mt5-1"],
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
