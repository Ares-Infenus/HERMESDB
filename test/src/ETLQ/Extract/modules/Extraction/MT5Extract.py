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

def generate_month_ranges(start_date: datetime, end_date: datetime) -> list:
    """
    Genera una lista de tuplas (inicio, fin) para cada mes entre start_date y end_date.
    """
    ranges = []
    current_start = start_date
    while current_start < end_date:
        if current_start.month == 12:
            next_month = datetime(current_start.year + 1, 1, 1)
        else:
            next_month = datetime(current_start.year, current_start.month + 1, 1)
        current_end = min(next_month, end_date)
        ranges.append((current_start, current_end))
        current_start = next_month
    return ranges


def setup_logger(log_folder: str) -> logging.Logger:
    """
    Configura y retorna un logger que escribe en un archivo ubicado en log_folder.
    """
    os.makedirs(log_folder, exist_ok=True)
    log_file = os.path.join(
        log_folder,
        f"download_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    )
    logger = logging.getLogger("DownloaderLogger")
    logger.setLevel(logging.INFO)
    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
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
        self.credentials = credentials

    def initialize(self):
        init_args = {
            "server": self.credentials["server"],
            "login": self.credentials["login"],
            "password": self.credentials["password"]
        }
        if "investor_password" in self.credentials:
            init_args["investor_password"] = self.credentials["investor_password"]

        if not mt5.initialize(**init_args):
            error_msg = f"Falló la inicialización de MT5: {mt5.last_error()}"
            raise Exception(error_msg)

    @staticmethod
    def shutdown():
        mt5.shutdown()


# ----------------------------
# Worker para descarga de datos
# ----------------------------

def worker_initializer(credentials: dict, data_folder: str, date_range: tuple):
    """
    Inicializa las variables globales necesarias en cada proceso worker.
    """
    global WORKER_CREDENTIALS, WORKER_DATA_FOLDER, WORKER_DATE_RANGE
    WORKER_CREDENTIALS = credentials
    WORKER_DATA_FOLDER = data_folder
    WORKER_DATE_RANGE = date_range

    connection = MT5Connection(WORKER_CREDENTIALS)
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
            "MN1": mt5.TIMEFRAME_MN1
        }
        collected_dfs = []
        timeframe_log = {}

        start_date, end_date = WORKER_DATE_RANGE
        month_ranges = generate_month_ranges(start_date, end_date)

        for tf_label, tf_value in timeframes.items():
            dfs = []
            for month_start, month_end in month_ranges:
                rates = mt5.copy_rates_range(symbol, tf_value, month_start, month_end)
                if rates is not None and len(rates) > 0:
                    df_temp = pd.DataFrame(rates)
                    dfs.append(df_temp)
            if dfs:
                df_tf = pd.concat(dfs, ignore_index=True)
                df_tf['time'] = pd.to_datetime(df_tf['time'], unit='s')
                df_tf['timeframe'] = tf_label
                timeframe_log[tf_label] = {
                    "rows": len(df_tf),
                    "first_date": str(df_tf['time'].min()),
                    "last_date": str(df_tf['time'].max())
                }
                collected_dfs.append(df_tf)
            else:
                timeframe_log[tf_label] = {"rows": 0, "first_date": None, "last_date": None}

        if collected_dfs:
            final_df = pd.concat(collected_dfs, ignore_index=True)
            final_df.drop_duplicates(subset=["time", "timeframe"], inplace=True)
        else:
            final_df = pd.DataFrame()

        safe_symbol = symbol.replace("/", "_")
        output_file = os.path.join(WORKER_DATA_FOLDER, f"{safe_symbol}.csv")
        final_df.to_csv(output_file, index=False)

        return {"symbol": symbol, "log": timeframe_log, "file": output_file, "error": None}

    except Exception as e:
        return {"symbol": symbol, "log": {}, "file": None, "error": str(e)}


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
        credentials_df: pd.DataFrame,
        log_folder: str,
        data_folder: str,
        start_date: datetime = None,
        end_date: datetime = None
    ):
        self.credentials_df = credentials_df
        self.log_folder = log_folder
        self.data_folder = data_folder
        self.logger = setup_logger(log_folder)

        os.makedirs(self.data_folder, exist_ok=True)

        self.start_date = start_date or datetime(2000, 1, 1)
        self.end_date = end_date or datetime.now()

    def extract_credentials(self, broker: str) -> dict:
        """
        Extrae las credenciales del DataFrame para un broker dado.
        """
        df = self.credentials_df.set_index("Tipo")
        credentials = {
            "login": int(df.at["user", broker]),
            "password": df.at["password", broker],
            "server": df.at["Server", broker]
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
        self.logger.info(f"Procesando broker: {broker}")
        credentials = self.extract_credentials(broker)
        connection = MT5Connection(credentials)
        connection.initialize()

        symbols = mt5.symbols_get()
        if symbols is None:
            error_msg = f"No se pudieron obtener símbolos para broker {broker}"
            self.logger.error(error_msg)
            connection.shutdown()
            raise Exception(error_msg)

        symbol_list = [s.name for s in symbols]
        connection.shutdown()
        self.logger.info(f"Broker {broker}: {len(symbol_list)} símbolos encontrados.")

        broker_data_folder = os.path.join(self.data_folder, broker)
        os.makedirs(broker_data_folder, exist_ok=True)

        pool = mp.Pool(
            processes=mp.cpu_count(),
            initializer=worker_initializer,
            initargs=(credentials, broker_data_folder, (self.start_date, self.end_date))
        )

        results = []
        for result in tqdm(pool.imap_unordered(download_symbol_data, symbol_list),
                           total=len(symbol_list),
                           desc=f"Broker {broker}"):
            results.append(result)
            if result.get("error"):
                pool.terminate()
                error_info = f"Error en activo {result['symbol']}: {result['error']}"
                self.logger.error(error_info)
                raise Exception(error_info)
            self.logger.info(
                f"Activo {result['symbol']} descargado. Detalles: {result['log']}. Archivo: {result['file']}"
            )

        pool.close()
        pool.join()
        return results

    def process_all_brokers(self):
        """
        Procesa todos los brokers disponibles en el DataFrame.
        """
        broker_columns = [col for col in self.credentials_df.columns if col != "Tipo"]
        total_files_downloaded = 0

        for broker in broker_columns:
            try:
                results = self.process_broker(broker)
                total_files_downloaded += len(results)
            except Exception as e:
                self.logger.error(f"Excepción al procesar broker {broker}: {str(e)}")
                raise e

        self.logger.info(f"Total de archivos descargados: {total_files_downloaded}")


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
    log_folder = r"C:\Users\spinz\OneDrive\Documentos\Portafolio oficial\HERMESDB\HERMESDB\test\data\logs"
    data_folder = r"C:\Users\spinz\OneDrive\Documentos\Portafolio oficial\HERMESDB\HERMESDB\test\data\backup"

    start_date = datetime(2000, 1, 1)
    end_date = datetime.now()

    # En Windows es importante usar el método "spawn"
    mp.set_start_method("spawn", force=True)

    downloader = HistoricalDataDownloader(
        credentials_df=credentials_df,
        log_folder=log_folder,
        data_folder=data_folder,
        start_date=start_date,
        end_date=end_date
    )

    try:
        downloader.process_all_brokers()
    except Exception as e:
        print(f"Se produjo un error durante la descarga: {e}")
