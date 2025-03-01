import os
import logging
import multiprocessing as mp
import pandas as pd
import MetaTrader5 as mt5
from datetime import datetime, timedelta
from tqdm import tqdm

# En Windows es importante usar el método "spawn"
mp.set_start_method("spawn", force=True)

def generate_month_ranges(start_date, end_date):
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

# Función que se usará en cada worker para inicializar la conexión a MT5
def worker_initializer(credentials, data_folder, date_range):
    """
    Inicializa la conexión a MetaTrader5 en cada proceso worker.
    Se almacenan las credenciales, la carpeta de salida y el rango de fechas en variables globales.
    """
    global worker_credentials, worker_data_folder, worker_date_range
    worker_credentials = credentials
    worker_data_folder = data_folder
    worker_date_range = date_range

    init_args = {
        "server": worker_credentials["server"],
        "login": worker_credentials["login"],
        "password": worker_credentials["password"]
    }
    if "investor_password" in worker_credentials:
        init_args["investor_password"] = worker_credentials["investor_password"]

    if not mt5.initialize(**init_args):
        raise Exception(f"Worker: Falló la inicialización de MT5: {mt5.last_error()}")

def download_symbol_worker(symbol):
    """
    Función worker que descarga los datos históricos de un activo (symbol)
    para diferentes temporalidades, descargando mes a mes dentro del rango de fechas especificado.
    Se utiliza mt5.copy_rates_range para cada mes. Luego se concatenan los datos y se eliminan duplicados.
    Finalmente se guarda el resultado en un archivo CSV.
    """
    try:
        timeframes = {
            "H1": mt5.TIMEFRAME_H1,
            "H4": mt5.TIMEFRAME_H4,
            "D1": mt5.TIMEFRAME_D1,
            "W1": mt5.TIMEFRAME_W1,
            "MN1": mt5.TIMEFRAME_MN1
        }

        collected_dfs = []  # Lista para almacenar DataFrame de cada timeframe
        timeframe_log = {}  # Información para el log por timeframe

        start_date, end_date = worker_date_range
        month_ranges = generate_month_ranges(start_date, end_date)

        for tf_label, tf_value in timeframes.items():
            dfs = []
            # Se descarga mes a mes para cada timeframe
            for month_start, month_end in month_ranges:
                rates = mt5.copy_rates_range(symbol, tf_value, month_start, month_end)
                if rates is not None and len(rates) > 0:
                    df_temp = pd.DataFrame(rates)
                    dfs.append(df_temp)
            if dfs:
                df_tf = pd.concat(dfs, ignore_index=True)
                # Convertir la columna 'time' de epoch a datetime
                df_tf['time'] = pd.to_datetime(df_tf['time'], unit='s')
                df_tf['timeframe'] = tf_label

                first_date = df_tf['time'].min()
                last_date = df_tf['time'].max()
                rows = len(df_tf)
                timeframe_log[tf_label] = {
                    "rows": rows,
                    "first_date": str(first_date),
                    "last_date": str(last_date)
                }
                collected_dfs.append(df_tf)
            else:
                timeframe_log[tf_label] = {"rows": 0, "first_date": None, "last_date": None}

        if collected_dfs:
            final_df = pd.concat(collected_dfs, ignore_index=True)
            # Eliminar duplicados basados en 'time' y 'timeframe'
            final_df = final_df.drop_duplicates(subset=["time", "timeframe"], keep="first")
        else:
            final_df = pd.DataFrame()

        safe_symbol = symbol.replace("/", "_")
        output_file = os.path.join(worker_data_folder, f"{safe_symbol}.csv")
        final_df.to_csv(output_file, index=False)

        return {"symbol": symbol, "log": timeframe_log, "file": output_file, "error": None}

    except Exception as e:
        return {"symbol": symbol, "log": {}, "file": None, "error": str(e)}

class HistoricalDataDownloader:
    def __init__(self, credentials_df, log_folder, data_folder, start_date=None, end_date=None):
        """
        Inicializa el objeto con el DataFrame de credenciales,
        la carpeta para el log y la carpeta para guardar los archivos de datos.
        Se pueden especificar start_date y end_date para definir el rango de descarga.
        """
        self.credentials_df = credentials_df
        self.log_folder = log_folder
        self.data_folder = data_folder

        os.makedirs(self.log_folder, exist_ok=True)
        os.makedirs(self.data_folder, exist_ok=True)

        log_file = os.path.join(
            self.log_folder,
            f"download_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        )
        self.logger = logging.getLogger("DownloaderLogger")
        self.logger.setLevel(logging.INFO)
        fh = logging.FileHandler(log_file)
        fh.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        self.logger.addHandler(fh)

        self.start_date = start_date if start_date is not None else datetime(2000, 1, 1)
        self.end_date = end_date if end_date is not None else datetime.now()

    def extract_credentials(self, broker):
        """
        Extrae las credenciales del DataFrame para un broker dado.
        Se espera que el DataFrame tenga las filas 'user', 'password', 'Investor' y 'Server'.
        """
        df = self.credentials_df.set_index("Tipo")
        credentials = {}
        credentials["login"] = int(df.at["user", broker])
        credentials["password"] = df.at["password", broker]
        investor = df.at["Investor", broker]
        if pd.notna(investor):
            credentials["investor_password"] = investor
        credentials["server"] = df.at["Server", broker]
        return credentials

    def process_brokers(self):
        """
        Procesa cada broker del DataFrame: se conecta, obtiene la lista de activos y
        descarga los datos históricos de cada uno utilizando multiprocesamiento.
        Si ocurre un error se detiene el proceso.
        """
        broker_columns = [col for col in self.credentials_df.columns if col != "Tipo"]
        total_files_downloaded = 0

        for broker in broker_columns:
            self.logger.info(f"Procesando broker: {broker}")
            try:
                credentials = self.extract_credentials(broker)

                init_args = {
                    "server": credentials["server"],
                    "login": credentials["login"],
                    "password": credentials["password"]
                }
                if "investor_password" in credentials:
                    init_args["investor_password"] = credentials["investor_password"]

                if not mt5.initialize(**init_args):
                    err_msg = f"Falló la inicialización de MT5 para broker {broker}: {mt5.last_error()}"
                    self.logger.error(err_msg)
                    raise Exception(err_msg)

                symbols = mt5.symbols_get()
                if symbols is None:
                    err_msg = f"No se pudieron obtener símbolos para broker {broker}"
                    self.logger.error(err_msg)
                    raise Exception(err_msg)

                symbol_list = [s.name for s in symbols]
                mt5.shutdown()
                self.logger.info(f"Broker {broker}: {len(symbol_list)} símbolos encontrados.")

                broker_data_folder = os.path.join(self.data_folder, broker)
                os.makedirs(broker_data_folder, exist_ok=True)

                pool = mp.Pool(
                    processes=mp.cpu_count(),
                    initializer=worker_initializer,
                    initargs=(credentials, broker_data_folder, (self.start_date, self.end_date))
                )

                results = []
                for result in tqdm(
                    pool.imap_unordered(download_symbol_worker, symbol_list),
                    total=len(symbol_list),
                    desc=f"Broker {broker}"
                ):
                    results.append(result)
                    if result.get("error"):
                        pool.terminate()
                        error_info = f"Error en activo {result['symbol']}: {result['error']}"
                        self.logger.error(error_info)
                        raise Exception(error_info)
                    else:
                        self.logger.info(
                            f"Activo {result['symbol']} descargado. Detalles: {result['log']}. Archivo: {result['file']}"
                        )
                        total_files_downloaded += 1

                pool.close()
                pool.join()

            except Exception as e:
                self.logger.error(f"Excepción al procesar broker {broker}: {str(e)}")
                raise e

        self.logger.info(f"Total de archivos descargados: {total_files_downloaded}")

# ======================================================
# Ejemplo de uso
# ======================================================
#if __name__ == "__main__":
#    data = {
#        "Tipo": ["user", "password", "Investor", "Server"],
#        "Dukascopy": [561753946, "=UcA9-:c", float("nan"), "Dukascopy-demo-mt5-1"],
#    }
#    credentials_df = pd.DataFrame(data)#

# #   log_folder = "C:\\Users\\spinz\\OneDrive\\Documentos\\Portafolio oficial\\HERMESDB\\HERMESDB\\test\\data\\logs"
# #   data_folder = "C:\\Users\\spinz\\OneDrive\\Documentos\\Portafolio oficial\\HERMESDB\\HERMESDB\\test\\data\\external"

#    # Especificamos el rango de fechas (por ejemplo, desde el 1 de enero de 2020 hasta hoy)
#    start_date = datetime(2000, 1, 1)
#    end_date = datetime.now()

#    downloader = HistoricalDataDownloader(credentials_df, log_folder, data_folder, start_date, end_date)

#    try:
#        downloader.process_brokers()
#    except Exception as e:
##        print(f"Se produjo un error durante la descarga: {e}")
