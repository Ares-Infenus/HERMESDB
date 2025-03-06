# ----------------------------
# Descripcion
# ----------------------------

"""Este código define la clase HistoricalDataDownloader, encargada de gestionar la conexión con
múltiples brokers, extraer credenciales desde un DataFrame, obtener la lista de símbolos y
descargar datos históricos de manera eficiente utilizando multiprocessing. Al inicializarse,
configura los logs, crea la carpeta de almacenamiento y define un rango de fechas para la descarga.
Luego, extrae las credenciales de cada broker, se conecta a través de MetaTrader5, obtiene la lista
de activos disponibles y, mediante un pool de procesos, descarga los datos en paralelo para
optimizar el rendimiento. Además, maneja errores y registra el número total de archivos
descargados,asegurando una ejecución robusta y eficiente.
"""
# ----------------------------
# librerias y dependencias
# ----------------------------

# Standard library imports
import os
import multiprocessing as mp
from datetime import datetime

# Third-party imports
from tqdm import tqdm
import pandas as pd
import MetaTrader5 as mt5
from profiling_utils import mem_profile


# ----------------------------
# Conexiones
# ----------------------------

from .logger_config import setup_logger
from .mt5_connection import MT5Connection
from .worker import worker_initializer, download_symbol_data

# ----------------------------
# Codigo
# ----------------------------


class HistoricalDataDownloader:
    """
    Gestiona la extracción de credenciales, la conexión a los brokers y la descarga
    de datos históricos utilizando multiprocessing.
    """

    @mem_profile
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

    @mem_profile
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

    @mem_profile
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

    @mem_profile
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
