import os
import pandas as pd
import MetaTrader5 as mt5
from tqdm import tqdm
import multiprocessing as mp
import atexit
from datetime import datetime

# Variable global para almacenar las credenciales actuales en el proceso
_CURRENT_CREDENTIALS = None

# Función que se ejecutará al terminar el proceso para cerrar MT5
def shutdown_mt5():
    mt5.shutdown()

atexit.register(shutdown_mt5)

def worker_process_file(task):
    """
    Función worker para procesar un archivo CSV.
    
    Parámetros en task:
      - broker: Nombre del broker.
      - credentials: Credenciales para conectar a MT5.
      - file_path: Ruta completa del archivo CSV.
      - timeframes: Diccionario de temporalidades.
      
    Retorna:
      - Una lista de diccionarios con los resultados para cada timeframe.
    """
    global _CURRENT_CREDENTIALS
    broker, credentials, file_path, timeframes = task
    symbol = os.path.basename(file_path).replace(".csv", "").replace("_", "/")
    
    # Verifica si ya se inició una conexión con las mismas credenciales
    if _CURRENT_CREDENTIALS != credentials:
        # Si hay una conexión previa, se cierra
        if _CURRENT_CREDENTIALS is not None:
            mt5.shutdown()
        if not mt5.initialize(**credentials):
            error_message = mt5.last_error()
            return [
                {
                    "Broker": broker,
                    "Symbol": symbol,
                    "Timeframe": tf_label,
                    "Available_Rows": None,
                    "Downloaded_Rows": None,
                    "Match": False,
                    "Error": f"Error al conectar con MT5: {error_message}"
                }
                for tf_label in timeframes.keys()
            ]
        _CURRENT_CREDENTIALS = credentials
    
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        return [
            {
                "Broker": broker,
                "Symbol": symbol,
                "Timeframe": tf_label,
                "Available_Rows": None,
                "Downloaded_Rows": None,
                "Match": False,
                "Error": f"Error al leer CSV: {str(e)}"
            }
            for tf_label in timeframes.keys()
        ]
    
    results = []
    # Para cada timeframe, comparar los datos descargados con los disponibles en MT5
    for timeframe_label, timeframe_value in timeframes.items():
        df_tf = df[df["timeframe"] == timeframe_label]
        if df_tf.empty:
            continue
        
        # Se utiliza la fecha más reciente disponible en el CSV para limitar la consulta
        last_date = pd.to_datetime(df_tf["time"].max())
        first_date = pd.to_datetime(df_tf["time"].min())
        
        # Descargar todo el historial disponible
        available_rates = mt5.copy_rates_range(symbol, timeframe_value, first_date, last_date)
        available_rows = len(available_rates) if available_rates is not None else 0
        downloaded_rows = len(df_tf)
        
        results.append({
            "Broker": broker,
            "Symbol": symbol,
            "Timeframe": timeframe_label,
            "Available_Rows": available_rows,
            "Downloaded_Rows": downloaded_rows,
            "Match": available_rows == downloaded_rows
        })
    
    return results


class MT5ExtractObserver:
    def __init__(self, credentials_df, data_folder):
        """
        Inicializa la clase con las credenciales de los brokers y la carpeta de datos.
        :param credentials_df: DataFrame con las credenciales de acceso a MT5.
        :param data_folder: Ruta donde se almacenan los archivos descargados.
        """
        self.credentials_df = credentials_df
        self.data_folder = data_folder
        self.timeframes = {
            "H1": mt5.TIMEFRAME_H1,
            "H4": mt5.TIMEFRAME_H4,
            "D1": mt5.TIMEFRAME_D1,
            "W1": mt5.TIMEFRAME_W1,
            "MN1": mt5.TIMEFRAME_MN1
        }

    def extract_credentials(self, broker):
        """Extrae las credenciales del DataFrame para un broker dado."""
        df = self.credentials_df.set_index("Tipo")
        credentials = {
            "login": int(df.at["user", broker]),
            "password": df.at["password", broker],
            "server": df.at["Server", broker]
        }
        if pd.notna(df.at["Investor", broker]):
            credentials["investor_password"] = df.at["Investor", broker]
        return credentials

    def check_extracted_data(self):
        """
        Verifica si los datos descargados coinciden con los disponibles en MT5.
        Devuelve (1, df_result) si todo está correcto, o (0, incorrect_df) si hay inconsistencias.
        Se utiliza multiprocesamiento para procesar cada archivo CSV en paralelo.
        """
        broker_columns = [col for col in self.credentials_df.columns if col != "Tipo"]
        tasks = []

        # Preparar la lista de tareas (una por archivo CSV)
        for broker in broker_columns:
            credentials = self.extract_credentials(broker)
            broker_folder = os.path.join(self.data_folder, broker)
            if not os.path.exists(broker_folder):
                continue

            csv_files = [file for file in os.listdir(broker_folder) if file.endswith(".csv")]
            for file in csv_files:
                file_path = os.path.join(broker_folder, file)
                tasks.append((broker, credentials, file_path, self.timeframes))

        results = []
        # Se crea un pool de procesos utilizando todos los núcleos menos 1
        pool = mp.Pool(processes=mp.cpu_count() - 1)
        # Procesamos las tareas en paralelo y mostramos una barra de progreso
        for task_result in tqdm(pool.imap_unordered(worker_process_file, tasks), total=len(tasks), desc="Procesando archivos", unit="archivo"):
            results.extend(task_result)
        pool.close()
        pool.join()

        df_results = pd.DataFrame(results)
        incorrect_df = df_results[df_results["Match"] == False]

        return (1, df_results) if incorrect_df.empty else (0, incorrect_df)


# ------------------------------
# Ejemplo de uso
# ------------------------------

if __name__ == "__main__":
    import multiprocessing as mp 
    mp.set_start_method("spawn", force=True)  # Asegura el método spawn en Windows
    
    # Ejemplo de DataFrame de credenciales
    data = {
        "Tipo": ["user", "password", "Investor", "Server"],
        "Darwinex": [3000076243, "@8CTdW@HRb", "inv_pass1", "demoUK-mt5.darwinex.com"],
        "Oanda": [6364744, "WgRp-0Ny", None, "OANDA-Demo-1"]
    }
    credentials_df = pd.DataFrame(data)

    # Ruta de la carpeta donde están los CSV
    data_folder = r"C:\Users\spinz\OneDrive\Documentos\Portafolio oficial\HERMESDB\HERMESDB\test\data\external"

    # Crear la instancia pasando ambos argumentos
    observer = MT5ExtractObserver(credentials_df, data_folder)

    # Ejecutar la verificación de los datos
    status, df_result = observer.check_extracted_data()

    if status == 1:
        print("✅ Todos los archivos tienen la cantidad correcta de datos.")
    else:
        print("⚠️ Algunos archivos no tienen la cantidad esperada de datos.")
        print(df_result)