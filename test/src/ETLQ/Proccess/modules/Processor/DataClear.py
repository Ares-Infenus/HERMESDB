import os
import pandas as pd
import numpy as np
from tqdm import tqdm  # Barra de progreso
import multiprocessing

def process_broker(args):
    """
    Función para procesar un broker (carpeta) completa.
    Se reciben los parámetros necesarios en una tupla: (input_dir, output_dir, broker, config)
    """
    input_dir, output_dir, broker, config = args
    broker_path = os.path.join(input_dir, broker)
    output_file = os.path.join(output_dir, f"{broker}.csv")
    
    # Eliminar archivo previo si existe
    if os.path.exists(output_file):
        os.remove(output_file)
    
    # Obtener la lista de archivos CSV en la carpeta del broker
    csv_files = [f for f in os.listdir(broker_path) if f.lower().endswith('.csv')]
    print(f"Procesando broker: {broker} con {len(csv_files)} archivos.")
    
    for file_name in tqdm(csv_files, desc=f"Procesando {broker}", unit="archivo"):
        file_path = os.path.join(broker_path, file_name)
        try:
            # Leer CSV con bajo consumo de memoria
            df = pd.read_csv(file_path, low_memory=True)
            
            # Asegurar que la columna 'time' tenga el formato adecuado:
            # Si la fecha no contiene hora, se le añade " 00:00:00"
            df['time'] = df['time'].apply(lambda x: x if " " in str(x) else f"{x} 00:00:00")
            
            # Convertir la columna 'time' a datetime, permitiendo inferir el formato
            df['time'] = pd.to_datetime(df['time'], errors='coerce', infer_datetime_format=True)
            
            # Obtener offset para el broker (si existe) y ajustar a UTC
            offset = config["timezone_offsets"].get(broker, 0)
            if offset != 0:
                tz_str = f'Etc/GMT{-offset}'
                df['time'] = df['time'].dt.tz_localize(tz_str, ambiguous='NaT', nonexistent='NaT').dt.tz_convert('UTC')
            else:
                df['time'] = df['time'].dt.tz_localize('UTC')
            
            # Limpiar datos: reemplazar Inf y -Inf, eliminar filas con NaN en columnas OHLC
            df.replace([np.inf, -np.inf], np.nan, inplace=True)
            df.dropna(subset=config["columns_to_clean"], inplace=True)
            
            # Agregar información adicional
            df['broker'] = broker
            asset_name = os.path.splitext(file_name)[0]
            df['asset'] = asset_name
            
            # Guardar el DataFrame procesado de forma incremental
            if not os.path.exists(output_file):
                df.to_csv(output_file, index=False)
            else:
                df.to_csv(output_file, mode='a', header=False, index=False)
        
        except Exception as e:
            print(f"Error al procesar el archivo {file_path}: {e}")
    
    print(f"Broker '{broker}' procesado. Archivo de salida: {output_file}")
    return output_file

class DataCleaner:
    """
    Clase para limpiar y unificar archivos CSV de datos históricos de distintos brokers.

    Requisitos:
      - Eliminar filas con valores no válidos (NaN, Inf, -Inf) únicamente en las columnas OHLC.
      - Convertir la columna 'time' al formato datetime64 y ajustar la hora a UTC.
      - Unificar los archivos CSV de cada broker (carpeta) en un único CSV y guardarlo en disco.
      - Optimización en el uso de memoria, procesando archivo por archivo.
      
    Se asume que:
      - La estructura de carpetas es: 
            input_dir/
                Broker1/   --> archivos CSV de activos de Broker1
                Broker2/   --> archivos CSV de activos de Broker2
                ...
      - Cada CSV tiene la siguiente estructura:
            time,open,high,low,close,tick_volume,spread,real_volume,timeframe
      - La columna 'time' puede venir con hora ("YYYY-MM-DD HH:MM:SS") o sin ella ("YYYY-MM-DD").
      - Cada broker utiliza una hora local que corresponde a UTC+2.
    """
    
    def __init__(self, input_dir, output_dir, config=None):
        self.input_dir = input_dir
        self.output_dir = output_dir
        # Configuración por defecto
        self.config = config if config is not None else {
            "columns_to_clean": ['open', 'high', 'low', 'close'],
            "timezone_offsets": {
                "Dukascopy": 2,
                "Darwinex": 2,
                "Pepperstone": 2,
                "Tickmill": 2,
                "Oanda": 2
            }
        }
        
        # Lista de brokers permitidos
        self.allowed_brokers = {"Darwinex", "Pepperstone", "Tickmill", "Oanda", "Dukascopy"}
        
        # Crear directorio de salida si no existe
        os.makedirs(self.output_dir, exist_ok=True)
    
    def process(self):
        """
        Recorre cada subcarpeta (broker) en input_dir, procesa cada archivo CSV y guarda
        un CSV unificado y limpio por broker en output_dir, utilizando procesamiento en paralelo.
        """
        # Listar subdirectorios (brokers permitidos)
        brokers = [d for d in os.listdir(self.input_dir) if os.path.isdir(os.path.join(self.input_dir, d))]
        brokers = [broker for broker in brokers if broker in self.allowed_brokers]
        
        # Preparar argumentos para cada broker
        args_list = [(self.input_dir, self.output_dir, broker, self.config) for broker in brokers]
        
        # Dejar un núcleo libre para evitar sobrecarga
        num_workers = max(multiprocessing.cpu_count() - 1, 1)
        print(f"Usando {num_workers} procesos en paralelo.")
        
        with multiprocessing.Pool(processes=num_workers) as pool:
            results = pool.map(process_broker, args_list)
        
        return results

#if __name__ == "__main__":
    # Ejemplo de uso:
    # Define la ruta donde están los datos de entrada (estructura de brokers en subcarpetas)
    # input_dir = r"C:\ruta\a\tu\input_dir"
    
    # Define la ruta de salida donde se guardarán los CSV unificados y limpios
    # output_dir = r"C:\ruta\a\tu\output_dir"
    
    # Crear instancia de la clase y ejecutar el proceso
    # cleaner = DataCleaner(input_dir, output_dir)
    # cleaned_files = cleaner.process()
    # print("Proceso completado. Archivos generados:", cleaned_files)
