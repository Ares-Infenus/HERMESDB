import pandas as pd
import numpy as np
from multiprocessing import Pool, cpu_count
from tqdm import tqdm

class DataCleaner:
    """
    Clase para limpiar los DataFrames dentro de un diccionario anidado que representa archivos financieros.

    Objetivo:
    El propósito de esta clase es limpiar los DataFrames que contienen datos financieros, eliminando o imputando
    valores inservibles (como NaN o inf) y reemplazándolos con valores válidos (por ejemplo, el promedio de los valores
    adyacentes). También utiliza multiprocesamiento para acelerar el proceso y maneja errores que puedan ocurrir durante
    la limpieza de los datos.

    Métodos:
    - clean_dataframe(df): Limpia un DataFrame individual.
    - process_file(file_info): Procesa un archivo de datos, limpiando su DataFrame correspondiente.
    - clean_data(data_dict): Limpia todos los archivos de datos dentro del diccionario, utilizando multiprocesamiento.
    """
    
    def __init__(self, max_missing_allowed=2):
        """
        Inicializa la clase DataCleaner con un valor máximo permitido para los valores faltantes.

        Parámetros:
        max_missing_allowed (int): Número máximo de valores faltantes permitidos en una fila antes de ser eliminada.
        """
        self.max_missing_allowed = max_missing_allowed

    def clean_dataframe(self, df):
        """
        Limpia un DataFrame, eliminando o reemplazando los valores inservibles.

        - Reemplaza inf y -inf por NaN.
        - Elimina filas con más de 'max_missing_allowed' valores NaN.
        - Rellena los valores NaN restantes con el promedio de las columnas adyacentes.
        - Lanza un error si después de la limpieza hay valores NaN restantes.

        Parámetros:
        df (DataFrame): El DataFrame que se limpiará.

        Retorna:
        tuple: El DataFrame limpio y el número de filas eliminadas.
        """
        initial_rows = len(df)
        
        # Reemplazar inf y -inf por NaN
        df.replace([np.inf, -np.inf], np.nan, inplace=True)
        
        # Eliminar filas con más de 'max_missing_allowed' valores NaN
        df.dropna(thresh=len(df.columns) - self.max_missing_allowed, inplace=True)
        
        # Reemplazar los valores NaN restantes con el promedio de las columnas adyacentes
        df.fillna(df.mean(), inplace=True)
        
        # Validar que no haya valores NaN después de la limpieza
        if df.isnull().values.any():
            raise ValueError("DataFrame contains NaN values after cleaning.")
        
        final_rows = len(df)
        return df, initial_rows - final_rows  # Devolver el DataFrame limpio y el número de filas eliminadas

    def process_file(self, file_info):
        """
        Procesa un archivo de datos (DataFrame) y lo limpia.

        Parámetros:
        file_info (tuple): Tupla que contiene la clave del archivo y su DataFrame.

        Retorna:
        tuple: La clave del archivo, el DataFrame limpio, y el número de filas eliminadas.
        """
        key, df = file_info
        try:
            cleaned_df, rows_removed = self.clean_dataframe(df)
            return key, cleaned_df, rows_removed
        except Exception as e:
            print(f"Error processing {key}: {e}")
            return key, None, 0

    def clean_data(self, data_dict):
        """
        Limpia todos los DataFrames dentro del diccionario de datos, utilizando multiprocesamiento.

        Parámetros:
        data_dict (dict): Diccionario anidado que contiene las claves de los archivos y sus DataFrames.

        Retorna:
        dict: Diccionario con los DataFrames limpios.
        """
        # Generar lista de archivos para procesar (tuplas con clave y DataFrame)
        files = [(f"{category}/{subcat}/{timeframe}/{filename}", df) 
                 for category, subcats in data_dict.items() 
                 for subcat, timeframes in subcats.items() 
                 for timeframe, files in timeframes.items() 
                 for filename, df in files.items()]

        # Usar multiprocesamiento para procesar los archivos en paralelo
        with Pool(cpu_count() - 1) as pool:
            results = list(tqdm(pool.imap(self.process_file, files), total=len(files)))

        # Crear un diccionario con los datos limpios
        cleaned_data = {}
        total_rows_removed = 0
        total_files_processed = 0

        for key, cleaned_df, rows_removed in results:
            if cleaned_df is not None and not cleaned_df.empty:
                # Estructurar los datos limpios en un diccionario anidado
                category, subcat, timeframe, filename = key.split('/')
                if category not in cleaned_data:
                    cleaned_data[category] = {}
                if subcat not in cleaned_data[category]:
                    cleaned_data[category][subcat] = {}
                if timeframe not in cleaned_data[category][subcat]:
                    cleaned_data[category][subcat][timeframe] = {}
                cleaned_data[category][subcat][timeframe][filename] = cleaned_df
                total_rows_removed += rows_removed
                total_files_processed += 1
            else:
                print(f"File {key} is empty after cleaning and has been removed.")

        # Imprimir el reporte final
        print(f"Total files processed: {total_files_processed}")
        print(f"Total rows removed: {total_rows_removed}")

        return cleaned_data


# Ejemplo de uso
#if __name__ == "__main__":
#    # Diccionario de ejemplo con datos financieros
#    data_dict = {
#        'forex': {
#            'eurusd': {
#                '1h': {
#                    'EURUSD_1H_ASK.csv': pd.DataFrame({
#                        'Open': [1.1, np.nan, 1.3, np.inf, 1.5],
#                        'High': [1.2, 1.4, np.nan, 1.6, 1.8],
#                        'Low': [1.0, 1.1, 1.2, np.inf, 1.4],
#                        'Close': [1.15, 1.2, 1.25, 1.3, 1.35],
#                        'Volume': [100, 200, np.nan, 400, 500]
#                    })
#                }
#            }
#       },
#        'metals': {},
#        'crypto': {}
#    }

    # Crear una instancia de la clase DataCleaner y limpiar los datos
#    cleaner = DataCleaner(max_missing_allowed=2)
#    cleaned_data = cleaner.clean_data(data_dict)
    
    # Mostrar los resultados
#    print(f"Cleaned data: {cleaned_data}")
