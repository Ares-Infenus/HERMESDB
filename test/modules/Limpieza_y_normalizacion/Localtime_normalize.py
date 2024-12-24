import pandas as pd
import pytz
from datetime import datetime

def process_dataframe(df, file_path):
    """
    Procesa un DataFrame de un archivo CSV con la estructura esperada.
    
    1. Convierte la columna 'Local time' a formato de fecha y hora en UTC.
    2. Establece la columna 'Date' como el índice del DataFrame.
    3. Ordena el DataFrame por fecha de manera descendente.
    
    Parámetros:
    df (pd.DataFrame): El DataFrame que contiene los datos a procesar.
    file_path (str): La ruta del archivo que se está procesando, utilizada para identificar el archivo en los mensajes de error.
    
    Retorna:
    pd.DataFrame: El DataFrame procesado con la fecha en formato UTC como índice, o None si ocurre un error.
    
    Ejemplo:
    >>> df = pd.DataFrame({
            'Local time': ['01.01.2020 00:00:00.000 UTC', '02.01.2020 00:00:00.000 UTC'],
            'Price': [7000, 7100]
        })
    >>> process_dataframe(df, "crypto/BTCUSD/1D/BTCUSD_1D_ASK.csv")
    """
    try:
        # Convertir la columna 'Local time' a formato datetime y ajustarlo a UTC
        df['Local time'] = df['Local time'].str.replace('GMT', '', regex=False)
        df['Local time'] = df['Local time'].str.replace(' UTC', ' +0000', regex=False)
        df['Date'] = pd.to_datetime(df['Local time'], format='%d.%m.%Y %H:%M:%S.%f %z').dt.tz_convert('UTC')
        
        # Eliminar la columna 'Local time' y usar 'Date' como índice
        df = df.drop(columns=['Local time'])
        df = df.set_index('Date')
        
        # Ordenar el DataFrame por la fecha en orden descendente
        df = df.sort_index(ascending=False)
        
        return df
    except Exception as e:
        # Si ocurre un error durante el procesamiento, imprimir el error y devolver None
        print(f"Error processing file {file_path}: {e}")
        return None

def process_hierarchical_dict(data_dict):
    """
    Procesa un diccionario jerárquico que contiene datos de activos financieros y los procesa 
    en función de su formato. Se recorre cada archivo CSV y se aplica el procesamiento 
    de la función 'process_dataframe'. Si el archivo no se puede procesar correctamente, 
    se imprime un mensaje de error detallando la ubicación del problema.

    1. Verifica si los activos o archivos están vacíos y notifica si se encuentran problemas.
    2. Procesa cada archivo CSV y lo agrega al nuevo diccionario si el procesamiento es exitoso.
    3. Continúa el procesamiento sin interrumpir el flujo, incluso si se encuentran errores.

    Parámetros:
    data_dict (dict): Un diccionario jerárquico que contiene los datos organizados por carpeta, activo, intervalo y archivo.
    
    Retorna:
    dict: Un nuevo diccionario con los archivos procesados correctamente.
    
    Ejemplo:
    >>> data_dict = {
            'crypto': {
                'BTCUSD': {
                    '1D': {
                        'BTCUSD_1D_ASK.csv': pd.DataFrame({
                            'Local time': ['01.01.2020 00:00:00.000 UTC', '02.01.2020 00:00:00.000 UTC'],
                            'Price': [7000, 7100]
                        })
                    }
                }
            }
        }
    >>> new_data_dict = process_hierarchical_dict(data_dict)
    """
    new_dict = {}
    
    # Recorre el diccionario jerárquico por carpetas
    for folder, assets in data_dict.items():
        if not assets:
            print(f"Carpeta vacía o archivo no compatible, revise documentación: {folder}")
            continue
        
        new_dict[folder] = {}
        
        # Recorre los activos dentro de cada carpeta
        for asset, intervals in assets.items():
            if not intervals:
                print(f"Carpeta vacía o archivo no compatible, revise documentación: {folder}/{asset}")
                continue
            
            new_dict[folder][asset] = {}
            
            # Recorre los intervalos de tiempo dentro de cada activo
            for interval, files in intervals.items():
                if not files:
                    print(f"Carpeta vacía o archivo no compatible, revise documentación: {folder}/{asset}/{interval}")
                    continue
                
                new_dict[folder][asset][interval] = {}
                
                # Recorre los archivos CSV dentro de cada intervalo
                for file_name, df in files.items():
                    if not isinstance(df, pd.DataFrame):
                        print(f"Carpeta vacía o archivo no compatible, revise documentación: {folder}/{asset}/{interval}/{file_name}")
                        continue
                    
                    # Procesar el DataFrame
                    processed_df = process_dataframe(df, f"{folder}/{asset}/{interval}/{file_name}")
                    
                    # Si el procesamiento es exitoso, agregar el archivo procesado al nuevo diccionario
                    if processed_df is not None:
                        new_dict[folder][asset][interval][file_name] = processed_df
    
    return new_dict

# Ejemplo de uso:
data_dict = {
    'crypto': {
        'BTCUSD': {
            '1D': {
                'BTCUSD_1D_ASK.csv': pd.DataFrame({
                    'Local time': ['01.01.2020 00:00:00.000 UTC', '02.01.2020 00:00:00.000 UTC'],
                    'Price': [7000, 7100]
                })
            }
        }
    }
}
new_data_dict = process_hierarchical_dict(data_dict)
print(new_data_dict)
