import pandas as pd
import numpy as np
import os
from datetime import datetime, timezone
from multiprocessing import Pool, cpu_count
from tqdm import tqdm
import pyarrow as pa
import pyarrow.csv as csv
from typing import Dict, List, Optional

class CSVProcessor:
    def __init__(self, input_folder: str, output_file: str, output_folder: str, assets_df: pd.DataFrame):
        self.input_folder = input_folder
        self.output_file = output_file
        self.output_folder = output_folder
        self.chunk_size = 500000  # Tamaño de cada fragmento para exportación
        self.current_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        # Precomputar el mapeo de activos para búsqueda más rápida
        self.assets_mapping = dict(zip(
            assets_df['ASSETS_NAME'],
            assets_df['ASSETS_ID']
        ))

        # Mapeo de timeframes precompilado
        self.timeframe_mapping = {
            "1H": 4,
            "4H": 5,
            "1D": 1,
            "1W": 3,
            "1M": 2
        }

        # Crear carpeta de salida si no existe
        os.makedirs(self.output_folder, exist_ok=True)

    def initialize_output_file(self):
        """Crear archivo de salida con encabezados."""
        headers = [
            "OPEN", "HIGH", "LOW", "CLOSE", "TICK_VOLUME", "SPREAD", "VOLUME", "DATE_RECORDED", "ASSETS_ID", "TIMEFRAME_ID",
            "DATA_ID", "CREATED_AT", "UPDATED_AT", "PRICE_TYPE"
        ]
        empty_data = {header: [] for header in headers}
        df = pd.DataFrame(empty_data)
        df.to_csv(self.output_file, index=False)

    def process_file(self, file_info: tuple) -> pd.DataFrame:
        """Procesar un solo archivo CSV y devolver un DataFrame."""
        file, start_id = file_info
        file_path = os.path.join(self.input_folder, file)

        try:
            # Leer archivo con PyArrow
            table = csv.read_csv(
                file_path,
                read_options=csv.ReadOptions(use_threads=True),
                parse_options=csv.ParseOptions(delimiter=','),
                convert_options=csv.ConvertOptions(
                    column_types=None  # PyArrow inferirá los tipos automáticamente
                )
            )

            # Convertir a DataFrame para transformaciones
            df = table.to_pandas()

            # Aplicar transformaciones
            df['DATE_RECORDED'] = pd.to_datetime(df['time']).dt.strftime("%Y-%m-%d %H:%M:%S")
            df['ASSETS_ID'] = df['Activo'].map(self.assets_mapping).fillna(-1)
            df['TIMEFRAME_ID'] = df['timeframe'].map(self.timeframe_mapping).fillna(-1)

            # Eliminar columnas originales innecesarias
            df.drop(['time', 'timeframe', 'Activo'], axis=1, inplace=True)

            # Agregar columnas adicionales
            n_rows = len(df)
            df['DATA_ID'] = np.arange(start_id, start_id + n_rows)
            df['CREATED_AT'] = self.current_timestamp
            df['UPDATED_AT'] = self.current_timestamp
            df['PRICE_TYPE'] = 'BID'

            return df

        except Exception as e:
            error_message = f"Error procesando {file}: {e}"
            with open('error_log.txt', 'a') as log_file:
                log_file.write(error_message + "\n")
            return pd.DataFrame()  # Retorna un DataFrame vacío en caso de error

    def split_and_export_dataframe(self, df: pd.DataFrame):
        """Dividir el DataFrame unificado en fragmentos y exportarlos a la carpeta de salida."""
        n_rows = len(df)
        num_chunks = (n_rows // self.chunk_size) + 1  # Calcular el número de fragmentos
        
        with tqdm(total=num_chunks, desc="Dividiendo y exportando fragmentos", ncols=100) as pbar:
            for i, chunk_start in enumerate(range(0, n_rows, self.chunk_size)):
                chunk_df = df.iloc[chunk_start:chunk_start + self.chunk_size]  # Definir el fragmento
                
                # Convertir los valores decimales al formato con punto
                chunk_df = chunk_df.apply(lambda col: col.map(lambda x: f"{x:.6f}" if isinstance(x, float) else x) if col.dtype == 'float64' else col)

                output_file = os.path.join(self.output_folder, f"unified_chunk_{i + 1}.csv")

                # Exportar el DataFrame con encabezado y datos al archivo CSV
                chunk_df.to_csv(
                    output_file,
                    sep=';',  # Separador de columnas
                    index=False,  # No incluir índices
                    quotechar='"',  # Agregar comillas a los datos
                    encoding='utf-8'  # Codificación UTF-8
                )

                # Actualizar la barra de progreso
                pbar.update(1)


    def process_files(self):
        """Procesar archivos en paralelo y dividir el DataFrame unificado en fragmentos."""
        self.initialize_output_file()

        # Obtener lista de archivos y calcular IDs iniciales
        files = [f for f in os.listdir(self.input_folder) if f.endswith('.csv')]
        cumulative_rows = 0
        file_info = []

        for file in files:
            # Estimar número de filas basado en conteo real
            n_rows = sum(1 for _ in open(os.path.join(self.input_folder, file))) - 1  # Sin encabezado
            file_info.append((file, cumulative_rows))
            cumulative_rows += n_rows

        # Procesar archivos en paralelo
        unified_df = pd.DataFrame()
        with Pool(processes=max(cpu_count() - 1, 1)) as pool:
            results = list(tqdm(
                pool.imap(self.process_file, file_info),
                total=len(files),
                desc="Procesando archivos"
            ))
            unified_df = pd.concat(results, ignore_index=True)

        # Dividir y exportar el DataFrame unificado
        self.split_and_export_dataframe(unified_df)

        print(f"Archivos divididos y exportados en: {self.output_folder}")

if __name__ == '__main__':
    assets_df = pd.read_csv("C:\\Users\\spinz\\Documents\\Portafolio Oficial\\HERMESDB\\data\\tables_data\\ASSETS.csv")
    input_folder = 'C:\\Users\\spinz\\Documents\\Portafolio Oficial\\HERMESDB\\data\\tables_data\\data_table\\data_market'
    output_file = 'C:\\Users\\spinz\\Documents\\Portafolio Oficial\\HERMESDB\\data\\tables_data\\MARKET_DATA_BID.csv'
    output_folder = 'C:\\Users\\spinz\\Documents\\Portafolio Oficial\\HERMESDB\\data\\tables_data\\processed_chunks'
    processor = CSVProcessor(input_folder, output_file, output_folder, assets_df)
    processor.process_files()
