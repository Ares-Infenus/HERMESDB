import pandas as pd
import numpy as np
import os
from datetime import datetime
from multiprocessing import Pool, cpu_count
from tqdm import tqdm
import pyarrow as pa
import pyarrow.csv as csv
from typing import Dict, List, Optional

class CSVProcessor:
    def __init__(self, input_folder: str, output_file: str, assets_df: pd.DataFrame):
        self.input_folder = input_folder
        self.output_file = output_file
        self.chunksize = 500000
        self.current_timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        
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
        
        # Esquema para PyArrow
        self.schema = pa.schema([
            ('time', pa.string()),
            ('open', pa.float64()),
            ('high', pa.float64()),
            ('low', pa.float64()),
            ('close', pa.float64()),
            ('tick_volume', pa.int64()),
            ('spread', pa.float64()),
            ('real_volume', pa.float64()),
            ('timeframe', pa.string()),
            ('Activo', pa.string())
        ])

    def initialize_output_file(self):
        """Crear archivo de salida con encabezados usando PyArrow."""
        headers = [
            "DATA_ID", "DATE_RECORDED", "OPEN", "HIGH", "LOW", "CLOSE",
            "TICK_VOLUME", "SPREAD", "VOLUME", "TIMEFRAME_ID", "ASSETS_ID",
            "CREATED_AT", "UPDATED_AT", "PRICE_TYPE"
        ]
        empty_data = {header: [] for header in headers}
        df = pd.DataFrame(empty_data)
        df.to_csv(self.output_file, index=False)

    def process_file(self, file_info: tuple) -> str:
        """Procesar un solo archivo CSV usando PyArrow para lectura más rápida."""
        file, start_id = file_info
        file_path = os.path.join(self.input_folder, file)
        
        try:
            # Leer archivo con PyArrow
            table = csv.read_csv(
                file_path,
                read_options=csv.ReadOptions(use_threads=True),
                parse_options=csv.ParseOptions(delimiter=','),
                convert_options=csv.ConvertOptions(
                    column_types=self.schema,
                    include_columns=self.schema.names
                )
            )
            
            # Convertir a DataFrame solo para las operaciones necesarias
            df = table.to_pandas()
            
            # Aplicar transformaciones vectorizadas
            df['DATE_RECORDED'] = pd.to_datetime(df['time']).dt.strftime("%Y-%m-%d %H:%M:%S")
            df['ASSETS_ID'] = df['Activo'].map(self.assets_mapping).fillna(-1)
            df['TIMEFRAME_ID'] = df['timeframe'].map(self.timeframe_mapping).fillna(-1)
            
            # Agregar columnas adicionales de manera vectorizada
            n_rows = len(df)
            df['DATA_ID'] = np.arange(start_id, start_id + n_rows)
            df['CREATED_AT'] = self.current_timestamp
            df['UPDATED_AT'] = self.current_timestamp
            df['PRICE_TYPE'] = 'BID'
            
            # Eliminar columna ASSETS_NAME
            df.drop('Activo', axis=1, inplace=True)
            
            # Guardar resultados concatenados al archivo existente
            df.to_csv(self.output_file, mode='a', index=False, header=False)
            
            return f"{file} procesado correctamente. Filas: {n_rows}"
            
        except Exception as e:
            error_message = f"Error procesando {file}: {e}"
            with open('error_log.txt', 'a') as log_file:
                log_file.write(error_message + "\n")
            return error_message

    def process_files(self):
        """Procesar archivos en paralelo con mejor manejo de IDs."""
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
        with Pool(processes=max(cpu_count() - 1, 1)) as pool:
            results = list(tqdm(
                pool.imap(self.process_file, file_info),
                total=len(files),
                desc="Procesando archivos"
            ))
        
        # Mostrar resultados
        for result in results:
            print(result)
        
        print(f"Archivos combinados en: {self.output_file}")
        
if __name__ == '__main__':
    assets_df = pd.read_csv("C:\\Users\\spinz\\Documents\\Portafolio Oficial\\HERMESDB\data\\tables_data\\ASSETS.csv")
    input_folder = 'C:\\Users\\spinz\\Documents\\Portafolio Oficial\\HERMESDB\\data\\tables_data\\data_table\\data_market\\test'
    output_file = 'C:\\Users\\spinz\\Documents\\Portafolio Oficial\\HERMESDB\\data\\tables_data\\MARKET_DATA_BID.csv'
    processor = CSVProcessor(input_folder, output_file, assets_df)
    processor.process_files()
