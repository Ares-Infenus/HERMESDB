import os
import pandas as pd
from tqdm import tqdm
import psutil
import re

class CSVToPostgresAdapter:
    def __init__(self, folder_path, chunksize=100000, spread_divisor=10**5, 
                 auto_adjust=False, safety_factor=0.5, sample_size=1000,
                 assets_df=None, brokers_df=None):
        """
        Inicializa la clase con la ruta de la carpeta donde se encuentran los CSV.
        Además se define el tamaño de chunk para leer archivos grandes con recursos limitados.
        
        Parámetros: 
          - folder_path: Ruta de la carpeta que contiene los CSV.
          - chunksize: Tamaño de chunk (número de filas). Si auto_adjust es True, este valor se calculará automáticamente.
          - spread_divisor: Divisor para ajustar el spread (por defecto 10^5).
          - auto_adjust: Si es True, se estima automáticamente el chunk size en función de la memoria disponible.
          - safety_factor: Margen de seguridad para no utilizar toda la memoria libre (valor entre 0 y 1).
          - sample_size: Número de filas a tomar para estimar el uso de memoria por fila.
          - assets_df: DataFrame con la información de Assets (debe tener las columnas ["activo_id", "simbolo"]).
          - brokers_df: DataFrame con la información de Brokers (debe tener las columnas ["broker_id", "nombre"]).
          
        Se definen:
         - Los nombres de archivos permitidos.
         - El divisor para el spread.
         - Un diccionario para convertir el timeframe.
        """
        self.folder_path = folder_path
        self.chunksize = chunksize  # Tamaño inicial del chunk
        self.spread_divisor = spread_divisor
        self.safety_factor = safety_factor
        self.sample_size = sample_size
        
        # DataFrames de Assets y Brokers (se deben importar previamente o pasar como argumento)
        self.assets_df = assets_df
        self.brokers_df = brokers_df
        
        # Archivos permitidos (nombre exacto, incluyendo extensión .csv)
        self.allowed_files = ['Darwinex.csv', 'Pepperstone.csv', 'Tickmill.csv', 'Dukascopy.csv', 'Oanda.csv']
        
        # Valor por defecto para mercado_id (no se provee en el CSV)
        self.default_mercado_id = 1
        
        # Mapeo para el timeframe: por ejemplo, "H1" se convierte en "1h"
        self.timeframe_mapping = {
            'H1': '1h',
            'M1': '1m',
            'M5': '5m',
            'M15': '15m',
            'M30': '30m',
            'H4': '4h',
            'D1': '1d',
            'W1': '1w',
            'MN': '1M'
        }
        
        # Si se desea ajustar el chunksize automáticamente, se estima con la primera muestra de un CSV permitido.
        if auto_adjust:
            all_files = os.listdir(self.folder_path)
            csv_files = [os.path.join(self.folder_path, f) for f in all_files if f in self.allowed_files]
            if csv_files:
                optimal_chunk = self._estimate_chunk_size(csv_files[0], sample_size, safety_factor)
                self.chunksize = optimal_chunk
                print(f"Chunk size ajustado a: {optimal_chunk} filas según recursos disponibles.")
            else:
                print("No se encontraron archivos permitidos para estimar el chunksize.")
    
    def _estimate_chunk_size(self, file_path, sample_size, safety_factor):
        """
        Estima el número óptimo de filas por chunk basado en la memoria disponible y un margen de seguridad.
        """
        # Leer una muestra del CSV
        sample = pd.read_csv(file_path, nrows=sample_size)
        # Calcular la memoria total usada por la muestra
        sample_memory = sample.memory_usage(deep=True).sum()
        # Estimar el uso de memoria por fila
        memory_per_row = sample_memory / sample_size
        # Obtener la memoria disponible en bytes
        available_memory = psutil.virtual_memory().available
        # Aplicar el safety_factor para no usar toda la memoria libre
        target_memory = available_memory * safety_factor
        # Calcular el número óptimo de filas
        optimal_chunk_size = int(target_memory // memory_per_row)
        return optimal_chunk_size
    
    def _process_chunk(self, df):
        """
        Procesa un chunk (subconjunto del CSV) realizando las transformaciones:
        - Renombrar columnas para adaptarlas a la estructura deseada.
        - Calcular el precio Ask usando la fórmula: Ask = Bid + (spread / spread_divisor)
        - Convertir el timeframe y asignar los valores originales para broker y asset.
        - Agregar la columna mercado_id.
        - Convertir el timestamp a datetime.
        - Eliminar columnas innecesarias.
        - **Reemplazar los valores de 'activo_id' y 'broker_id' utilizando los dataframes Assets y Brokers.**
        """
        # Renombrar columnas
        df.rename(columns={
            'time': 'timestamp',
            'open': 'bid_open',
            'high': 'bid_high',
            'low': 'bid_low',
            'close': 'bid_close',
            'tick_volume': 'volumen_contratos',
            'spread': 'spread_promedio'
        }, inplace=True)
        
        # Calcular el precio Ask basado en la fórmula: Ask = Bid + (spread / spread_divisor)
        df['ask_open']  = df['bid_open']  + (df['spread_promedio'] / self.spread_divisor)
        df['ask_high']  = df['bid_high']  + (df['spread_promedio'] / self.spread_divisor)
        df['ask_low']   = df['bid_low']   + (df['spread_promedio'] / self.spread_divisor)
        df['ask_close'] = df['bid_close'] + (df['spread_promedio'] / self.spread_divisor)
        
        # Convertir el timeframe usando el mapeo; si no se encuentra, se deja igual.
        df['timeframe'] = df['timeframe'].apply(lambda x: self.timeframe_mapping.get(x, x))
        
        # Asignar los nombres originales para broker y asset en las columnas correspondientes
        df['broker_id'] = df['broker']  # Se conserva el nombre original, se realizará conversión más adelante
        df['activo_id'] = df['asset']   # Se conserva el nombre original (símbolo del activo)
        
        # **Eliminar sufijos en los símbolos de activos antes de asignar IDs**
        if "activo_id" in df.columns:
            df["activo_id"] = df["activo_id"].astype(str)  # Asegurar que es string
            df["activo_id"] = df["activo_id"].apply(lambda x: re.sub(r"_CFD.*$", "", x))
            df["activo_id"] = df["activo_id"].str.replace(r"\.(?!IDX$|HK$)[A-Za-z0-9-]+$", "", regex=True)
        
        # Asignar valor por defecto a mercado_id
        df['mercado_id'] = self.default_mercado_id
        
        # Convertir el timestamp a objeto datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Eliminar columnas que no se requieren
        df.drop(columns=['real_volume', 'broker', 'asset'], inplace=True, errors='ignore')
        
        # Reordenar columnas según la estructura de la tabla
        cols_order = ['activo_id', 'broker_id', 'mercado_id', 'timestamp', 'timeframe',
                    'bid_open', 'bid_high', 'bid_low', 'bid_close',
                    'ask_open', 'ask_high', 'ask_low', 'ask_close',
                    'volumen_contratos', 'spread_promedio']
        df = df[cols_order]
        
        # Reemplazar los valores de activo_id utilizando el dataframe Assets
        if self.assets_df is not None:
            # Se asume que en assets_df la columna 'simbolo' tiene los mismos valores que los de df['activo_id']
            mapping_assets = dict(zip(self.assets_df['simbolo'], self.assets_df['activo_id']))
            df['activo_id'] = df['activo_id'].map(mapping_assets)
        
        # Reemplazar los valores de broker_id utilizando el dataframe Brokers
        if self.brokers_df is not None:
            # Se asume que en brokers_df la columna 'nombre' tiene los mismos valores que los de df['broker_id']
            mapping_brokers = dict(zip(self.brokers_df['nombre'], self.brokers_df['broker_id']))
            df['broker_id'] = df['broker_id'].map(mapping_brokers)
        
        return df
    
    def transform(self):
        """
        Procesa de forma secuencial cada archivo CSV permitido en chunks para no sobrecargar la memoria.
        Este método es un generador que rinde cada chunk transformado, lo que permite insertarlo
        en PostgreSQL de forma incremental o exportarlo.
        """
        # Listar todos los archivos en la carpeta y filtrar los permitidos
        all_files = os.listdir(self.folder_path)
        csv_files = [os.path.join(self.folder_path, f) for f in all_files if f in self.allowed_files]
        
        # Procesar cada archivo secuencialmente
        for file in csv_files:
            print(f"Procesando archivo: {os.path.basename(file)}")
            for chunk in tqdm(pd.read_csv(file, chunksize=self.chunksize), 
                              desc=f"Chunks de {os.path.basename(file)}"):
                processed_chunk = self._process_chunk(chunk.copy())
                yield processed_chunk
    
    def preview(self, n=5):
        """
        Retorna una vista previa (las primeras n filas) del primer chunk del primer archivo encontrado.
        Esto es útil para verificar que la transformación se realiza correctamente.
        """
        all_files = os.listdir(self.folder_path)
        csv_files = [os.path.join(self.folder_path, f) for f in all_files if f in self.allowed_files]
        if csv_files:
            chunk = next(pd.read_csv(csv_files[0], chunksize=self.chunksize))
            processed_chunk = self._process_chunk(chunk.copy())
            return processed_chunk.head(n)
        else:
            return pd.DataFrame()
    
    def export_unified_dataframe(self, output_folder, output_filename='unified.csv'):
        """
        Exporta el dataframe unificado (resultado de la transformación en chunks) a un archivo CSV
        en la carpeta especificada.
        Si la carpeta no existe, se crea.
        Se agrega una columna 'registro_id' incremental que empieza en 1 hasta el final.
        """
        # Crear la carpeta de destino si no existe
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
        
        output_path = os.path.join(output_folder, output_filename)
        first_chunk = True
        registro_counter = 1  # Inicializa el contador para la columna registro_id
        print(f"Exportando dataframe unificado a: {output_path}")
        
        for chunk in self.transform():
            n_rows = len(chunk)
            # Agregar la columna 'registro_id' que se incrementa a lo largo de todos los chunks
            chunk.insert(0, 'registro_id', range(registro_counter, registro_counter + n_rows))
            registro_counter += n_rows
            
            if first_chunk:
                chunk.to_csv(output_path, index=False, mode='w', header=True)
                first_chunk = False
            else:
                chunk.to_csv(output_path, index=False, mode='a', header=False)
        print("Exportación completada.")

# =====================================================
# Ejemplo de uso:
# =====================================================
if __name__ == "__main__":
    # Importar los dataframes de Assets y Brokers
    assets_df = pd.read_csv(r"C:\Users\spinz\OneDrive\Documentos\Portafolio oficial\HERMESDB\HERMESDB\test\data\backup\Table_Assets.csv")    # Debe tener columnas ['activo_id', 'simbolo']
    brokers_df = pd.read_csv(r"C:\Users\spinz\OneDrive\Documentos\Portafolio oficial\HERMESDB\HERMESDB\test\data\backup\Table_Broker.csv")   # Debe tener columnas ['broker_id', 'nombre']
    
    # Ruta a la carpeta donde se encuentran los CSV
    folder = r"C:\Users\spinz\OneDrive\Documentos\Portafolio oficial\HERMESDB\HERMESDB\test\data\processed"
    
    # Se activa el auto ajuste de chunk con un safety factor del 90%
    adapter = CSVToPostgresAdapter(folder, chunksize=100000, spread_divisor=10**5, 
                                   auto_adjust=True, safety_factor=0.9, sample_size=100000, 
                                   assets_df=assets_df, brokers_df=brokers_df)
    
    # Visualizar una vista previa de las primeras filas
    df_preview = adapter.preview()
    print(df_preview)
    
    # Exportar el dataframe unificado a una carpeta específica
    output_folder = r"C:\Users\spinz\OneDrive\Documentos\Portafolio oficial\HERMESDB\HERMESDB\test\data\backup"
    adapter.export_unified_dataframe(output_folder, output_filename='data_unificada.csv')
