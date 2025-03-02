import os
import re
import pandas as pd
import psutil
from tqdm import tqdm

class CSVToPostgresAdapter:
    # Archivos permitidos y mapeos fijos
    ALLOWED_FILES = {'Darwinex.csv', 'Pepperstone.csv', 'Tickmill.csv', 'Dukascopy.csv', 'Oanda.csv'}
    DEFAULT_MERCADO_ID = 1
    TIMEFRAME_MAPPING = {
        'H1': '1h', 'M1': '1m', 'M5': '5m', 'M15': '15m', 'M30': '30m',
        'H4': '4h', 'D1': '1d', 'W1': '1w', 'MN': '1M'
    }
    
    def __init__(self, folder_path: str, chunksize: int = 100000, spread_divisor: int = 10**5,
                 auto_adjust: bool = False, safety_factor: float = 0.5, sample_size: int = 1000,
                 assets_df: pd.DataFrame = None, brokers_df: pd.DataFrame = None):
        # Validaciones básicas
        assert os.path.isdir(folder_path), "La ruta de la carpeta no es válida."
        assert 0 < safety_factor <= 1, "El safety_factor debe estar entre 0 y 1."
        
        self.folder_path = folder_path
        self.chunksize = chunksize
        self.spread_divisor = spread_divisor
        self.safety_factor = safety_factor
        self.sample_size = sample_size
        self.assets_df = assets_df
        self.brokers_df = brokers_df
        
        # Precompilar regex para mejorar el rendimiento
        self._regex_cfd = re.compile(r"_CFD.*$")
        self._regex_suffix = re.compile(r"\.(?!IDX$|HK$)[A-Za-z0-9-]+$")
        
        if auto_adjust:
            csv_files = [os.path.join(self.folder_path, f) for f in os.listdir(self.folder_path)
                         if f in self.ALLOWED_FILES]
            if csv_files:
                optimal = self._estimate_chunk_size(csv_files[0])
                self.chunksize = optimal
                print(f"Chunk size ajustado a: {optimal} filas.")
            else:
                print("No se encontraron archivos permitidos para el ajuste automático.")
    
    def _estimate_chunk_size(self, file_path: str) -> int:
        sample = pd.read_csv(file_path, nrows=self.sample_size)
        memory_per_row = sample.memory_usage(deep=True).sum() / self.sample_size
        target_memory = psutil.virtual_memory().available * self.safety_factor
        return int(target_memory // memory_per_row)
    
    def _transform_chunk(self, df: pd.DataFrame, map_ids: bool = True) -> pd.DataFrame:
        # Renombrar columnas de forma vectorizada
        rename_map = {
            'time': 'timestamp', 'open': 'bid_open', 'high': 'bid_high',
            'low': 'bid_low', 'close': 'bid_close', 'tick_volume': 'volumen_contratos',
            'spread': 'spread_promedio'
        }
        df.rename(columns=rename_map, inplace=True)
        # Calcular precios Ask sin usar funciones lambda en cada elemento
        for col in ['open', 'high', 'low', 'close']:
            bid_col = f'bid_{col}'
            ask_col = f'ask_{col}'
            df[ask_col] = df[bid_col] + (df['spread_promedio'] / self.spread_divisor)
        # Convertir timeframe de forma vectorizada
        df['timeframe'] = df['timeframe'].map(self.TIMEFRAME_MAPPING).fillna(df['timeframe'])
        # Asignar valores originales para luego mapear
        df['broker_id'] = df['broker']
        df['activo_id'] = df['asset'].astype(str)
        # Eliminar sufijos en activos de forma vectorizada
        df['activo_id'] = df['activo_id'].apply(lambda x: self._regex_suffix.sub("", self._regex_cfd.sub("", x)))
        # Asignar valores fijos y convertir timestamp
        df['mercado_id'] = self.DEFAULT_MERCADO_ID
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        # Eliminar columnas innecesarias
        df.drop(columns=['real_volume', 'broker', 'asset'], inplace=True, errors='ignore')
        # Reordenar columnas
        cols = ['activo_id', 'broker_id', 'mercado_id', 'timestamp', 'timeframe',
                'bid_open', 'bid_high', 'bid_low', 'bid_close',
                'ask_open', 'ask_high', 'ask_low', 'ask_close',
                'volumen_contratos', 'spread_promedio']
        df = df[cols]
        # Mapear IDs si corresponde
        if map_ids:
            if self.assets_df is not None:
                asset_map = dict(zip(self.assets_df['simbolo'], self.assets_df['activo_id']))
                df['activo_id'] = df['activo_id'].map(asset_map)
            if self.brokers_df is not None:
                broker_map = dict(zip(self.brokers_df['nombre'], self.brokers_df['broker_id']))
                df['broker_id'] = df['broker_id'].map(broker_map)
        return df

    def _iter_files(self):
        # Generador de archivos CSV permitidos
        for filename in os.listdir(self.folder_path):
            if filename in self.ALLOWED_FILES:
                yield os.path.join(self.folder_path, filename)
    
    def transform_generator(self, map_ids: bool = True):
        # Generador que procesa cada archivo en chunks
        for file in self._iter_files():
            print(f"Procesando: {os.path.basename(file)}")
            for chunk in tqdm(pd.read_csv(file, chunksize=self.chunksize), desc=f"Chunks {os.path.basename(file)}"):
                yield self._transform_chunk(chunk.copy(), map_ids)
    
    def preview(self, n: int = 5, map_ids: bool = True) -> pd.DataFrame:
        # Vista previa del primer chunk del primer archivo
        files = list(self._iter_files())
        if files:
            chunk = next(pd.read_csv(files[0], chunksize=self.chunksize))
            return self._transform_chunk(chunk.copy(), map_ids).head(n)
        return pd.DataFrame()
    
    def export_dataframe(self, output_folder: str, output_filename: str, map_ids: bool = True):
        # Exporta el dataframe unificado con columna incremental 'registro_id'
        os.makedirs(output_folder, exist_ok=True)
        output_path = os.path.join(output_folder, output_filename)
        registro_id = 1
        first = True
        for df in self.transform_generator(map_ids):
            n_rows = len(df)
            df.insert(0, 'registro_id', range(registro_id, registro_id + n_rows))
            registro_id += n_rows
            mode = 'w' if first else 'a'
            header = first
            df.to_csv(output_path, index=False, mode=mode, header=header)
            first = False
        print(f"Exportación completada en: {output_path}")

# =====================================================
# Ejemplo de uso
# =====================================================
if __name__ == "__main__":
    # Cargar dataframes de Assets y Brokers
    assets_df = pd.read_csv(r"C:\Users\spinz\OneDrive\Documentos\Portafolio oficial\HERMESDB\HERMESDB\test\data\backup\Table_Assets.csv")  # columnas: ['activo_id', 'simbolo']
    brokers_df = pd.read_csv(r"C:\Users\spinz\OneDrive\Documentos\Portafolio oficial\HERMESDB\HERMESDB\test\data\backup\Table_Broker.csv")   # columnas: ['broker_id', 'nombre']
    folder = r"C:\Users\spinz\OneDrive\Documentos\Portafolio oficial\HERMESDB\HERMESDB\test\data\processed"
    
    adapter = CSVToPostgresAdapter(
        folder_path=folder,
        chunksize=100000,
        spread_divisor=10**5,
        auto_adjust=True,
        safety_factor=0.9,
        sample_size=100000,
        assets_df=assets_df,
        brokers_df=brokers_df
    )
    
    # Vista previa
    print(adapter.preview(n=5))
    
    # Exportar datos mapeados y pre-mapeo (se controla con el parámetro map_ids)
    output_folder = r"C:\Users\spinz\OneDrive\Documentos\Portafolio oficial\HERMESDB\HERMESDB\test\data\backup"
    adapter.export_dataframe(output_folder, 'Table_Datos_historicos.csv', map_ids=True)
    adapter.export_dataframe(output_folder, 'datos_pre_mapping.csv', map_ids=False)
