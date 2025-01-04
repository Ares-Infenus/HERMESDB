import pandas as pd
from datetime import datetime

class AssetProcessor:
    def __init__(self, input_csv_path, output_csv_path):
        """
        Inicializa el procesador de activos con las rutas de entrada y salida.
        
        :param input_csv_path: Ruta al archivo CSV de entrada.
        :param output_csv_path: Ruta donde se exportará el archivo procesado.
        """
        self.input_csv_path = input_csv_path
        self.output_csv_path = output_csv_path
        
        # Definición de clasificaciones y valores
        self.forex_majors = ['EURUSD', 'USDJPY', 'GBPUSD', 'USDCHF', 'USDCAD', 'AUDUSD', 'NZDUSD']
        self.metals_forex = ['XAUUSD', 'XAGUSD']
        self.energy_commodities = ['XTIUSD', 'XNGUSD']
        self.america_indices = ['WS30', 'SP500', 'NDX']
        self.europe_indices = ['FCHI40', 'UK100', 'STOXX50E', 'GDAXI', 'SPA35']
        self.asia_pacific_indices = ['NI225', 'AUS200']
        self.valores_sector = {
            'Forex': (0, 0),
            'Stocks': (29, 4),
            'Indices': (0, 0)
        }
        self.valores = {
            'Forex_mayors': (1, 0),
            'Metals_forex': (2, 0),
            'Energy_commodities': (4, 1),
            'America_indices': (6, 2),
            'Europe_indices': (8, 2),
            'Asia_pacific_indices': (7, 2),
            'ETFs': (33, 5),
            'Stocks': (29, 4)
        }

    def process_assets(self):
        """
        Procesa el archivo de activos y genera un archivo procesado en la ubicación especificada.
        """
        # Cargar el archivo CSV
        assets_info = pd.read_csv(self.input_csv_path)

        # Asignar METADATA_ID y MARKET_ID
        assets_info[['METADATA_ID', 'MARKET_ID']] = assets_info.apply(self._get_metadata_market_id, axis=1).apply(pd.Series)

        # Agregar columnas adicionales
        assets_info['IS_ACTIVE'] = 'Y'
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        assets_info['CREATED_AT'] = current_time
        assets_info['UPDATED_AT'] = current_time
        assets_info['ASSETS_ID'] = assets_info.index

        # Limpiar y renombrar columnas
        assets_info.drop(columns=['Sector'], inplace=True)
        assets_info.rename(columns={'Activo': 'ASSETS_NAME'}, inplace=True)

        # Exportar el DataFrame procesado
        assets_info.to_csv(self.output_csv_path, index=False)
        print(f"Archivo procesado y guardado en: {self.output_csv_path}")

    def _get_metadata_market_id(self, row):
        """
        Asigna METADATA_ID y MARKET_ID según el activo y su sector.
        
        :param row: Fila del DataFrame.
        :return: Tupla con (METADATA_ID, MARKET_ID).
        """
        activo, sector = row['Activo'], row['Sector']
        
        if sector == 'Forex':
            if activo in self.forex_majors:
                return self.valores['Forex_mayors']
            elif activo in self.metals_forex:
                return self.valores['Metals_forex']
            else:
                return self.valores_sector['Forex']
        
        elif sector == 'Indices':
            if activo in self.america_indices:
                return self.valores['America_indices']
            elif activo in self.europe_indices:
                return self.valores['Europe_indices']
            elif activo in self.asia_pacific_indices:
                return self.valores['Asia_pacific_indices']
            else:
                return self.valores_sector['Indices']
        
        elif sector == 'Stocks':
            return self.valores['Stocks']
        
        return (0, 0)  # Valores por defecto para casos no clasificados
