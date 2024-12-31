"""
MetaTrader 5 Asset Data Processor
--------------------------------
This module processes financial asset data from MetaTrader 5 and organizes it into structured dataframes
for different asset classes including forex, indices, stocks, ETFs, and commodities.

Example Usage:
    # Initialize asset lists and mappings
    majors = ["EURUSD", "GBPUSD", "USDJPY", "USDCHF", "USDCAD", "AUDUSD", "NZDUSD"]
    commodities_forex = ["XAUUSD", "XAGUSD"]
    index_mapping = {
        "7": ["NI225", "AUS200"],           # Asian indices
        "6": ["WS30", "SP500", "NDX"],      # US indices
        "8": ["FCHI40", "SPA35", "GDAXI", "UK100", "STOXX50E"]  # European indices
    }

    # Create processor instance and process data
    processor = AssetDataProcessor(majors, commodities_forex, index_mapping)
    raw_data = processor.fetch_assets()
    processor.classify_assets(raw_data)
    unified_df = processor.process_dataframes()

    # Example output dataframe structure:
    #    ASSETS_ID  ASSETS_NAME  MARKET_ID  METADATA_ID IS_ACTIVE            CREATED_AT            UPDATED_AT
    # 0          0       EURUSD          0            0         Y  2024-12-30 12:00:00  2024-12-30 12:00:00
    # 1          1       GBPUSD          0            0         Y  2024-12-30 12:00:00  2024-12-30 12:00:00
"""

import pandas as pd
from datetime import datetime
import MetaTrader5 as mt5

class AssetDataProcessor:
    """
    A class to process and categorize financial assets from MetaTrader 5.
    
    This class fetches asset data from MT5, categorizes them into different asset classes,
    and processes them into a unified format suitable for database storage.

    Attributes:
        majors (list): List of major forex currency pairs
        commodities_forex (list): List of forex-traded commodities (e.g., XAUUSD)
        index_mapping (dict): Mapping of index categories to their respective symbols
        dataframes (dict): Dictionary containing categorized asset DataFrames
    """

    def __init__(self, majors, commodities_forex, index_mapping):
        """
        Initialize the AssetDataProcessor with required mappings and empty dataframes.

        Args:
            majors (list): List of major forex currency pairs
            commodities_forex (list): List of forex-traded commodities
            index_mapping (dict): Dictionary mapping index categories to symbols
        """
        self.majors = majors
        self.commodities_forex = commodities_forex
        self.index_mapping = index_mapping
        # Initialize empty dataframes for each asset category
        self.dataframes = {
            'forex': pd.DataFrame(columns=["Nombre Activo", "Sector"]),
            'indices': pd.DataFrame(columns=["Nombre Activo", "Sector"]),
            'stocks': pd.DataFrame(columns=["Nombre Activo", "Sector"]),
            'etfs': pd.DataFrame(columns=["Nombre Activo", "Sector"]),
            'commodities': pd.DataFrame(columns=["Nombre Activo", "Sector"])
        }

    def fetch_assets(self):
        """
        Fetch available assets from MetaTrader 5.

        Returns:
            pd.DataFrame: DataFrame containing asset names and their sectors
        
        Raises:
            ConnectionError: If unable to initialize MetaTrader 5
        """
        if not mt5.initialize():
            raise ConnectionError("Error al inicializar MetaTrader 5")

        symbols = mt5.symbols_get()
        data = [[symbol.name, symbol.path] for symbol in symbols]
        mt5.shutdown()
        return pd.DataFrame(data, columns=["Nombre Activo", "Sector"])

    def classify_assets(self, raw_data):
        """
        Classify assets into their respective categories based on sector information.

        Args:
            raw_data (pd.DataFrame): DataFrame containing asset information from MT5
        """
        # Iterate through each asset and classify into appropriate category
        for _, row in raw_data.iterrows():
            name, sector = row['Nombre Activo'], row['Sector']

            # Determine category based on sector and append to appropriate dataframe
            if 'Forex' in sector:
                self.dataframes['forex'] = pd.concat([
                    self.dataframes['forex'],
                    pd.DataFrame([{"Nombre Activo": name, "Sector": sector}])
                ], ignore_index=True)
            elif 'INDEX' in sector or 'Indices' in sector:
                self.dataframes['indices'] = pd.concat([
                    self.dataframes['indices'],
                    pd.DataFrame([{"Nombre Activo": name, "Sector": sector}])
                ], ignore_index=True)
            elif 'Stocks' in sector:
                self.dataframes['stocks'] = pd.concat([
                    self.dataframes['stocks'],
                    pd.DataFrame([{"Nombre Activo": name, "Sector": sector}])
                ], ignore_index=True)
            elif 'ETFs' in sector:
                self.dataframes['etfs'] = pd.concat([
                    self.dataframes['etfs'],
                    pd.DataFrame([{"Nombre Activo": name, "Sector": sector}])
                ], ignore_index=True)
            elif 'Commodities' in sector:
                self.dataframes['commodities'] = pd.concat([
                    self.dataframes['commodities'],
                    pd.DataFrame([{"Nombre Activo": name, "Sector": sector}])
                ], ignore_index=True)

    def process_dataframes(self):
        """
        Process all categorized dataframes into a unified format.

        Returns:
            pd.DataFrame: Unified DataFrame containing all processed assets with unique IDs
        """
        # Process each category using specific processing methods
        definitive_dataframes = {
            'forex': self._process_forex(),
            'indices': self._process_indices(),
            'stocks': self._process_generic('stocks', market_id=4, metadata_id=29),
            'etfs': self._process_generic('etfs', market_id=5, metadata_id=33),
            'commodities': self._process_commodities()
        }
        # Combine all processed dataframes and add unique IDs
        unified_dataframe = pd.concat(definitive_dataframes.values(), ignore_index=True)
        unified_dataframe['ASSETS_ID'] = range(len(unified_dataframe))
        return unified_dataframe

    def _process_forex(self):
        """
        Process forex assets, distinguishing between major and minor pairs.

        Returns:
            pd.DataFrame: Processed forex DataFrame with standardized columns
        """
        df = self.dataframes['forex']
        df_processed = pd.DataFrame()
        df_processed['ASSETS_NAME'] = df['Nombre Activo']
        df_processed['MARKET_ID'] = 0
        # Assign metadata ID based on whether it's a major currency pair
        df_processed['METADATA_ID'] = df['Nombre Activo'].apply(
            lambda x: 0 if x in self.majors else 1
        )
        df_processed['IS_ACTIVE'] = 'Y'
        df_processed['CREATED_AT'] = pd.to_datetime(datetime.now())
        df_processed['UPDATED_AT'] = pd.to_datetime(datetime.now())
        return df_processed

    def _process_indices(self):
        """
        Process index assets using the provided index mapping.

        Returns:
            pd.DataFrame: Processed indices DataFrame with standardized columns
        """
        df = self.dataframes['indices']
        df_processed = pd.DataFrame()
        df_processed['ASSETS_NAME'] = df['Nombre Activo']
        df_processed['MARKET_ID'] = 2
        # Create mapping dictionary for index metadata IDs
        mapping = {asset: int(metadata_id) for metadata_id, assets in self.index_mapping.items() for asset in assets}
        df_processed['METADATA_ID'] = df['Nombre Activo'].map(mapping)
        df_processed['IS_ACTIVE'] = 'Y'
        df_processed['CREATED_AT'] = pd.to_datetime(datetime.now())
        df_processed['UPDATED_AT'] = pd.to_datetime(datetime.now())
        return df_processed

    def _process_generic(self, category, market_id, metadata_id):
        """
        Process generic assets (stocks and ETFs) with fixed market and metadata IDs.

        Args:
            category (str): Asset category ('stocks' or 'etfs')
            market_id (int): Market identifier
            metadata_id (int): Metadata identifier

        Returns:
            pd.DataFrame: Processed DataFrame with standardized columns
        """
        df = self.dataframes[category]
        df_processed = pd.DataFrame()
        df_processed['ASSETS_NAME'] = df['Nombre Activo']
        df_processed['MARKET_ID'] = market_id
        df_processed['METADATA_ID'] = metadata_id
        df_processed['IS_ACTIVE'] = 'Y'
        df_processed['CREATED_AT'] = pd.to_datetime(datetime.now())
        df_processed['UPDATED_AT'] = pd.to_datetime(datetime.now())
        return df_processed

    def _process_commodities(self):
        """
        Process commodity assets, distinguishing between forex and non-forex traded.

        Returns:
            pd.DataFrame: Processed commodities DataFrame with standardized columns
        """
        df = self.dataframes['commodities']
        df_processed = pd.DataFrame()
        df_processed['ASSETS_NAME'] = df['Nombre Activo']
        # Assign market ID based on whether it's a forex-traded commodity
        df_processed['MARKET_ID'] = df['Nombre Activo'].apply(
            lambda x: 0 if x in self.commodities_forex else 1
        )
        # Assign metadata ID based on commodity type
        df_processed['METADATA_ID'] = df['Nombre Activo'].apply(
            lambda x: 2 if x in self.commodities_forex else 4
        )
        df_processed['IS_ACTIVE'] = 'Y'
        df_processed['CREATED_AT'] = pd.to_datetime(datetime.now())
        df_processed['UPDATED_AT'] = pd.to_datetime(datetime.now())
        return df_processed

# Example usage
if __name__ == "__main__":
    # Define asset classifications
    majors = ["EURUSD", "GBPUSD", "USDJPY", "USDCHF", "USDCAD", "AUDUSD", "NZDUSD"]
    commodities_forex = ["XAUUSD", "XAGUSD"]
    index_mapping = {
        "7": ["NI225", "AUS200"],  # Asian indices
        "6": ["WS30", "SP500", "NDX"],  # US indices
        "8": ["FCHI40", "SPA35", "GDAXI", "UK100", "STOXX50E"]  # European indices
    }

    # Initialize and run processor
    processor = AssetDataProcessor(majors, commodities_forex, index_mapping)
    raw_data = processor.fetch_assets()
    processor.classify_assets(raw_data)
    unified_dataframe = processor.process_dataframes()

# Define el archivo de salida
output_file = "C:\\Users\\spinz\\Documents\\Portafolio Oficial\\HERMESDB\\data\\tables_data\\assets_classified.csv"

# Exportar el DataFrame a un archivo CSV
unified_dataframe.to_csv(output_file, index=False, encoding="utf-8")

print(f"Archivo CSV exportado exitosamente a: {output_file}")