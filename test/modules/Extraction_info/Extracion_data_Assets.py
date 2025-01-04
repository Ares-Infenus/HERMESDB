import MetaTrader5 as mt5
import pandas as pd
from datetime import datetime
import os

import concurrent.futures
import time  # Importa el módulo time para las pausas

class MetaTraderDataExtractor:
    def __init__(self, login, password, server, assets_csv_path, output_folder):
        self.login = login
        self.password = password
        self.server = server
        self.assets_csv_path = assets_csv_path
        self.output_folder = output_folder
        self.timeframes = {
            '1H': mt5.TIMEFRAME_H1,
            '4H': mt5.TIMEFRAME_H1,
            '1D': mt5.TIMEFRAME_D1,
            '1W': mt5.TIMEFRAME_W1,
            '1M': mt5.TIMEFRAME_MN1
        }
        self.initialize_mt5()
        self.load_assets()

    def initialize_mt5(self):
        if not mt5.initialize(login=self.login, password=self.password, server=self.server):
            print("Failed to initialize MetaTrader 5.")
            mt5.shutdown()
        else:
            print("MetaTrader 5 initialized successfully.")

    def load_assets(self):
        self.assets_df = pd.read_csv(self.assets_csv_path)
        self.assets_df = self.assets_df[self.assets_df['IS_ACTIVE'] == 'Y']

    def get_historical_data(self, symbol, timeframe, timeframe_label, num_bars):
        rates = mt5.copy_rates_from_pos(symbol, timeframe, 0, num_bars)
        
        if rates is None or len(rates) == 0:
            print(f"No data found for {symbol} on timeframe {timeframe_label}.")
            return None
        
        df = pd.DataFrame(rates)
        df['time'] = pd.to_datetime(df['time'], unit='s', utc=True)
        df = df[['time', 'open', 'high', 'low', 'close', 'tick_volume', 'spread', 'real_volume']]
        df['timeframe'] = timeframe_label
        df['Activo'] = symbol
        
        return df

    def download_data_for_asset(self, symbol):
        all_data = []
        for timeframe_label, timeframe in self.timeframes.items():
            df = self.get_historical_data(symbol, timeframe, timeframe_label, 99999)
            if df is not None:
                all_data.append(df)
                print(f"Downloaded {len(df)} bars for {symbol} on timeframe {timeframe_label}.")
            
            # Pausa de 1 segundo entre cada solicitud
            time.sleep(1)
        
        if all_data:
            final_df = pd.concat(all_data, ignore_index=True)
            file_name = f"{symbol}.csv"
            if not os.path.exists(self.output_folder):
                os.makedirs(self.output_folder)
            final_df.to_csv(os.path.join(self.output_folder, file_name), index=False)
            print(f"Data for {symbol} saved to {os.path.join(self.output_folder, file_name)}")
        else:
            print(f"No data downloaded for {symbol}.")

    def download_all_data(self):
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.map(self.download_data_for_asset, self.assets_df['ASSETS_NAME'])
        mt5.shutdown()

# Example usage:
extractor = MetaTraderDataExtractor(login=3000074280, password='#%@Q$20vk5o', server='demoUK-mt5.darwinex.com', assets_csv_path='C:\\Users\\spinz\\Documents\\Portafolio Oficial\\HERMESDB\\data\\tables_data\\data_table\\ASSETS_PART_2.csv', output_folder='C:\\Users\\spinz\\Documents\\Portafolio Oficial\\HERMESDB\\data\\tables_data\\data_table\\data_market')
extractor.download_all_data()