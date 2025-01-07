"""
Enhanced MetaTrader Data Extractor
This class handles the extraction of historical market data from MetaTrader 5
with progress tracking, colored output, and improved error handling.
"""

import MetaTrader5 as mt5
import pandas as pd
import os
import concurrent.futures
import time
from tqdm import tqdm
from colorama import Fore, Back, Style, init
from datetime import datetime

# Initialize colorama for colored console output
init(autoreset=True)

class MetaTraderDataExtractor:
    """
    A class to extract historical market data from MetaTrader 5 with enhanced features.
    Includes progress tracking, colored output, and concurrent downloads.
    """
    
    def __init__(self, login, password, server, assets_csv_path, output_folder):
        """
        Initialize the MetaTrader Data Extractor with the given credentials and paths.
        
        Args:
            login (int): MetaTrader account login
            password (str): MetaTrader account password
            server (str): MetaTrader server address
            assets_csv_path (str): Path to the CSV file containing asset information
            output_folder (str): Path where the extracted data will be saved
        """
        self.login = login
        self.password = password
        self.server = server
        self.assets_csv_path = assets_csv_path
        self.output_folder = output_folder
        self.assets_df = None
        
        # Define available timeframes
        self.timeframes = {
            '1H': mt5.TIMEFRAME_H1,
            '4H': mt5.TIMEFRAME_H4,
            '1D': mt5.TIMEFRAME_D1,
            '1W': mt5.TIMEFRAME_W1,
            '1M': mt5.TIMEFRAME_MN1
        }
        
        # Create output directory if it doesn't exist
        os.makedirs(self.output_folder, exist_ok=True)
        
        # Initialize MT5 connection
        self._initialize_mt5()
        self._load_assets()

    def _initialize_mt5(self):
        """Initialize connection to MetaTrader 5 platform."""
        try:
            if not mt5.initialize(login=self.login, password=self.password, server=self.server):
                raise Exception(f"Error: {mt5.last_error()}")
            
            print(f"{Fore.GREEN}✓ MetaTrader 5 initialized successfully{Style.RESET_ALL}")
            
            # Get and display available symbols count
            symbols = mt5.symbols_get()
            print(f"{Fore.CYAN}Available symbols: {len(symbols)}{Style.RESET_ALL}")
            
        except Exception as e:
            print(f"{Fore.RED}Failed to initialize MetaTrader 5: {e}{Style.RESET_ALL}")
            mt5.shutdown()
            exit(1)

    def _load_assets(self):
        """Load and validate assets from CSV file."""
        try:
            self.assets_df = pd.read_csv(self.assets_csv_path)
            self.assets_df = self.assets_df[self.assets_df['IS_ACTIVE'] == 'Y']
            
            if self.assets_df.empty:
                raise Exception("No active assets found in CSV file")
            
            print(f"{Fore.GREEN}✓ Loaded {len(self.assets_df)} active assets{Style.RESET_ALL}")
            
        except Exception as e:
            print(f"{Fore.RED}Error loading assets: {e}{Style.RESET_ALL}")
            exit(1)

    def _get_historical_data(self, symbol, timeframe, timeframe_label, num_bars=99999):
        """
        Retrieve historical data for a specific symbol and timeframe.
        
        Returns:
            pandas.DataFrame or None: Historical data if successful, None otherwise
        """
        try:
            rates = mt5.copy_rates_from_pos(symbol, timeframe, 0, num_bars)
            if rates is None or len(rates) == 0:
                print(f"{Fore.YELLOW}No data for {symbol} ({timeframe_label}){Style.RESET_ALL}")
                return None
            
            df = pd.DataFrame(rates)
            df['time'] = pd.to_datetime(df['time'], unit='s', utc=True)
            df = df[['time', 'open', 'high', 'low', 'close', 
                    'tick_volume', 'spread', 'real_volume']]
            df['timeframe'] = timeframe_label
            df['Activo'] = symbol
            
            return df
            
        except Exception as e:
            print(f"{Fore.RED}Error getting data for {symbol} ({timeframe_label}): {e}{Style.RESET_ALL}")
            return None

    def _download_data_for_asset(self, symbol):
        """Download data for a single asset across all timeframes."""
        all_data = []
        
        # Create progress bar for timeframes
        with tqdm(self.timeframes.items(), 
                 desc=f"{Fore.CYAN}Downloading {symbol}{Style.RESET_ALL}",
                 bar_format='{l_bar}{bar:20}{r_bar}{bar:-10b}') as pbar:
            
            for timeframe_label, timeframe in pbar:
                df = self._get_historical_data(symbol, timeframe, timeframe_label)
                if df is not None:
                    all_data.append(df)
                    pbar.set_postfix(rows=len(df))
                time.sleep(0.5)  # Reduced delay for efficiency
        
        if all_data:
            try:
                final_df = pd.concat(all_data, ignore_index=True)
                timestamp = datetime.now().strftime("%Y%m%d")
                filename = f"{symbol}_{timestamp}.csv"
                filepath = os.path.join(self.output_folder, filename)
                
                final_df.to_csv(filepath, index=False)
                print(f"{Fore.GREEN}✓ Saved {symbol} data ({len(final_df)} rows){Style.RESET_ALL}")
                return True
                
            except Exception as e:
                print(f"{Fore.RED}Error saving {symbol} data: {e}{Style.RESET_ALL}")
                return False
        return False

    def download_all_data(self, max_workers=5):
        """
        Download data for all assets using parallel processing.
        
        Args:
            max_workers (int): Maximum number of concurrent downloads
        """
        assets = self.assets_df['ASSETS_NAME'].tolist()
        total_assets = len(assets)
        
        print(f"\n{Fore.CYAN}Starting download for {total_assets} assets...{Style.RESET_ALL}\n")
        
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Create progress bar for overall progress
                with tqdm(total=total_assets,
                         desc=f"{Fore.CYAN}Overall Progress{Style.RESET_ALL}",
                         bar_format='{l_bar}{bar:20}{r_bar}{bar:-10b}') as pbar:
                    
                    futures = {executor.submit(self._download_data_for_asset, asset): asset 
                             for asset in assets}
                    
                    success_count = 0
                    for future in concurrent.futures.as_completed(futures):
                        asset = futures[future]
                        try:
                            if future.result():
                                success_count += 1
                        except Exception as e:
                            print(f"{Fore.RED}Failed to process {asset}: {e}{Style.RESET_ALL}")
                        pbar.update(1)
            
            # Final summary
            print(f"\n{Fore.GREEN}Download Complete!")
            print(f"Successfully processed: {success_count}/{total_assets} assets{Style.RESET_ALL}")
            
        except Exception as e:
            print(f"{Fore.RED}Error during parallel processing: {e}{Style.RESET_ALL}")
        finally:
            mt5.shutdown()
            print(f"{Fore.GREEN}MetaTrader 5 connection closed{Style.RESET_ALL}")

# Example usage
#if __name__ == "__main__":
#    extractor = MetaTraderDataExtractor(
#        login=3000074280,
#        password='#%@Q$20vk5o',
#        server='demoUK-mt5.darwinex.com',
#        assets_csv_path='C:\\Users\\spinz\\Documents\\Portafolio Oficial\\HERMESDB\\data\\tables_data\\ASSETS.csv',
#        output_folder='C:\\Users\\spinz\\Documents\\Portafolio Oficial\\HERMESDB\\data\\tables_data\\data_table\\data_market'
#    )
#    extractor.download_all_data(max_workers=5)

