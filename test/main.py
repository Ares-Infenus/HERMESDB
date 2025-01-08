"""
HERMES Database Tables Creation Script
This script creates and exports various database tables for the HERMES trading system.
Each section handles the creation and export of a specific table with its corresponding data.
"""

# =============================================================================
# Imports
# =============================================================================
#Librerias NECESARIAS
import pandas as pd
# Market-related imports
from modules.Creation_tables.Market import MarketData
from modules.Creation_tables.Market_metadata import MarketMetadataManager

# Asset-related imports
from modules.Extraction_info.Extraction_info_Assets import MT5AssetExtractor
from modules.Creation_tables.Assets import AssetProcessor

# Additional table imports
from modules.Creation_tables.Timeframe import TimeFrameProcessor
from modules.Creation_tables.Data_source import DATA_SOURCE

# Market Data
from modules.Extraction_info.Extracion_data_Assets import MetaTraderDataExtractor
from modules.Creation_tables.Market_Data import CSVProcessor

# =============================================================================
# Global Configuration
# =============================================================================
# Base directory for all output files
BASE_DIR = "C:\\Users\\spinz\\Documents\\Portafolio Oficial\\HERMESDB\\data\\tables_data"

# =============================================================================
# 1. Market Data Table Creation
# =============================================================================
def create_market_table():
    """
    Creates and exports the MARKET table containing basic market information.
    This table serves as a reference for different trading markets available in the system.
    """
    print("\n=== Creating MARKET Table ===")
    market_data_instance = MarketData(BASE_DIR)
    market_data_instance.export_to_csv()

# =============================================================================
# 2. Market Metadata Table Creation
# =============================================================================
def create_market_metadata():
    """
    Creates and exports the MARKET_METADATA table.
    Contains detailed metadata and additional information about each market.
    """
    print("\n=== Creating MARKET_METADATA Table ===")
    manager = MarketMetadataManager()
    metadata_path = f"{BASE_DIR}\\MARKET_METADATA.csv"
    manager.export_metadata(metadata_path, 'csv')

# =============================================================================
# 3. Assets Table Creation
# =============================================================================
def create_assets_table():
    """
    Creates and exports the ASSETS table through a two-step process:
    1. Extracts raw asset data from MT5
    2. Processes and formats the data for final table structure
    """
    print("\n=== Creating ASSETS Table ===")
    
    # Step 1: Extract raw asset data
    raw_assets_path = f"{BASE_DIR}\\data_table\\ASSETS.csv"
    extractor = MT5AssetExtractor(raw_assets_path)
    extractor.extract_assets()
    
    # Step 2: Process and format the data
    final_assets_path = f"{BASE_DIR}\\ASSETS.csv"
    processor = AssetProcessor(raw_assets_path, final_assets_path)
    processor.process_assets()

# =============================================================================
# 4. Timeframe Table Creation
# =============================================================================
def create_timeframe_table():
    """
    Creates and exports the TIMEFRAME table.
    This table contains different time intervals used for market data analysis.
    """
    print("\n=== Creating TIMEFRAME Table ===")
    timeframe_path = f"{BASE_DIR}\\TIMEFRAME.csv"
    processor = TimeFrameProcessor(timeframe_path)
    processor.save_to_csv()

# =============================================================================
# 5. Data Source Table Creation
# =============================================================================
def create_data_source_table():
    """
    Creates and exports the DATA_SOURCE table.
    Contains information about different data providers and their characteristics.
    """
    print("\n=== Creating DATA_SOURCE Table ===")
    data_source = DATA_SOURCE(export_directory=BASE_DIR)
    data_source.export_to_csv("DATA_SOURCE.csv")
    
# =============================================================================
# 6. Market Data Table Creation
# =============================================================================
def create_market_data():
    #step 1: extract info 
    #extractor = MetaTraderDataExtractor(
    #    login=3000074280,
    #    password='#%@Q$20vk5o',
    #    server='demoUK-mt5.darwinex.com',
    #    assets_csv_path='C:\\Users\\spinz\\Documents\\Portafolio Oficial\\HERMESDB\\data\\tables_data\\ASSETS.csv',
    #    output_folder='C:\\Users\\spinz\\Documents\\Portafolio Oficial\\HERMESDB\\data\\tables_data\\data_table\\data_market'
    #)
    #extractor.download_all_data(max_workers=5)
    #step_2: Acopland la tabla Market_data
    assets_df = pd.read_csv("C:\\Users\\spinz\\Documents\\Portafolio Oficial\\HERMESDB\\data\\tables_data\\ASSETS.csv")
    input_folder = 'C:\\Users\\spinz\\Documents\\Portafolio Oficial\\HERMESDB\\data\\tables_data\\data_table\\data_market'
    output_file = 'C:\\Users\\spinz\\Documents\\Portafolio Oficial\\HERMESDB\\data\\tables_data\\MARKET_DATA_BID.csv'
    output_folder = 'C:\\Users\\spinz\\Documents\\Portafolio Oficial\\HERMESDB\\data\\tables_data\\processed_chunks'
    processor = CSVProcessor(input_folder, output_file, output_folder, assets_df)
    processor.process_files()

# =============================================================================
# 7. Operative Cost Table Creation.
# =============================================================================
def create_operative_cost():
    
    pass

# =============================================================================
# Main Execution
# =============================================================================
def main():
    """
    Main function to orchestrate the creation of all database tables.
    Executes each table creation function in a logical order.
    """
    print("Starting HERMES Database Tables Creation...")
    
    # Create tables in sequence
    create_market_table()
    create_market_metadata()
    create_assets_table()
    create_timeframe_table()
    create_data_source_table()
    create_market_data()
    print("\nAll tables have been created successfully!")

if __name__ == "__main__":
    main()


#Creacion de la tabla Market_data
#extractor = MetaTraderDataExtractor(login=3000074280, password='#%@Q$20vk5o', server='demoUK-mt5.darwinex.com', assets_csv_path='C:\\Users\\spinz\\Documents\\Portafolio Oficial\\HERMESDB\\data\\tables_data\\data_table\\ASSETS.csv', output_folder='C:\\Users\\spinz\\Documents\\Portafolio Oficial\\HERMESDB\\data\\tables_data\\data_table\\data_market')
#extractor.download_all_data()

#Unificacion de los dato y creaciond definitiva del la tabla Market