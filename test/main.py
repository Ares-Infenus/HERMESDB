"""
HERMES Database Tables Creation Script
This script creates and exports various database tables for the HERMES trading system.
Each section handles the creation and export of a specific table with its corresponding data.
"""

# =============================================================================
# Imports
# =============================================================================
import os
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

# Market Data imports
from modules.Extraction_info.Extracion_data_Assets import MetaTraderDataExtractor
from modules.Creation_tables.Market_Data import CSVProcessor

# Operative cost imports
from modules.Extraction_info.extraction_operative_cost import FinancialOperationalCostExtractor
from modules.Extraction_info.OperationalCostProcessor import OperationalCostProcessor

# =============================================================================
# Global Configuration
# =============================================================================
BASE_DIR = "C:\\Users\\spinz\\Documents\\Portafolio Oficial\\HERMESDB\\data\\tables_data"

# =============================================================================
# Table Creation Functions
# =============================================================================

def create_market_table():
    """
    Creates and exports the MARKET table containing basic market information.
    """
    print("\n=== Creating MARKET Table ===")
    market_data_instance = MarketData(BASE_DIR)
    market_data_instance.export_to_csv()

def create_market_metadata():
    """
    Creates and exports the MARKET_METADATA table with detailed market metadata.
    """
    print("\n=== Creating MARKET_METADATA Table ===")
    manager = MarketMetadataManager()
    metadata_path = os.path.join(BASE_DIR, "MARKET_METADATA.csv")
    manager.export_metadata(metadata_path, 'csv')

def create_assets_table():
    """
    Creates and exports the ASSETS table by extracting and processing raw asset data.
    """
    print("\n=== Creating ASSETS Table ===")
    
    # Step 1: Extract raw asset data
    raw_assets_path = os.path.join(BASE_DIR, "data_table", "ASSETS.csv")
    extractor = MT5AssetExtractor(raw_assets_path)
    extractor.extract_assets()
    
    # Step 2: Process and format the data
    final_assets_path = os.path.join(BASE_DIR, "ASSETS.csv")
    processor = AssetProcessor(raw_assets_path, final_assets_path)
    processor.process_assets()

def create_timeframe_table():
    """
    Creates and exports the TIMEFRAME table containing different time intervals.
    """
    print("\n=== Creating TIMEFRAME Table ===")
    timeframe_path = os.path.join(BASE_DIR, "TIMEFRAME.csv")
    processor = TimeFrameProcessor(timeframe_path)
    processor.save_to_csv()

def create_data_source_table():
    """
    Creates and exports the DATA_SOURCE table with information about data providers.
    """
    print("\n=== Creating DATA_SOURCE Table ===")
    data_source = DATA_SOURCE(export_directory=BASE_DIR)
    data_source.export_to_csv("DATA_SOURCE.csv")

def create_market_data():
    """
    Creates and processes the MARKET_DATA table by extracting and consolidating market data.
    """
    print("\n=== Creating MARKET_DATA Table ===")
    
    # Step 1: Extract data
    extractor = MetaTraderDataExtractor(
        login=3000074280,
        password='#%@Q$20vk5o',
        server='demoUK-mt5.darwinex.com',
        assets_csv_path=os.path.join(BASE_DIR, "ASSETS.csv"),
        output_folder=os.path.join(BASE_DIR, "data_table", "data_market")
    )
    extractor.download_all_data(max_workers=5)
    
    # Step 2: Consolidate data
    assets_df = pd.read_csv(os.path.join(BASE_DIR, "ASSETS.csv"))
    input_folder = os.path.join(BASE_DIR, "data_table", "data_market")
    output_file = os.path.join(BASE_DIR, "MARKET_DATA_BID.csv")
    output_folder = os.path.join(BASE_DIR, "processed_chunks")
    processor = CSVProcessor(input_folder, output_file, output_folder, assets_df)
    processor.process_files()

def create_operative_cost():
    """
    Creates and exports the OPERATIVE_COST table by extracting and organizing cost data.
    """
    print("\n=== Creating OPERATIVE_COST Table ===")
    
    # Step 1: Extract cost data
    output_dir = os.path.join(BASE_DIR, "data_table", "data_cost_operative")
    driver_path = os.path.join("test", "modules", "Extraction_info", "chrome_driver", "chromedriver.exe")
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, "operative_costs.csv")
    
    try:
        extractor = FinancialOperationalCostExtractor(driver_path)
        extractor.extract_all_data(output_file)
        print(f"Operative costs exported to: {output_file}")
    except Exception as e:
        print(f"Error extracting operative costs: {str(e)}")
    
    # Step 2: Organize extracted data
    input_path = output_file
    output_directory = os.path.join(output_dir, "processed")
    processor = OperationalCostProcessor()
    processor.load_data(input_path)
    processor.save_to_csv(output_prefix="operative_costs", output_dir=output_directory)
    processor.display_summary()

# =============================================================================
# Main Execution
# =============================================================================

def main():
    """
    Main function to orchestrate the creation of all database tables.
    """
    print("Starting HERMES Database Tables Creation...")

    # Sequential table creation
    create_market_table()
    create_market_metadata()
    create_assets_table()
    create_timeframe_table()
    create_data_source_table()
    # create_market_data()  # Uncomment if market data processing is required
    create_operative_cost()

    print("\nAll tables have been created successfully!")

if __name__ == "__main__":
    main()
