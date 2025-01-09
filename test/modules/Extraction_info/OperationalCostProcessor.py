import pandas as pd
from typing import Dict, List

class OperationalCostProcessor:
    """
    A class to process and organize operational costs data for different financial assets.
    """
    
    def __init__(self):
        self.asset_keywords = {
            "forex": "forex",
            "ETFs": "ETFs",
            "indices": "indices",
            "commodities": "commodities",
            "stocks": "stocks",
        }
        
        self.columns_by_sector = {
            "forex": ["ACTIVE", "SPREAD", "MARGIN", "MIN_SIZE", "CONTRACT_SIZE", "PIP_VALUE", 
                     "SWAP_LONG", "SWAP_SHORT", "ELIMINATED", "COMISSION_PER_ORDER", "SECTOR"],
            "indices": ["ACTIVE", "SPREAD", "MARGIN", "MIN_SIZE", "CONTRACT_SIZE", "PIP_VALUE", 
                       "SWAP_LONG", "SWAP_SHORT", "ELIMINATED", "COMISSION_PER_ORDER", "SECTOR"],
            "stocks": ["ACTIVE", "NAME", "PLATAFORM", "MARGIN", "MIN_SIZE", "CONTRACT_SIZE", 
                      "TICK_VALUE", "SWAP_LONG", "SWAP_SHORT", "ELIMINATED", "COMISSION_PER_ORDER", 
                      "AVAILABLE_TO_SELL_SHORT", "SECTOR"],
            "commodities": ["ACTIVE", "NAME", "SPREAD", "MARGIN", "MIN_SIZE", "CONTRACT_SIZE", 
                          "TICK_VALUE", "SWAP_LONG", "SWAP_SHORT", "SWAP_ROLLOVER", 
                          "COMMISSION_PER_ORDER", "SECTOR"],
            "ETFs": ["ACTIVE", "NAME", "MARGIN", "MIN_SIZE", "CONTRACT_SIZE", "TICK_VALUE", 
                    "SWAP_LONG", "SWAP_SHORT", "ELIMINATED", "COMISSION_PER_ORDER", "SECTOR"],
        }
        self.dataframes_by_type: Dict[str, pd.DataFrame] = {}

    def load_data(self, file_path: str) -> None:
        """
        Load and clean data from CSV file.
        
        Args:
            file_path (str): Path to the CSV file containing operational costs data
        """
        # Read CSV file with all columns as strings
        df = pd.read_csv(file_path, header=None, dtype=str, sep=";")
        
        # Clean completely empty rows
        df = df.dropna(how='all')
        
        # Initialize dictionary for DataFrames by asset type
        self.dataframes_by_type = {asset_type: [] for asset_type in self.asset_keywords.values()}
        
        # Classify rows by asset type
        self._classify_rows(df)
        
        # Process each asset type DataFrame
        self._process_dataframes()

    def _classify_rows(self, df: pd.DataFrame) -> None:
        """
        Classify rows into different asset types based on keywords.
        
        Args:
            df (pd.DataFrame): Input DataFrame to classify
        """
        for index, row in df.iterrows():
            found = False
            for col in row:
                if isinstance(col, str):
                    for keyword, asset_type in self.asset_keywords.items():
                        if keyword.lower() in col.lower():
                            self.dataframes_by_type[asset_type].append(row)
                            found = True
                            break
                if found:
                    break

    def _process_dataframes(self) -> None:
        """Process and standardize each asset type DataFrame."""
        processed_dfs = {}
        for asset_type, rows in self.dataframes_by_type.items():
            if rows:  # Check if there are any rows
                df_asset = pd.DataFrame(rows)
                # Remove completely empty columns
                df_asset = df_asset.dropna(axis=1, how='all')
                
                # Adjust columns to match expected structure
                expected_columns = self.columns_by_sector.get(asset_type)
                if expected_columns:
                    df_asset = self._adjust_columns(df_asset, expected_columns, asset_type)
                
                processed_dfs[asset_type] = df_asset
            else:
                processed_dfs[asset_type] = pd.DataFrame()  # Empty DataFrame if no rows
        
        self.dataframes_by_type = processed_dfs

    def _adjust_columns(self, df: pd.DataFrame, expected_columns: List[str], asset_type: str) -> pd.DataFrame:
        """
        Adjust DataFrame columns to match expected structure.
        
        Args:
            df (pd.DataFrame): DataFrame to adjust
            expected_columns (List[str]): List of expected column names
            asset_type (str): Type of asset being processed
            
        Returns:
            pd.DataFrame: Adjusted DataFrame
        """
        num_expected = len(expected_columns)
        num_current = df.shape[1]
        
        if num_current > num_expected:
            df = df.iloc[:, :num_expected]
        elif num_current < num_expected:
            for i in range(num_expected - num_current):
                df[f"Extra_{i}"] = None
        
        if df.shape[1] == num_expected:
            df.columns = expected_columns
        else:
            print(f"Warning: Could not fully adjust columns for {asset_type}")
        
        return df

    def save_to_csv(self, output_prefix: str = "operative_costs") -> None:
        """
        Save each asset type DataFrame to a separate CSV file.
        
        Args:
            output_prefix (str): Prefix for output CSV files
        """
        for asset_type, df in self.dataframes_by_type.items():
            if isinstance(df, pd.DataFrame) and not df.empty:
                output_file = f"{output_prefix}_{asset_type}.csv"
                df.to_csv(output_file, index=False)
                print(f"Saved: {output_file}")

    def get_dataframe(self, asset_type: str) -> pd.DataFrame:
        """
        Get the DataFrame for a specific asset type.
        
        Args:
            asset_type (str): Type of asset to retrieve
            
        Returns:
            pd.DataFrame: DataFrame for the specified asset type
        """
        return self.dataframes_by_type.get(asset_type, pd.DataFrame())

    def display_summary(self) -> None:
        """Display summary of all processed DataFrames."""
        for asset_type, df in self.dataframes_by_type.items():
            print(f"\nDataFrame for '{asset_type}':")
            if isinstance(df, pd.DataFrame) and not df.empty:
                print(f"Shape: {df.shape}")
                print("\nFirst few rows:")
                print(df.head())
            else:
                print("No data available")

# Example usage
#if __name__ == "__main__":
#    processor = OperationalCostProcessor()
#    file_path = r"C:\Users\spinz\Documents\Portafolio Oficial\HERMESDB\data\tables_data\data_table\data_cost_operative\operative_costs.csv"
#    processor.load_data(file_path)
#    processor.save_to_csv()
#    processor.display_summary()