import pandas as pd
import os
from pathlib import Path
from typing import Optional, Union

class MarketMetadataManager:
    """
    A class to manage market metadata and handle DataFrame operations including export functionality.
    """
    
    def __init__(self):
        """Initialize the MarketMetadataManager with predefined market metadata."""
        # Initialize empty lists for each column
        sectors = []
        instrument_types = []
        countries = []
        descriptions = []
        metadata_ids = []
        
        # Add data to lists
        data = [
            ('CROSSES', 'FOREX', 'INTERNATIONAL', 'Currency cross pairs, not including the USD.'),
            ('MAJORS', 'FOREX', 'INTERNATIONAL', 'Major currency pairs, always including the USD.'),
            ('METALS', 'FOREX', 'INTERNATIONAL', 'Precious or industrial metals traded in the foreign exchange or commodities market.'),
            ('AGRICULTURA', 'COMMODITIES', 'INTERNATIONAL', 'Agricultural products such as wheat, corn, coffee, among others.'),
            ('ENERGY', 'COMMODITIES', 'INTERNATIONAL', 'Energy resources such as oil, natural gas, among others.'),
            ('METALS', 'COMMODITIES', 'INTERNATIONAL', 'The commodity metals sector includes precious and non-precious metals, used in a wide range of industries, from the manufacture of electronic components and automobiles to construction and jewelry. The fluctuation in the supply and demand of these metals directly influences their prices, making this sector a key area in the global economy.'),
            ('AMERICA', 'INDICES', 'AMERICA', 'Stock market indices of America.'),
            ('ASIA', 'INDICES', 'ASIA', 'Stock market indices of Asia.'),
            ('EUROPE', 'INDICES', 'EUROPE', 'Stock market indices of Europe.'),
            ('AFRICA', 'INDICES', 'AFRICA', 'Stock market indices of Africa.'),
            ('BONDS', 'BONDS', 'INTERNATIONAL', 'Government and corporate bonds.'),
            ('AUSTRIA', 'STOCKS', 'AUSTRIA', 'Stock market shares of Austria.'),
            ('BELGIUM', 'STOCKS', 'BELGIUM', 'Stock market shares of Belgium.'),
            ('DENMARK', 'STOCKS', 'DENMARK', 'Stock market shares of Denmark.'),
            ('FINLAND', 'STOCKS', 'FINLAND', 'Stock market shares of Finland.'),
            ('FRANCE', 'STOCKS', 'FRANCE', 'Stock market shares of France.'),
            ('GERMANY', 'STOCKS', 'GERMANY', 'Stock market shares of Germany.'),
            ('HONG KONG', 'STOCKS', 'HONG KONG', 'Stock market shares of Hong Kong.'),
            ('IRELAND', 'STOCKS', 'IRELAND', 'Stock market shares of Ireland.'),
            ('ITALY', 'STOCKS', 'ITALY', 'Stock market shares of Italy.'),
            ('JAPAN', 'STOCKS', 'JAPAN', 'Stocks Stock market in Japan.'),
            ('MEXICO', 'STOCKS', 'MEXICO', 'Stock market in Mexico.'),
            ('NETHERLANDS', 'STOCKS', 'NETHERLANDS', 'Stock market in the Netherlands.'),
            ('NORWAY', 'STOCKS', 'NORWAY', 'Stock market in Norway.'),
            ('PORTUGAL', 'STOCKS', 'PORTUGAL', 'Stock market in Portugal.'),
            ('SPAIN', 'STOCKS', 'SPAIN', 'Stock market in Spain.'),
            ('SWEDEN', 'STOCKS', 'SWEDEN', 'Stock market in Sweden.'),
            ('SWITZERLAND', 'STOCKS', 'SWITZERLAND', 'Stock market in Switzerland.'),
            ('UK', 'STOCKS', 'UK', 'Stock market in the United Kingdom.'),
            ('US', 'STOCKS', 'US', 'Stock market in the United States.'),
            ('GERMANY', 'ETF', 'GERMANY', 'Exchange-traded funds (ETFs) in the German market.'),
            ('FRANCE', 'ETF', 'FRANCE', 'Exchange-traded funds (ETFs) in the French market.'),
            ('HONG KONG', 'ETF', 'HONG KONG', 'Exchange-traded funds (ETFs) in the Hong Kong market.'),
            ('US', 'ETF', 'US', 'Exchange-traded funds (ETFs) in the United States market.'),
            ('CRYPTO', 'CRYPTO', 'INTERNATIONAL', 'Decentralized Currencies')
        ]
        
        # Populate the lists
        for i, (sector, inst_type, country, desc) in enumerate(data):
            sectors.append(sector)
            instrument_types.append(inst_type)
            countries.append(country)
            descriptions.append(desc)
            metadata_ids.append(i)
        
        # Create the DataFrame
        self.metadata = pd.DataFrame({
            'SECTOR': sectors,
            'INSTRUMENT_TYPE': instrument_types,
            'COUNTRY': countries,
            'DESCRIPTION': descriptions,
            'METADATA_ID': metadata_ids
        })
    
    def export_metadata(self, 
                       output_path: Union[str, Path], 
                       file_format: str = 'csv') -> bool:
        """
        Export the metadata DataFrame to a specified location in the desired format.
        
        Args:
            output_path (Union[str, Path]): The path where the file should be saved
            file_format (str): The format to export ('csv', 'excel', 'json', 'pickle')
            
        Returns:
            bool: True if export was successful, False otherwise
        """
        try:
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            # Convert string path to Path object
            output_path = Path(output_path)
            
            # Export based on specified format, always excluding index
            if file_format.lower() == 'csv':
                self.metadata.to_csv(output_path, index=False)
            elif file_format.lower() == 'excel':
                self.metadata.to_excel(output_path, index=False)
            elif file_format.lower() == 'json':
                self.metadata.to_json(output_path, orient='records')
            elif file_format.lower() == 'pickle':
                self.metadata.to_pickle(output_path)
            else:
                raise ValueError(f"Unsupported file format: {file_format}")
            
            return True
            
        except Exception as e:
            print(f"Error exporting metadata: {str(e)}")
            return False
    
    def get_metadata(self) -> pd.DataFrame:
        """Return a copy of the metadata DataFrame."""
        return self.metadata.copy()
    
    def filter_by_sector(self, sector: str) -> pd.DataFrame:
        """Filter metadata by sector."""
        return self.metadata[self.metadata['SECTOR'] == sector.upper()]
    
    def filter_by_instrument_type(self, instrument_type: str) -> pd.DataFrame:
        """Filter metadata by instrument type."""
        return self.metadata[self.metadata['INSTRUMENT_TYPE'] == instrument_type.upper()]
    
    def filter_by_country(self, country: str) -> pd.DataFrame:
        """Filter metadata by country."""
        return self.metadata[self.metadata['COUNTRY'] == country.upper()]