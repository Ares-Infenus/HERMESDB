import pandas as pd
import os

class DATA_SOURCE:
    def __init__(self, export_directory):
        """
        Initialize the DATA_SOURCE class with an export directory.
        
        Args:
            export_directory (str): Directory path where the CSV will be exported
        """
        self.export_directory = export_directory
        self.data = None
        self._create_dataframe()
        print("✓ DataFrame creado exitosamente")
        
    def _create_dataframe(self):
        """Create the default DataFrame with source information."""
        self.data = pd.DataFrame({
            'SOURCE_NAME': ['Dukascopy', 'Darwinex'],
            'DESCRIPTION': [
                "Dukascopy is a Swiss financial data provider and broker offering a wide range of financial instruments. Its historical data is known for its accuracy and high resolution, including ticks, candlesticks, and data in multiple timeframes. It is ideal for advanced technical analysis and detailed historical testing.",
                "Darwinex is a trading platform that combines brokerage and asset management services. It offers quality historical data focused on currency pairs, indices, commodities and other instruments. Its data is useful for traders who want to model strategies based on real market conditions."
            ]
        })
        self.data['SOURCE_ID'] = self.data.index
        
    def export_to_csv(self, filename='data_sources.csv'):
        """
        Export the DataFrame to a CSV file in the specified directory.
        
        Args:
            filename (str): Name of the CSV file (default: 'data_sources.csv')
        
        Returns:
            str: Path to the exported CSV file
        """
        # Create directory if it doesn't exist
        os.makedirs(self.export_directory, exist_ok=True)
        
        # Create full file path
        file_path = os.path.join(self.export_directory, filename)
        
        try:
            # Export to CSV
            self.data.to_csv(file_path, index=False)
            print("\n=== EXPORTACIÓN EXITOSA ===")
            print(f"✓ Archivo: {filename}")
            print(f"✓ Directorio: {self.export_directory}")
            print(f"✓ Ruta completa: {file_path}")
            print("========================")
            return file_path
        except Exception as e:
            print(f"❌ Error durante la exportación: {str(e)}")
            return None