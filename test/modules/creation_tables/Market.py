import pandas as pd
from datetime import datetime
import os

class MarketData:
    """
    Esta clase es responsable de crear un DataFrame con información sobre diferentes mercados 
    (como FOREX, COMMODITIES, etc.), y exportarlo automáticamente a un archivo CSV en la carpeta 
    que elijas.
    
    Métodos:
    - create_market_data: Crea el DataFrame con información predefinida de los mercados.
    - export_to_csv: Guarda el DataFrame en un archivo CSV en la carpeta indicada.
    """
    
    def __init__(self, output_folder):
        """
        Constructor de la clase. Inicializa el objeto y crea el DataFrame de mercado.
        
        Parámetros:
        - output_folder (str): La carpeta donde se guardará el archivo CSV.
        """
        # Almacenamos la ruta de la carpeta de salida
        self.output_folder = output_folder
        
        # Llamamos al método para crear el DataFrame con la información del mercado
        self.market_data = self.create_market_data()

    def create_market_data(self):
        """
        Este método crea el DataFrame con información acerca de los mercados.
        
        Devuelve:
        - market_data (DataFrame): Un DataFrame de pandas con los detalles de los mercados.
        """
        # Crear un DataFrame vacío
        market_data = pd.DataFrame()
        
        # Agregar información sobre los mercados
        market_data['MARKET_NAME'] = ['FOREX', 'COMMODITIES', 'INDICES', 'BONDS', 'STOCKS', 'ETFs', 'CRYPTO']
        market_data['DESCRIPTION'] = [
            'International market for currency trading, known for its high liquidity and 24-hour operation.',
            'Market for the trading of physical commodities or derivative contracts for products such as metals, energy and agricultural products.',
            'Market where indices representing the aggregate performance of stocks or economic sectors are traded.',
            'Debt market where investors buy bonds as a form of financing for governments and corporations. ',
            'Market where shares of public companies are traded, representing partial ownership in such companies.',
            'Exchange-traded investment funds that combine diversification and flexibility in equity trading.',
            'Decentralized blockchain-based digital asset market, known for its high volatility and rapid growth.'
        ]
        
        # Obtener la fecha y hora actual en formato 'Año-Mes-Día Hora:Minuto:Segundo'
        fecha_actual = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Añadir la fecha de creación y actualización del DataFrame
        market_data['CREATED_AT'] = fecha_actual
        market_data['UPDATED_AT'] = fecha_actual
        
        # Agregar un identificador único para cada mercado (basado en el índice del DataFrame)
        market_data['ID_MARKET'] = market_data.index
        
        # Devolver el DataFrame creado
        return market_data

    def export_to_csv(self):
        """
        Este método guarda el DataFrame en un archivo CSV en la carpeta especificada.
        Si la carpeta no existe, la crea automáticamente.
        
        El archivo CSV será nombrado con la fecha y hora actual para hacerlo único.
        """
        # Verificar si la carpeta de salida existe
        # Si no existe, la creamos
        if not os.path.exists(self.output_folder):
            os.makedirs(self.output_folder)
        
        # Crear un nombre único para el archivo CSV usando la fecha y hora actual
        filename = "MARKET.csv"
        
        # Unir la ruta de la carpeta con el nombre del archivo
        file_path = os.path.join(self.output_folder, filename)
        
        # Guardar el DataFrame en un archivo CSV en la carpeta especificada
        self.market_data.to_csv(file_path, index=False)
        
        # Imprimir un mensaje confirmando que el archivo ha sido guardado
        print(f"Archivo exportado a: {file_path}")


