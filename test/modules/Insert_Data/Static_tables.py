import pandas as pd

def get_market_data():
    # Creación del diccionario de datos con las descripciones en inglés
    market_data = {
        'market_id': [1, 2, 3, 4, 5, 6, 7],
        'market_name': ['FOREX', 'COMMODITIES', 'INDICES', 'BONDS', 'STOCKS', 'ETFs', 'CRYPTO'],
        'description': [
            'International market for currency trading, known for its high liquidity and 24-hour operation.',
            'Market for the buying and selling of physical commodities or derivative contracts on products such as metals, energy, and agriculture.',
            'Market where indices representing the combined performance of stocks or economic sectors are traded.',
            'Debt market where investors buy bonds as a form of financing for governments and companies.',
            'Market where stocks of public companies are traded, representing partial ownership in those companies.',
            'Exchange-traded funds that combine diversification and flexibility in stock trading.',
            'Market for decentralized digital assets based on blockchain, known for its high volatility and rapid growth.'
        ],
        'created_at': [
            pd.Timestamp('2024-12-28 12:00:00'),
            pd.Timestamp('2024-12-28 14:30:00'),
            pd.Timestamp('2024-12-28 12:00:00'),
            pd.Timestamp('2024-12-28 14:30:00'),
            pd.Timestamp('2024-12-28 12:00:00'),
            pd.Timestamp('2024-12-28 14:30:00'),
            pd.Timestamp('2024-12-28 12:00:00')
        ],
        'updated_at': [pd.Timestamp.now()] * 7  # Hora actual para todos los elementos
    }

    # Crear el DataFrame
    df = pd.DataFrame(market_data)
    
    return df
