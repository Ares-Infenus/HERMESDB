import pandas as pd
from datetime import datetime

class TimeFrameProcessor:
    def __init__(self, output_csv_path):
        """
        Inicializa el procesador de intervalos de tiempo y genera el DataFrame.
        
        :param output_csv_path: Ruta donde se exportará el DataFrame generado.
        """
        self.output_csv_path = output_csv_path
        self.timeframe_data = self._create_timeframe_data()

    def _create_timeframe_data(self):
        """
        Crea el DataFrame con los intervalos de tiempo y sus detalles.
        
        :return: DataFrame con los intervalos de tiempo generados.
        """
        timeframe = pd.DataFrame()
        timeframe['TIMERAME_NAME'] = ['H1', 'H4', 'Daily', 'Weekly', 'Monthly']
        timeframe['MINUTES_INTERVAL'] = [60, 240, 1440, 7200, 28800]  # Recuerda que no se cuentan los sábados y domingos.
        timeframe['CREATED_AT'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        timeframe['TIMEFRAME_ID'] = timeframe.index
        return timeframe

    def save_to_csv(self):
        """
        Guarda el DataFrame generado en el archivo CSV especificado.
        """
        self.timeframe_data.to_csv(self.output_csv_path, index=False)
        print(f"Archivo procesado y guardado en: {self.output_csv_path}")
