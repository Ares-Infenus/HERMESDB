import pandas as pd
import os
from datetime import datetime

class CSVProcessor:
    def __init__(self, input_folder, output_file):
        self.input_folder = input_folder
        self.output_file = output_file
        self.chunksize = 100000  # Procesar 100,000 filas a la vez
        self.current_timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    def process_files(self):
        # Crear archivo de salida vacío y agregar encabezados una sola vez
        with open(self.output_file, "w") as f:
            header = [
                "DATA_ID", "DATE_RECORDED", "OPEN", "HIGH", "LOW", "CLOSE",
                "TICK_VOLUME", "SPREAD", "VOLUME", "TIMEFRAME_ID", "ASSETS_ID",
                "CREATED_AT", "UPDATED_AT", "PRICE_TYPE"
            ]
            f.write(",".join(header) + "\n")

        # Iterar sobre los archivos en la carpeta
        for file in os.listdir(self.input_folder):
            if file.endswith(".csv"):
                file_path = os.path.join(self.input_folder, file)
                print(f"Procesando archivo: {file_path}")

                # Leer el archivo por chunks
                for chunk in pd.read_csv(file_path, chunksize=self.chunksize):
                    # Renombrar columnas al formato deseado
                    chunk.columns = [
                        "DATE_RECORDED", "OPEN", "HIGH", "LOW", "CLOSE",
                        "TICK_VOLUME", "SPREAD", "VOLUME", "TIMEFRAME_ID", "ASSETS_ID"
                    ]

                    # Agregar nuevas columnas
                    chunk["CREATED_AT"] = self.current_timestamp
                    chunk["UPDATED_AT"] = self.current_timestamp
                    chunk["PRICE_TYPE"] = "BID"
                    chunk.insert(0, "DATA_ID", range(chunk.index[0] + 1, chunk.index[0] + 1 + len(chunk)))

                    # Guardar el chunk procesado en el archivo final
                    chunk.to_csv(self.output_file, mode="a", index=False, header=False)

        print(f"Archivos combinados en: {self.output_file}")

# Uso de la clase
if __name__ == "__main__":
    input_folder = "C:\\Users\\spinz\\Documents\\Portafolio Oficial\\HERMESDB\\data\\tables_data\\data_table\\data_market"
    output_file = "C:\\Users\\spinz\\Documents\\Portafolio Oficial\\HERMESDB\\data\\tables_data\\MARKET_DATA.csv"

    processor = CSVProcessor(input_folder, output_file)
    processor.process_files()
