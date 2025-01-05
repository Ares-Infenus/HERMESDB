import pandas as pd
import os
from datetime import datetime
from multiprocessing import Pool, cpu_count

class CSVProcessor:
    def __init__(self, input_folder, output_file, assets_df):
        self.input_folder = input_folder
        self.output_file = output_file
        self.chunksize = 100000  # Procesar 100,000 filas a la vez
        self.current_timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        self.assets_df = assets_df  # DataFrame con los IDs de los activos

    def initialize_output_file(self):
        """Crear archivo de salida vacío con encabezados."""
        with open(self.output_file, "w") as f:
            header = [
                "DATA_ID", "DATE_RECORDED", "OPEN", "HIGH", "LOW", "CLOSE",
                "TICK_VOLUME", "SPREAD", "VOLUME", "TIMEFRAME_ID", "ASSETS_ID",
                "CREATED_AT", "UPDATED_AT", "PRICE_TYPE"
            ]
            f.write(",".join(header) + "\n")

    def get_asset_id(self, asset_name):
        """Obtener el ASSETS_ID del DataFrame basado en el nombre del activo."""
        asset_row = self.assets_df[self.assets_df['ASSETS_NAME'] == asset_name]
        if not asset_row.empty:
            return asset_row.iloc[0]['ASSETS_ID']
        else:
            return None  # Si no se encuentra el activo, retorna None
 
    def process_file(self, file):
        """Procesar un solo archivo CSV."""
        file_path = os.path.join(self.input_folder, file)
        print(f"Procesando archivo: {file_path}")

        # Mapeo para reemplazar TIMEFRAME_ID
        timeframe_mapping = {
            "1H": 4,
            "4H": 5,
            "1D": 1,
            "1W": 3,
            "1M": 2
        }

        # Procesar el archivo en chunks
        output_chunks = []
        for chunk in pd.read_csv(file_path, chunksize=self.chunksize):
            # Renombrar columnas al formato deseado
            chunk.columns = [
                "DATE_RECORDED", "OPEN", "HIGH", "LOW", "CLOSE",
                "TICK_VOLUME", "SPREAD", "VOLUME", "TIMEFRAME_ID", "ASSETS_NAME"
            ]

            # Convertir DATE_RECORDED al formato timestamp compatible con Oracle
            chunk["DATE_RECORDED"] = pd.to_datetime(chunk["DATE_RECORDED"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")

            # Reemplazar el nombre del activo con su ASSETS_ID
            chunk["ASSETS_NAME"] = chunk["ASSETS_NAME"].apply(self.get_asset_id)

            # Validar si hay activos no encontrados
            if chunk["ASSETS_NAME"].isnull().any():
                missing_assets = chunk[chunk["ASSETS_NAME"].isnull()]
                print(f"Advertencia: Activos no encontrados:\n{missing_assets}")
                # Opcional: Puedes manejar errores aquí, como detener el proceso si es crítico

            # Renombrar columna ASSETS_NAME a ASSETS_ID
            chunk.rename(columns={"ASSETS_NAME": "ASSETS_ID"}, inplace=True)

            # Reemplazar valores en la columna TIMEFRAME_ID
            chunk["TIMEFRAME_ID"] = chunk["TIMEFRAME_ID"].map(timeframe_mapping)

            # Agregar nuevas columnas
            chunk["CREATED_AT"] = self.current_timestamp
            chunk["UPDATED_AT"] = self.current_timestamp
            chunk["PRICE_TYPE"] = "BID"
            chunk.insert(0, "DATA_ID", range(chunk.index[0] + 1, chunk.index[0] + 1 + len(chunk)))

            # Guardar en un buffer temporal
            output_chunks.append(chunk)

        # Concatenar todos los chunks procesados y guardar en el archivo final
        pd.concat(output_chunks).to_csv(self.output_file, mode="a", index=False, header=False)
        print(f"Archivo procesado: {file_path}")


    def process_files(self):
        """Procesar todos los archivos CSV en paralelo."""
        # Crear archivo de salida vacío
        self.initialize_output_file()

        # Obtener lista de archivos CSV en la carpeta
        files = [file for file in os.listdir(self.input_folder) if file.endswith(".csv")]

        # Usar multiprocesamiento para procesar los archivos
        with Pool(processes=max(cpu_count() - 1, 1)) as pool:  # Usa todos los núcleos menos 1
            pool.map(self.process_file, files)

        print(f"Archivos combinados en: {self.output_file}")


# Uso de la clase
if __name__ == "__main__":
    # Cargar el DataFrame de activos
    assets_df = pd.read_csv("C:\\Users\\spinz\\Documents\\Portafolio Oficial\\HERMESDB\\data\\tables_data\\ASSETS.csv")

    input_folder = "C:\\Users\\spinz\\Documents\\Portafolio Oficial\\HERMESDB\\data\\tables_data\\data_table\\data_market"
    output_file = "C:\\Users\\spinz\\Documents\\Portafolio Oficial\\HERMESDB\\data\\tables_data\\MARKET_DATA_BID.csv"

    processor = CSVProcessor(input_folder, output_file, assets_df)
    processor.process_files()
