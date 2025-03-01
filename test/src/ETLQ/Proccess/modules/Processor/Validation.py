import pandas as pd
import os
from tqdm import tqdm  # Asegúrate de tener instalada la librería: pip install tqdm

class DataFrameValidator:
    REQUIRED_COLUMNS = [
        "time", "open", "high", "low", "close", "tick_volume", "spread", "real_volume", "timeframe", "broker", "asset"
    ]
    TIMEFRAME_VALUES = {"H1", "H4", "D1", "W1", "MN1"}
    BROKER_VALUES = {"Darwinex", "Dukascopy", "Oanda", "Tickmill", "Pepperstone"}
    
    @staticmethod
    def validate_csv_files_and_report(input_directory: str, output_directory: str):
        overall_results = []   # Reporte global: por archivo si pasó la validación
        detailed_results = []  # Reporte detallado: por archivo, cada prueba en booleano
        
        # Obtener solo archivos (ignora directorios)
        files = [file for file in os.listdir(input_directory) if os.path.isfile(os.path.join(input_directory, file))]
        
        for file in tqdm(files, desc="Validando archivos CSV"):
            file_path = os.path.join(input_directory, file)
            broker_name = os.path.splitext(file)[0]  # Extraer el nombre del broker del nombre del archivo
            
            # Si el broker extraído no es uno de los válidos, se ignora el archivo
            if broker_name not in DataFrameValidator.BROKER_VALUES:
                continue
            
            # Inicializar las variables de validación
            columns_valid = True
            broker_valid = True
            time_valid = True
            open_valid = True
            high_valid = True
            low_valid = True
            close_valid = True
            tick_volume_valid = True
            spread_valid = True
            real_volume_valid = True
            timeframe_valid = True
            asset_valid = True
            
            chunk_size = 100000  # Ajustable según memoria disponible
            try:
                for chunk in pd.read_csv(file_path, chunksize=chunk_size):
                    # Validar estructura de columnas
                    if list(chunk.columns) != DataFrameValidator.REQUIRED_COLUMNS:
                        columns_valid = False
                    
                    # Validar que la columna "broker" contenga el nombre esperado
                    if not chunk["broker"].eq(broker_name).all():
                        broker_valid = False
                    
                    # Validar la columna "time" convirtiéndola a datetime
                    time_series = pd.to_datetime(chunk["time"], errors='coerce')
                    if time_series.isna().any():
                        time_valid = False
                    
                    # Validar columnas numéricas
                    for col, flag in zip(
                        ["open", "high", "low", "close", "tick_volume", "spread", "real_volume"],
                        ["open_valid", "high_valid", "low_valid", "close_valid", "tick_volume_valid", "spread_valid", "real_volume_valid"]
                    ):
                        try:
                            chunk[col].astype(float)
                        except Exception:
                            if col == "open":
                                open_valid = False
                            elif col == "high":
                                high_valid = False
                            elif col == "low":
                                low_valid = False
                            elif col == "close":
                                close_valid = False
                            elif col == "tick_volume":
                                tick_volume_valid = False
                            elif col == "spread":
                                spread_valid = False
                            elif col == "real_volume":
                                real_volume_valid = False
                    
                    # Validar la columna "timeframe"
                    if not chunk["timeframe"].isin(DataFrameValidator.TIMEFRAME_VALUES).all():
                        timeframe_valid = False
                    
                    # Validar la columna "asset" (que sean strings)
                    if not chunk["asset"].apply(lambda x: isinstance(x, str)).all():
                        asset_valid = False
            except Exception as e:
                # Si ocurre cualquier error durante la lectura, se marca todo como inválido
                columns_valid = False
                broker_valid = False
                time_valid = False
                open_valid = False
                high_valid = False
                low_valid = False
                close_valid = False
                tick_volume_valid = False
                spread_valid = False
                real_volume_valid = False
                timeframe_valid = False
                asset_valid = False
            
            overall_valid = (columns_valid and broker_valid and time_valid and open_valid and high_valid 
                             and low_valid and close_valid and tick_volume_valid and spread_valid 
                             and real_volume_valid and timeframe_valid and asset_valid)
            
            # Acumular resultados generales
            overall_results.append({
                "file": file,
                "broker": broker_name,
                "overall_valid": overall_valid
            })
            
            # Acumular resultados detallados por cada prueba
            detailed_results.append({
                "file": file,
                "broker": broker_name,
                "columns_valid": columns_valid,
                "broker_valid": broker_valid,
                "time_valid": time_valid,
                "open_valid": open_valid,
                "high_valid": high_valid,
                "low_valid": low_valid,
                "close_valid": close_valid,
                "tick_volume_valid": tick_volume_valid,
                "spread_valid": spread_valid,
                "real_volume_valid": real_volume_valid,
                "timeframe_valid": timeframe_valid,
                "asset_valid": asset_valid
            })
        
        # Convertir resultados a DataFrame y exportar CSV
        overall_df = pd.DataFrame(overall_results)
        detailed_df = pd.DataFrame(detailed_results)
        
        # Asegurar que la carpeta de salida exista
        os.makedirs(output_directory, exist_ok=True)
        overall_csv_path = os.path.join(output_directory, "overall_report.csv")
        detailed_csv_path = os.path.join(output_directory, "detailed_report.csv")
        
        overall_df.to_csv(overall_csv_path, index=False)
        detailed_df.to_csv(detailed_csv_path, index=False)
        
        return overall_csv_path, detailed_csv_path

# Ejemplo de uso:
input_dir = r"C:\Users\spinz\OneDrive\Documentos\Portafolio oficial\HERMESDB\HERMESDB\test\data\processed"
output_dir = r"C:\Users\spinz\OneDrive\Documentos\Portafolio oficial\HERMESDB\HERMESDB\test\data\logs"  # Aquí indicas la carpeta donde se guardarán los CSV

report_paths = DataFrameValidator.validate_csv_files_and_report(input_dir, output_dir)
print("Reportes generados:", report_paths)
