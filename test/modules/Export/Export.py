import os
import pandas as pd

class DataFrameExporter:
    """
    La clase DataFrameExporter permite exportar un diccionario jerárquico de DataFrames
    a archivos CSV, manteniendo la estructura de carpetas original del diccionario.
    
    Atributos:
        data_dict (dict): Diccionario que contiene DataFrames en su estructura.
        base_path (str): Ruta base donde se creará la estructura de carpetas y se exportarán los archivos.
    
    Métodos:
        export(): Exporta todos los DataFrames del diccionario a archivos CSV en la ruta especificada.
    """
    
    def __init__(self, data_dict, base_path):
        """
        Inicializa la instancia de DataFrameExporter con el diccionario de datos y la ruta base.
        
        Parámetros:
            data_dict (dict): Diccionario que contiene DataFrames a ser exportados.
            base_path (str): Ruta base donde se crearán las carpetas y se exportarán los archivos CSV.
        """
        self.data_dict = data_dict
        self.base_path = base_path

    def export(self):
        """
        Exporta recursivamente los DataFrames dentro de `data_dict` a archivos CSV,
        creando las carpetas necesarias según la estructura del diccionario.
        El proceso incluye la creación de carpetas y la exportación de los archivos, además
        de proporcionar un informe detallado de las acciones realizadas.
        """
        created_folders = set()  # Conjunto para almacenar las carpetas creadas.
        exported_files = []  # Lista para almacenar los archivos exportados.
        warnings = []  # Lista para almacenar advertencias, como DataFrames vacíos.

        def create_folder_structure(path):
            """
            Crea una carpeta en el sistema si no existe.
            
            Parámetros:
                path (str): Ruta de la carpeta a crear.
            """
            if not os.path.exists(path):
                os.makedirs(path)
                created_folders.add(path)  # Agrega la ruta a las carpetas creadas.

        def export_dataframe(path, df, filename):
            """
            Exporta un DataFrame a un archivo CSV si no está vacío.
            
            Parámetros:
                path (str): Ruta de la carpeta donde se guardará el archivo.
                df (pd.DataFrame): DataFrame que se exportará.
                filename (str): Nombre del archivo CSV a exportar.
            """
            if not df.empty:
                if not filename.endswith('.csv'):
                    filename += '.csv'  # Asegura que el archivo termine en '.csv'.
                file_path = os.path.join(path, filename)
                df.reset_index(inplace=True)  # Resetea el índice para incluirlo como columna.
                df.to_csv(file_path, index=False)  # Exporta el DataFrame sin el índice como columna adicional.
                exported_files.append(file_path)  # Agrega el archivo exportado a la lista.
            else:
                warnings.append(f"Warning: DataFrame for {filename} is empty and was not exported.")  # Añade advertencia si el DataFrame está vacío.

        def process_dict(d, current_path):
            """
            Procesa recursivamente un diccionario, exportando los DataFrames y creando carpetas.
            
            Parámetros:
                d (dict): Diccionario que contiene claves y valores, donde los valores pueden ser DataFrames o subdiccionarios.
                current_path (str): Ruta actual en la que se está procesando el diccionario.
            """
            for key, value in d.items():
                new_path = os.path.join(current_path, key)  # Calcula la nueva ruta para cada subdiccionario.
                if isinstance(value, dict):
                    create_folder_structure(new_path)  # Crea la carpeta para el subdiccionario.
                    process_dict(value, new_path)  # Llama recursivamente para procesar el subdiccionario.
                elif isinstance(value, pd.DataFrame):
                    create_folder_structure(current_path)  # Crea la carpeta en la ruta actual si es un DataFrame.
                    export_dataframe(current_path, value, key)  # Exporta el DataFrame.
                else:
                    warnings.append(f"Warning: Key {key} does not contain a DataFrame.")  # Añade advertencia si el valor no es un DataFrame.

        process_dict(self.data_dict, self.base_path)  # Inicia el procesamiento del diccionario.

        # Imprime el informe detallado de la exportación.
        print("Export Report:")
        print("Created Folders:")
        for folder in created_folders:
            print(f" - {folder}")
        print("Exported Files:")
        for file in exported_files:
            print(f" - {file}")
        if warnings:
            print("Warnings:")
            for warning in warnings:
                print(f" - {warning}")


# Ejemplo de uso:
data_dict = {
    'crypto': {  # Carpeta principal 'crypto'
        'BTCUSD': {  # Subcarpeta 'BTCUSD'
            '1D': {  # Subcarpeta para la temporalidad '1D'
                'BTCUSD_1D_ASK': pd.DataFrame({  # DataFrame con datos de BTCUSD para la temporalidad 1D
                    'Date': ['2021-01-01', '2021-01-02'], 
                    'col1': [1, 2], 
                    'col2': [3, 4]
                }).set_index('Date'),  # Establecer 'Date' como índice
                'BTCUSD_1D_BID': pd.DataFrame({
                    'Date': ['2021-01-01', '2021-01-02'],
                    'col1': [5, 6],
                    'col2': [7, 8]
                }).set_index('Date')  # Otro DataFrame con datos para 'BID'
            }
        }
    }
}

# Crear una instancia del exportador de DataFrames
exporter = DataFrameExporter(data_dict, '/path/to/export')

# Llamar al método 'export' para exportar los DataFrames
exporter.export()
