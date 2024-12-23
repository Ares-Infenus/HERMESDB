import pandas as pd
import numpy as np
import asyncio
import multiprocessing
import psutil
import time
import json
from tqdm import tqdm
from sklearn.metrics.pairwise import cosine_similarity

class DuplicateChecker:
    """
    Clase para detectar similitudes entre DataFrames en un diccionario basado en el promedio de similitud coseno
    entre los valores de la columna 'Close'. 

    Propósito:
    Identificar grupos de DataFrames con alta similitud para analizar posibles duplicados o redundancias.

    Parámetros del constructor:
    - data_dict: Diccionario con DataFrames (clave: identificador, valor: DataFrame).
    - max_execution_time: Tiempo máximo de ejecución permitido (en segundos).
    - similarity_threshold: Umbral de similitud para considerar dos DataFrames como duplicados.
    - chunk_size: Tamaño de los bloques de datos a procesar en iteraciones (para grandes conjuntos de datos).
    """

    def __init__(self, data_dict, max_execution_time=3600, similarity_threshold=0.786, chunk_size=1000):
        self.data_dict = data_dict  # Diccionario de DataFrames a analizar
        self.max_execution_time = max_execution_time  # Tiempo máximo permitido para la ejecución
        self.similarity_threshold = similarity_threshold  # Umbral de similitud para marcar duplicados
        self.chunk_size = chunk_size  # Tamaño del bloque de datos para procesar
        self.start_time = None  # Tiempo de inicio de la ejecución
        self.end_time = None  # Tiempo de finalización de la ejecución
        self.report = {  # Diccionario para almacenar resultados y estadísticas
            "execution_time": "",
            "cpu_usage": "",
            "memory_usage": "",
            "duplicates": []  # Lista para almacenar pares de DataFrames duplicados
        }
        self.errors = []  # Lista para registrar errores ocurridos durante la comparación

    async def check_duplicates(self):
        """
        Función principal que coordina la detección de duplicados utilizando paralelismo.
        """
        self.start_time = time.time()  # Registrar el tiempo de inicio

        # Determinar el número de CPUs disponibles para procesamiento paralelo
        cpu_count = multiprocessing.cpu_count() - 1
        pool = multiprocessing.Pool(cpu_count)
        tasks = []  # Lista para almacenar tareas asíncronas

        # Crear tareas para comparar pares de DataFrames
        for key1, df1 in self.data_dict.items():
            for key2, df2 in self.data_dict.items():
                if key1 != key2:  # Evitar comparar un DataFrame consigo mismo
                    tasks.append(pool.apply_async(self.compare_dataframes, (key1, df1, key2, df2)))

        # Procesar las tareas y recolectar resultados
        for task in tqdm(tasks, desc="Comparing DataFrames"):
            result = task.get()
            if result:
                self.report["duplicates"].append(result)

        pool.close()
        pool.join()

        self.end_time = time.time()  # Registrar el tiempo de finalización
        self.generate_report()  # Generar informe final

    def compare_dataframes(self, key1, df1, key2, df2):
        """
        Compara dos DataFrames utilizando similitud coseno sobre la columna 'Close'.

        Parámetros:
        - key1, key2: Identificadores de los DataFrames a comparar.
        - df1, df2: DataFrames a comparar.

        Retorno:
        - Un diccionario con los identificadores de los DataFrames y el valor de similitud si supera el umbral.
        - None si la similitud no supera el umbral o ocurre un error.
        """
        try:
            # Extraer valores de la columna 'Close' y calcular la similitud coseno promedio
            df1_close = df1['Close'].values.reshape(-1, 1)
            df2_close = df2['Close'].values.reshape(-1, 1)
            similarity = cosine_similarity(df1_close, df2_close).mean()

            if similarity >= self.similarity_threshold:  # Comparar con el umbral definido
                return {
                    "group": [key1, key2],
                    "similarity": similarity
                }
        except Exception as e:
            # Registrar errores ocurridos durante la comparación
            self.errors.append(f"Error comparing {key1} and {key2}: {str(e)}")
        return None

    def generate_report(self):
        """
        Genera un informe con las estadísticas de la ejecución y los resultados de las comparaciones.

        Incluye:
        - Tiempo de ejecución
        - Uso de CPU y memoria
        - Pares de DataFrames duplicados
        """
        # Calcular el tiempo total de ejecución
        execution_time = self.end_time - self.start_time
        self.report["execution_time"] = time.strftime("%H:%M:%S", time.gmtime(execution_time))
        self.report["cpu_usage"] = f"{psutil.cpu_percent()}%"
        self.report["memory_usage"] = f"{psutil.virtual_memory().percent}%"

        # Guardar el informe en un archivo JSON
        with open("duplicate_report.json", "w") as f:
            json.dump(self.report, f, indent=4)

        # Guardar los errores en un archivo de texto
        with open("error_log.txt", "w") as f:
            for error in self.errors:
                f.write(error + "\n")

    def run(self):
        """
        Método para iniciar la detección de duplicados.
        """
        asyncio.run(self.check_duplicates())

# Ejemplo de uso
if __name__ == '__main__':
    # Crear un diccionario con DataFrames de ejemplo
    data_dict = {
        'df1': pd.DataFrame({'Close': np.random.rand(1000)}),
        'df2': pd.DataFrame({'Close': np.random.rand(1000)}),
        'df3': pd.DataFrame({'Close': np.random.rand(1000)})
    }

    # Instanciar la clase y ejecutar la detección de duplicados
    checker = DuplicateChecker(data_dict)
    checker.run()
