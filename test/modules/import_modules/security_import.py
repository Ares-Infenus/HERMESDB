import re
import pandas as pd
import numpy as np
import asyncio
import multiprocessing
import psutil
import time
import json
from tqdm import tqdm
from sklearn.metrics.pairwise import cosine_similarity
# Función principal para verificar la estructura y validez de un diccionario de archivos CSV.
# Este diccionario contiene los datos organizados por mercado, activo, intervalo y los archivos CSV asociados.
# Se realizan diversas validaciones para asegurar que los archivos cumplen con las expectativas de formato y organización.
def verificar_diccionario(diccionario):
    """
    Verifica la estructura y validez de los archivos CSV en un diccionario de datos.

    Esta función verifica las siguientes condiciones:
    - Si los archivos están presentes y no son de más.
    - Si los archivos ASK y BID están correctamente nombrados y no están duplicados.
    - Si los archivos están correctamente organizados dentro de las carpetas.
    - Si los archivos siguen el formato correcto de nombre (ACTIVO_INTERVALO_ASK/BID.csv).

    Parámetros:
        diccionario (dict): Un diccionario que contiene la estructura de mercados, activos, intervalos y los archivos CSV asociados.

    Retorna:
        dict: Un diccionario con los errores encontrados y un resumen de las validaciones.
              El resumen incluye el número de errores por tipo (archivos de más, duplicados, faltantes, errores de nombres y carpetas vacías).
    """
    # Diccionario para almacenar los errores encontrados en las validaciones
    errores = {}

    # Contadores de errores clasificados por tipo
    error_count = {
        "Archivos de más": 0,
        "Duplicados": 0,
        "Faltantes": 0,
        "Errores en nombres": 0,
        "Carpetas vacías": 0
    }
    
    # ID único para cada error encontrado
    error_id = 1

    # Función auxiliar para agregar un error al diccionario de errores
    def agregar_error(mensaje):
        """
        Agrega un mensaje de error al diccionario de errores.

        Esta función asigna un ID único a cada error y lo guarda en el diccionario de errores con una clave
        basada en el ID incrementado.
        
        Parámetros:
            mensaje (dict): Un diccionario con los detalles del error, como el mercado, activo, intervalo y el mensaje de error.
        """
        nonlocal error_id  # Usamos error_id de la función principal
        errores[f"Error{error_id}"] = mensaje  # Agregamos el mensaje de error al diccionario
        error_id += 1  # Incrementamos el ID para el siguiente error

    # Función para validar si un archivo tiene el formato correcto
    def validar_archivo(nombre_archivo, activo, intervalo):
        """
        Verifica si el nombre de un archivo cumple con el formato esperado.

        El formato esperado es "ACTIVO_INTERVALO_ASK/BID.csv", donde ACTIVO y INTERVALO son variables dinámicas.

        Parámetros:
            nombre_archivo (str): El nombre del archivo a validar.
            activo (str): El nombre del activo (por ejemplo, "EURUSD").
            intervalo (str): El intervalo de tiempo (por ejemplo, "1H").

        Retorna:
            bool: True si el archivo cumple con el formato, False en caso contrario.
        """
        patron = rf"^{activo}_{intervalo}_(ASK|BID)\.csv$"
        return re.match(patron, nombre_archivo) is not None  # Retorna True si el archivo coincide con el patrón

    # Iteramos sobre el diccionario de datos, verificando cada elemento
    for mercado, activos in diccionario.items():
        for activo, intervalos in activos.items():
            for intervalo, archivos in intervalos.items():
                # Verificamos si la carpeta está vacía, en cuyo caso se reporta un error
                if not archivos:
                    agregar_error({
                        "Mercado": mercado,
                        "Activo": activo,
                        "Horario": intervalo,
                        "Error": "Carpeta vacía o archivo no compatible, revise documentación"
                    })
                    error_count["Carpetas vacías"] += 1
                    continue  # Continuamos al siguiente intervalo si la carpeta está vacía

                # Filtramos los archivos por tipo: ASK, BID y otros desconocidos
                archivos_ask = [archivo for archivo in archivos if "ASK" in archivo]
                archivos_bid = [archivo for archivo in archivos if "BID" in archivo]
                archivos_desconocidos = [archivo for archivo in archivos if "ASK" not in archivo and "BID" not in archivo]

                # Si hay archivos desconocidos, los reportamos como "Archivos de más"
                for archivo in archivos_desconocidos:
                    agregar_error({
                        "Mercado": mercado,
                        "Activo": activo,
                        "Horario": intervalo,
                        "Error": f"Archivo de más: {archivo}"
                    })
                    error_count["Archivos de más"] += 1

                # Verificamos que no haya duplicados de archivos ASK
                if len(archivos_ask) > 1:
                    for archivo in archivos_ask:
                        agregar_error({
                            "Mercado": mercado,
                            "Activo": activo,
                            "Horario": intervalo,
                            "Error": f"Duplicación de archivo: {archivo}"
                        })
                        error_count["Duplicados"] += 1

                # Verificamos que no haya duplicados de archivos BID
                if len(archivos_bid) > 1:
                    for archivo in archivos_bid:
                        agregar_error({
                            "Mercado": mercado,
                            "Activo": activo,
                            "Horario": intervalo,
                            "Error": f"Duplicación de archivo: {archivo}"
                        })
                        error_count["Duplicados"] += 1

                # Si falta el archivo ASK, lo reportamos como faltante
                if len(archivos_ask) == 0:
                    agregar_error({
                        "Mercado": mercado,
                        "Activo": activo,
                        "Horario": intervalo,
                        "Error": "Falta el archivo ASK"
                    })
                    error_count["Faltantes"] += 1

                # Si falta el archivo BID, lo reportamos como faltante
                if len(archivos_bid) == 0:
                    agregar_error({
                        "Mercado": mercado,
                        "Activo": activo,
                        "Horario": intervalo,
                        "Error": "Falta el archivo BID"
                    })
                    error_count["Faltantes"] += 1

                # Verificamos que todos los archivos tengan el nombre correcto
                for archivo in archivos:
                    if not validar_archivo(archivo, activo, intervalo):
                        agregar_error({
                            "Mercado": mercado,
                            "Activo": activo,
                            "Horario": intervalo,
                            "Error": "Error en la declaración de intervalos y activos"
                        })
                        error_count["Errores en nombres"] += 1

    # Finalmente, agregamos el resumen con la cantidad de errores por tipo
    errores["Resumen"] = error_count
    return errores  # Retornamos el diccionario con los errores encontrados

# Llamar a la función con el diccionario de datos para generar el reporte de errores
#reporte_errores = verificar_diccionario(diccionario_datos)
#print(reporte_errores)




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
