import os
import pandas as pd
from multiprocessing import Pool, cpu_count

def importar_carpetas_a_diccionario(direccion_base):
    """
    Importa todos los archivos CSV de una estructura de carpetas utilizando multiprocesamiento
    y los organiza en un diccionario siguiendo la misma jerarquía de carpetas.

    Parámetros:
    direccion_base (str): Ruta base de la carpeta que contiene los datos.

    Retorna:
    dict: Diccionario donde las claves representan las carpetas y
          los valores son DataFrames o diccionarios anidados.
    """
    def leer_csv(ruta):
        """Función para leer un archivo CSV y manejar errores."""
        try:
            return pd.read_csv(ruta)
        except Exception as e:
            print(f"Error al leer {ruta}: {e}")
            return None

    def recorrer_carpetas(ruta):
        """
        Recorre las carpetas de forma recursiva para construir el diccionario
        sin leer aún los CSV.
        """
        estructura = {}
        archivos_csv = []
        for nombre in os.listdir(ruta):
            ruta_completa = os.path.join(ruta, nombre)
            if os.path.isdir(ruta_completa):
                # Carpeta: procesar subcarpetas recursivamente
                estructura[nombre] = recorrer_carpetas(ruta_completa)
            elif os.path.isfile(ruta_completa) and nombre.endswith('.csv'):
                archivos_csv.append(ruta_completa)
        
        # Retornar estructura y lista de archivos CSV en este nivel
        return estructura, archivos_csv

    def construir_diccionario(estructura, resultados):
        """
        Inserta los DataFrames leídos en la estructura del diccionario.
        """
        for clave, valor in estructura.items():
            if isinstance(valor, tuple):
                subestructura, csv_rutas = valor
                estructura[clave] = construir_diccionario(subestructura, resultados)
            else:
                estructura[clave] = valor

        # Asignar resultados a archivos CSV en este nivel
        if 'archivos_csv' in estructura:
            for ruta in estructura['archivos_csv']:
                nombre_archivo = os.path.basename(ruta)
                if ruta in resultados:
                    estructura[nombre_archivo] = resultados[ruta]

        return estructura

    # Crear estructura inicial y lista de archivos CSV
    estructura, archivos_csv = recorrer_carpetas(direccion_base)

    # Leer los archivos CSV utilizando multiprocesamiento
    with Pool(processes=cpu_count()) as pool:
        resultados = pool.map(leer_csv, archivos_csv)

    # Asociar resultados a sus rutas originales
    resultados_dict = {ruta: df for ruta, df in zip(archivos_csv, resultados) if df is not None}

    # Construir el diccionario final con los DataFrames
    return construir_diccionario(estructura, resultados_dict)

# Hiperparametros de Uso Ejemplo:
#if __name__ == "__main__":
#    direccion_base = "ruta/a/tu/carpeta"
#    diccionario_datos = importar_carpetas_a_diccionario("C:\\Users\\spinz\\Documents\\Portafolio Oficial\\HERMESDB\\data\\raw")
#    print(diccionario_datos)
