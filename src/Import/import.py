import os
import pandas as pd

def cargar_dataframes(carpeta_base):
    estructura = {}

    for root, dirs, files in os.walk(carpeta_base):
        # Obtener la ruta relativa desde la carpeta base
        ruta_relativa = os.path.relpath(root, carpeta_base)
        partes_ruta = ruta_relativa.split(os.sep)

        # Navegar a través del diccionario para llegar a la ubicación correcta
        actual = estructura
        for parte in partes_ruta:
            if parte not in actual:
                actual[parte] = {}
            actual = actual[parte]

        # Cargar los archivos CSV en la ubicación correcta del diccionario
        for file in files:
            if file.endswith('.csv'):
                nombre_archivo = os.path.splitext(file)[0]
                ruta_completa = os.path.join(root, file)
                actual[nombre_archivo] = pd.read_csv(ruta_completa)

    return estructura

# Ruta de la carpeta base
carpeta_base = 'C:\\Users\\spinz\\Documents\\Portafolio Oficial\\HERMESDB\\data\\raw'

# Cargar los dataframes
diccionario_datos = cargar_dataframes(carpeta_base)