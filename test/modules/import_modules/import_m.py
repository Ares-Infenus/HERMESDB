import os
import pandas as pd

def cargar_dataframes(carpeta_base):
    """
    Carga todos los archivos CSV desde una carpeta base y los organiza en un diccionario 
    con la estructura de subcarpetas como claves. Los archivos CSV son cargados como 
    DataFrames de pandas.
    
    Parámetros:
        carpeta_base (str): Ruta a la carpeta principal desde donde se iniciará la búsqueda 
                             de archivos CSV.
    
    Retorna:
        dict: Un diccionario que representa la estructura de carpetas, donde las claves 
              son las subcarpetas y los archivos CSV son los valores, que se almacenan como 
              DataFrames de pandas.
    """
    
    # Diccionario que almacenará los DataFrames organizados por carpeta
    estructura = {}

    # Recorrer recursivamente la carpeta base para encontrar todos los archivos
    for root, dirs, files in os.walk(carpeta_base):
        # Obtener la ruta relativa desde la carpeta base
        ruta_relativa = os.path.relpath(root, carpeta_base)
        
        # Dividir la ruta relativa en partes para crear la estructura jerárquica
        partes_ruta = ruta_relativa.split(os.sep)

        # Navegar a través del diccionario para llegar a la ubicación correcta
        actual = estructura
        for parte in partes_ruta:
            # Si la parte de la ruta no existe en el diccionario, la creamos
            if parte not in actual:
                actual[parte] = {}
            actual = actual[parte]

        # Cargar los archivos CSV que encontramos en esta carpeta
        for file in files:
            # Verificar si el archivo es un archivo CSV
            if file.endswith('.csv'):
                # Obtener el nombre del archivo sin la extensión
                nombre_archivo = os.path.splitext(file)[0]
                
                # Obtener la ruta completa del archivo CSV
                ruta_completa = os.path.join(root, file)
                
                # Cargar el archivo CSV en un DataFrame y almacenarlo en el diccionario
                actual[nombre_archivo] = pd.read_csv(ruta_completa)

    # Devolver la estructura de datos que contiene todos los DataFrames organizados
    return estructura


#===============================# Ejemplo #===============================#
# Ruta de la carpeta base (ajústala según tu estructura de directorios)
#carpeta_base = 'C:\\Users\\spinz\\Documents\\Portafolio Oficial\\HERMESDB\\data\\raw'
# Llamar a la función para cargar los archivos CSV en la estructura
#diccionario_datos = cargar_dataframes(carpeta_base)

def agregar_extension_csv(diccionario):
    """
    Agrega la extensión '.csv' a las claves que corresponden a DataFrames en el diccionario.
    
    Esta función recorre de manera recursiva un diccionario, y cuando encuentra un valor
    que es un DataFrame (pd.DataFrame), agrega la extensión '.csv' al nombre de la clave 
    asociada. Si el valor es otro diccionario, la función se llama recursivamente sobre 
    ese diccionario para procesar sus claves de manera similar.
    
    Argumentos:
    diccionario : dict
        Un diccionario que puede contener DataFrames como valores, o a su vez otros diccionarios.
    
    Retorna:
    dict
        Un nuevo diccionario con las claves de los DataFrames actualizadas con la extensión '.csv'.
    """
    
    nuevo_diccionario = {}  # Diccionario vacío para almacenar las claves modificadas
    
    # Itera sobre las claves y valores del diccionario original
    for clave, valor in diccionario.items():
        
        # Si el valor es un DataFrame, se agrega la extensión '.csv' a la clave
        if isinstance(valor, pd.DataFrame):
            nuevo_diccionario[clave + '.csv'] = valor
        
        # Si el valor es otro diccionario, se llama recursivamente a la función
        else:
            nuevo_diccionario[clave] = agregar_extension_csv(valor)
    
    return nuevo_diccionario

# Ejemplo de uso:
# Actualiza el diccionario de datos agregando la extensión '.csv' a las claves de los DataFrames.
# diccionario_datos = agregar_extension_csv(diccionario_datos)
