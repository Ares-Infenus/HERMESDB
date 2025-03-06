# ----------------------------
# Descripcion
# ----------------------------
"""
El código define un decorador llamado mem_profile, que mide el uso de memoria en MiB
durante la ejecución de una función y almacena la máxima cantidad de memoria
consumida en el diccionario global memory_usage_dict, usando como clave una tupla
(nombre_archivo, nombre_función). Utiliza memory_usage de memory_profiler para capturar
el consumo de memoria antes y después de la ejecución de la función decorada y registra
la diferencia. También emplea functools.wraps para preservar los metadatos originales
de la función. Esto permite realizar un monitoreo eficiente del uso de memoria en
diferentes funciones dentro del proyecto.
"""

# ----------------------------
# librerias y dependencias
# ----------------------------

# Standard library imports
import functools
import os

# Third-party imports
from memory_profiler import memory_usage

# ----------------------------
# Conexiones
# ----------------------------


# ----------------------------
# Codigo
# ----------------------------

# Diccionario global para almacenar el consumo de memoria por función
# La clave será una tupla: (nombre_archivo, nombre_función)
memory_usage_dict = {}


def mem_profile(func):
    """
    Decorador para medir el incremento de memoria (en MiB) durante la ejecución de una función.
    Almacena el valor medido en memory_usage_dict con clave (nombre_archivo, nombre_función).
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        mem_before = memory_usage()[0]
        result = func(*args, **kwargs)
        mem_after = memory_usage()[0]
        mem_used = mem_after - mem_before

        # Se obtiene el nombre del archivo de la función
        filename = os.path.basename(func.__code__.co_filename)
        key = (filename, func.__name__)

        # Si ya se ha medido antes, se toma el máximo (o podrías acumular, según lo requieras)
        memory_usage_dict[key] = max(memory_usage_dict.get(key, 0), mem_used)
        return result

    return wrapper
