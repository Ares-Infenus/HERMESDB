#==============================#            Description            #==============================#

"""Este módulo se encarga de gestionar y administrar la exportacion de datos, asegurando una integración eficiente entre los módulos de procesamiento ETL y el almacenamiento en la carpeta data. Su función principal es facilitar la comunicación entre estos componentes, garantizando un flujo de datos estructurado y optimizado para su posterior análisis y transformación."""

#==============================# Importamos las librerias necesarias #==============================#
import pandas as pd
import numpy as np

#==============================#      Fuction main [Interface]       #==============================#
def ExportInterface(Option):
    match Option:
        case "Log-Data": # Carpeta de localizacion de los logs y los datos
            log_folder = "C:\\Users\\spinz\\OneDrive\\Documentos\\Portafolio oficial\\HERMESDB\\HERMESDB\\test\\data\\logs"
            data_folder = "C:\\Users\\spinz\\OneDrive\\Documentos\\Portafolio oficial\\HERMESDB\\HERMESDB\\test\\data\\external"
            return log_folder, data_folder
        case "swap_carpet": #carpeta de salida de los swaps
            output_dir=r"C:\Users\spinz\OneDrive\Documentos\Portafolio oficial\HERMESDB\HERMESDB\test\data\external\datos_swaps"
            return output_dir
