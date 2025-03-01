#==============================#            Description            #==============================#

"""Este módulo se encarga de gestionar y administrar la exportacion de datos, asegurando una integración eficiente entre los módulos de procesamiento y el almacenamiento en la carpeta data. Su función principal es facilitar la comunicación entre estos componentes, garantizando un flujo de datos estructurado y optimizado para su posterior análisis y transformación."""

#==============================# Importamos las librerias necesarias #==============================#
import pandas as pd
import numpy as np

#==============================#      Fuction main [Interface]       #==============================#
def ExportInterface(Option):
    match Option:
        case "Process": # Carpeta de localizacion de los logs y los datos
            output_dir = "C:\\Users\\spinz\\OneDrive\\Documentos\\Portafolio oficial\\HERMESDB\\HERMESDB\\test\\data\\processed"
            return output_dir 

