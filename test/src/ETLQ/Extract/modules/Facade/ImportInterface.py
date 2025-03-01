#==============================#            Description            #==============================#

"""Este módulo se encarga de gestionar y administrar la importación de datos, asegurando una integración eficiente entre los módulos de procesamiento ETL y el almacenamiento en la carpeta data. Su función principal es facilitar la comunicación entre estos componentes, garantizando un flujo de datos estructurado y optimizado para su posterior análisis y transformación."""

#==============================# Importamos las librerias necesarias #==============================#
import pandas as pd
import numpy as np

#==============================#      Fuction main [Interface]       #==============================#
def ImportInterface(Option):
    match Option:
        case "ExtractImportCredentials": #Importacion de las credenciales para ingresar a los diferentes brokers
            credentials = Extract() 
            return credentials
#==============================#           modules funtion           #==============================
def Extract(): 
    df = pd.read_csv(r"C:\Users\spinz\OneDrive\Documentos\Portafolio oficial\HERMESDB\HERMESDB\test\data\raw\config.csv")
    return df
