#==============================#            Description            #==============================#

"""Este modulo sera el orquestador de todo el proceso de Limpieza Transformacion y Validacion."""

#==============================# Importamos los módulos necesarios #==============================#
from modules.Facade.ImportInterface import * #eSTE MODULO GESTIONA LAS DIRECCIONES DE importacin LOS ARCHIVOS DE LOGS Y DATOS
from modules.Facade.ExportInterface import * #eSTE MODULO GESTIONA LAS DIRECCIONES DE exportacion LOS ARCHIVOS DE LOGS Y DATOS

#Módulos de Importación y Exportación entre Componentes
from modules.Processor.DataClear import *  # Modulo de limeza elimna nan inf y datso que interferirian con el analisis.

#==============================#     Fuction main [Interface]      #==============================#
def ProcessorInterface():
    #======================# Limpieza #======================#
    # Define la ruta donde están los datos de entrada (estructura de brokers en subcarpetas)
    input_dir = ImportInterface("Process")
    # Define la ruta de salida donde se guardarán los CSV unificados y limpios
    output_dir = ExportInterface("Process")
    # Crear instancia de la clase
    cleaner = DataCleaner(input_dir, output_dir)
    # Procesar todos los archivos CSV y generar archivos limpios por broker
    cleaner.process()
    print("Limpieza completada.")
    


if __name__ == "__main__":
    ProcessorInterface()