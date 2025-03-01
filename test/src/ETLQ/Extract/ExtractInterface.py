#==============================#            Description            #==============================#

"""este modulo para gestionar los componentes que realizan la extraccion de datos de Metatrader 5, este modulo es la interfaz y es el orquestador o controller de todo el procedimiento de extraccion este componentemanejara el orden de ejecucion y hace las llamadas para hacer las tareas."""

#==============================# Importamos los módulos necesarios #==============================#

#Módulos de Importación y Exportación entre Componentes
from modules.Facade.ImportInterface import ImportInterface # Modulo de importacion de datos
from modules.Facade.ExportInterface import ExportInterface # Modulo de exportacion de datos

#Modulo de extraccion de datos_historicos de diferentes brokers 
from modules.Extraction.MT5Extract import *
from modules.Extraction.ExtractCostOperative import *
from modules.Extraction.Clasification import *
#==============================#      Fuction main [Interface]       #==============================#
def ExtractInterface():
    
    #================# Extraction of data from Metatrader 5 #================#
    credentials = ImportInterface("ExtractImportCredentials")       
    # Modulo observer de MT5_extract
    
    # Definimos las carpetas para log y datos
    credentials_df = pd.DataFrame(credentials)

    # Importacion de als direcciones para el log y los datos
    log_folder, data_folder = ExportInterface("Log-Data")

    # Especificamos el rango de fechas (por ejemplo, desde el 1 de enero de 2000 hasta hoy)
    start_date = datetime(2000, 1, 1)
    end_date = datetime.now()

    #downloader = HistoricalDataDownloader(credentials_df, log_folder, data_folder, start_date, end_date) #Esta linea ejecuta la descarga de datos
    #================# Extraction of Cost Operative from Metatrader 5 #================#
    df_cred = pd.DataFrame(credentials_df)

    # Especificar la carpeta de destino, por ejemplo: "datos_swaps"
    extractor = SwapExtractor(df_cred, output_dir=ExportInterface("swap_carpet"))
    #extractor.run() #Esta linea ejecuta la extraccion de los costos operativos swap ademas
    #================# Extraction OF INFOR OF aCTIVE from Metatrader 5 #================#
    CSV_FILE = r"C:\Users\spinz\OneDrive\Documentos\Portafolio oficial\HERMESDB\HERMESDB\test\data\raw\config.csv"
    OUTPUT_FILE = r"C:\Users\spinz\OneDrive\Documentos\Portafolio oficial\HERMESDB\HERMESDB\test\data\external\symbol_info.csv"
    extractor = MT5DataExtractor(CSV_FILE, OUTPUT_FILE)
    extractor.run()
if __name__ == '__main__':
    ExtractInterface()