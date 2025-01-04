import MetaTrader5 as mt5
import pandas as pd

class MT5AssetExtractor:
    def __init__(self, output_path):
        """
        Inicializa la clase con la ruta de salida para el archivo CSV.

        :param output_path: Ruta donde se guardará el archivo CSV final.
        """
        self.output_path = output_path

    def extract_assets(self):
        """
        Extrae los activos disponibles en MetaTrader 5 junto con sus sectores
        y los guarda en un archivo CSV en la ruta especificada.
        """
        # Inicializar MetaTrader 5
        if not mt5.initialize():
            print("Error al inicializar MetaTrader 5")
            return

        try:
            # Obtener todos los símbolos disponibles
            symbols = mt5.symbols_get()
            data = []

            for symbol in symbols:
                # Obtener información detallada del símbolo
                symbol_info = mt5.symbol_info(symbol.name)
                if symbol_info is not None:
                    # Extraer nombre del activo y el sector (carpeta principal)
                    name = symbol_info.name
                    sector = symbol_info.path.split('\\')[0]  # `path` contiene el grupo del activo
                    data.append({"Activo": name, "Sector": sector})

            # Crear un DataFrame con los datos
            df = pd.DataFrame(data)

            # Guardar el DataFrame en un archivo CSV
            df.to_csv(self.output_path, index=False)
            print(f"Archivo creado con éxito en: {self.output_path}")

        except Exception as e:
            print(f"Ocurrió un error: {e}")

        finally:
            # Desconectar MetaTrader 5
            mt5.shutdown()