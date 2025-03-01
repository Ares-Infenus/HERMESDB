import MetaTrader5 as mt5
import pandas as pd
import os

class MT5DataExtractor:
    """
    Clase para extraer información de activos de MetaTrader5 y exportarla a CSV.

    Atributos:
        csv_file (str): Ruta del archivo CSV con las credenciales.
        output_file (str): Ruta de salida para guardar la información extraída.
    """

    ISIN_COUNTRY_MAP = {
        "AD": "Andorra", "AE": "Emiratos Árabes Unidos", "AF": "Afganistán",
        "AG": "Antigua y Barbuda", "AI": "Anguila", "AL": "Albania",
        "AM": "Armenia", "AO": "Angola", "AR": "Argentina", "AT": "Austria",
        "AU": "Australia", "AW": "Aruba", "AZ": "Azerbaiyán", "BA": "Bosnia y Herzegovina",
        "BB": "Barbados", "BD": "Bangladés", "BE": "Bélgica", "BF": "Burkina Faso",
        "BG": "Bulgaria", "BH": "Baréin", "BI": "Burundi", "BJ": "Benín",
        "BN": "Brunéi", "BO": "Bolivia", "BR": "Brasil", "BS": "Bahamas",
        "BT": "Bután", "BW": "Botsuana", "BY": "Bielorrusia", "BZ": "Belice",
        "CA": "Canadá", "CH": "Suiza", "CI": "Costa de Marfil", "CL": "Chile",
        "CM": "Camerún", "CN": "China", "CO": "Colombia", "CR": "Costa Rica",
        "CV": "Cabo Verde", "CY": "Chipre", "CZ": "República Checa", "DE": "Alemania",
        "DJ": "Yibuti", "DK": "Dinamarca", "DM": "Dominica", "DO": "República Dominicana",
        "DZ": "Argelia", "EC": "Ecuador", "EE": "Estonia", "EG": "Egipto",
        "ES": "España", "FI": "Finlandia", "FJ": "Fiyi", "FR": "Francia",
        "GB": "Reino Unido", "GE": "Georgia", "GH": "Ghana", "GR": "Grecia",
        "GT": "Guatemala", "HK": "Hong Kong", "HN": "Honduras", "HR": "Croacia",
        "HU": "Hungría", "ID": "Indonesia", "IE": "Irlanda", "IL": "Israel",
        "IN": "India", "IQ": "Irak", "IR": "Irán", "IS": "Islandia",
        "IT": "Italia", "JM": "Jamaica", "JO": "Jordania", "JP": "Japón",
        "KE": "Kenia", "KG": "Kirguistán", "KH": "Camboya", "KR": "Corea del Sur",
        "KW": "Kuwait", "KZ": "Kazajistán", "LA": "Laos", "LB": "Líbano",
        "LI": "Liechtenstein", "LK": "Sri Lanka", "LT": "Lituania", "LU": "Luxemburgo",
        "LV": "Letonia", "LY": "Libia", "MA": "Marruecos", "MC": "Mónaco",
        "MD": "Moldavia", "ME": "Montenegro", "MG": "Madagascar", "MK": "Macedonia del Norte",
        "ML": "Malí", "MM": "Myanmar", "MN": "Mongolia", "MO": "Macao",
        "MT": "Malta", "MU": "Mauricio", "MV": "Maldivas", "MX": "México",
        "MY": "Malasia", "MZ": "Mozambique", "NA": "Namibia", "NE": "Níger",
        "NG": "Nigeria", "NI": "Nicaragua", "NL": "Países Bajos", "NO": "Noruega",
        "NP": "Nepal", "NZ": "Nueva Zelanda", "OM": "Omán", "PA": "Panamá",
        "PE": "Perú", "PH": "Filipinas", "PK": "Pakistán", "PL": "Polonia",
        "PT": "Portugal", "PY": "Paraguay", "QA": "Catar", "RO": "Rumania",
        "RS": "Serbia", "RU": "Rusia", "RW": "Ruanda", "SA": "Arabia Saudita",
        "SC": "Seychelles", "SD": "Sudán", "SE": "Suecia", "SG": "Singapur",
        "SI": "Eslovenia", "SK": "Eslovaquia", "SN": "Senegal", "SV": "El Salvador",
        "SY": "Siria", "TH": "Tailandia", "TN": "Túnez", "TR": "Turquía",
        "TT": "Trinidad y Tobago", "TW": "Taiwán", "TZ": "Tanzania", "UA": "Ucrania",
        "UG": "Uganda", "US": "Estados Unidos", "UY": "Uruguay", "UZ": "Uzbekistán",
        "VE": "Venezuela", "VN": "Vietnam", "YE": "Yemen", "ZA": "Sudáfrica",
        "ZM": "Zambia", "ZW": "Zimbabue", "JE":"Swiss", "BM": "Bermudas","KY": "Islas Caiman","LR":"Liberia","MH":"Islas Marshall", "AN": "Antillas Neerlandesas","GG":"Guernsey"
    }

    def __init__(self, csv_file: str, output_file: str):
        """
        Inicializa la clase con las rutas de entrada y salida.

        Args:
            csv_file (str): Ruta del archivo CSV con credenciales.
            output_file (str): Ruta donde se guardará el archivo CSV final.
        """
        self.csv_file = csv_file
        self.output_file = output_file

    def get_country_from_isin(self, isin: str) -> str:
        """
        Extrae el país a partir del ISIN utilizando los dos primeros caracteres.

        Args:
            isin (str): Código ISIN.

        Returns:
            str: Nombre del país o "Desconocido" si no se encuentra.
        """
        if isinstance(isin, str):
            country_code = isin[:2]
            return self.ISIN_COUNTRY_MAP.get(country_code, "Desconocido")
        return "N/A"

    def load_credentials(self) -> list:
        """
        Carga las credenciales desde el archivo CSV.

        Returns:
            list: Lista de diccionarios con las credenciales de cada broker.
        """
        df = pd.read_csv(self.csv_file, index_col=0)
        credentials = []

        for broker in df.columns:
            credentials.append({
                "Broker": broker,
                "User": df.loc["user", broker],
                "Password": df.loc["password", broker],
                "Investor": df.loc["Investor", broker] if "Investor" in df.index else None,
                "Server": df.loc["Server", broker]
            })
        return credentials

    def connect_mt5(self, user: str, password: str, server: str) -> bool:
        """
        Inicializa la conexión a MetaTrader5 con las credenciales proporcionadas.

        Args:
            user (str): Usuario de la cuenta.
            password (str): Contraseña de la cuenta.
            server (str): Servidor al que conectarse.

        Returns:
            bool: True si la conexión fue exitosa; de lo contrario, False.
        """
        mt5.initialize()
        if not mt5.login(int(user), password, server):
            print(f"❌ Error al conectar con {server}: {mt5.last_error()}")
            return False
        print(f"✅ Conectado a {server}")
        return True

    def get_symbols_info(self, broker: str) -> list:
        """
        Extrae la información de los activos del broker actual.

        Args:
            broker (str): Nombre del broker.

        Returns:
            list: Lista de diccionarios con la información de cada activo.
        """
        symbols = mt5.symbols_get()
        data = []

        for symbol in symbols:
            info = mt5.symbol_info(symbol.name)
            if info:
                isin = info.isin if info.isin else "N/A"
                data.append({
                    "Broker": broker,
                    "Symbol": info.name,
                    "Description": info.description,
                    "Currency": info.currency_base,
                    "Category": info.path.split("\\")[0] if "\\" in info.path else "Unknown",
                    "Sector/Industry": info.path.split("\\")[1] if "\\" in info.path else "Unknown",
                    "ISIN": isin,
                    "Pais": self.get_country_from_isin(isin)
                })
        return data

    def extract_data(self) -> list:
        """
        Recorre cada broker utilizando las credenciales y extrae su información de activos.

        Returns:
            list: Lista con la información acumulada de todos los brokers.
        """
        credentials = self.load_credentials()
        all_data = []

        for cred in credentials:
            if self.connect_mt5(cred["User"], cred["Password"], cred["Server"]):
                all_data.extend(self.get_symbols_info(cred["Broker"]))
                mt5.shutdown()
        return all_data

    def save_data(self, data: list):
        """
        Guarda la información extraída en un archivo CSV.

        Args:
            data (list): Lista de diccionarios con la información a guardar.
        """
        df = pd.DataFrame(data)
        os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
        df.to_csv(self.output_file, index=False)
        print(f"📂 Archivo guardado en: {self.output_file}")

    def run(self):
        """
        Ejecuta el flujo completo: extrae la información y la guarda en el CSV.
        """
        data = self.extract_data()
        self.save_data(data)


# Ejecución
if __name__ == "__main__":
    CSV_FILE = r"C:\Users\spinz\OneDrive\Documentos\Portafolio oficial\HERMESDB\HERMESDB\test\data\raw\config.csv"
    OUTPUT_FILE = r"C:\Users\spinz\OneDrive\Documentos\Portafolio oficial\HERMESDB\HERMESDB\test\data\external\symbol_info.csv"
    extractor = MT5DataExtractor(CSV_FILE, OUTPUT_FILE)
    extractor.run()
