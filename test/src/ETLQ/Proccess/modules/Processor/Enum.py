import pandas as pd
# TODO: Agregar manejo de errores
# QUESTION: ¿Por qué no se usa la librería logging para manejar errores?
# Mapeos y definiciones constantes
MARKET_MAP = {
    "forex": "Forex", "reval": "Forex",
    "stocks": "Stocks", "sharecfds": "Stocks",
    "commodities": "Commodities", "bullion": "Commodities", "cfd-crude-oil": "Commodities",
    "indices": "Indices", "cfd": "Indices", "cfd-2": "Indices", "cfd-jp225": "Indices",
    "etfs": "ETFs",
    "crypto": "Crypto", "cryptos": "Crypto", "cryptocurrencies": "Crypto",
    "treasuries": "Derivatives", "forwards": "Derivatives"
}

CURRENCY_PAIRS = {
    "Majors": ["EURUSD", "USDJPY", "GBPUSD", "USDCHF", "USDCAD", "AUDUSD", "NZDUSD"],
    "Minors": ["EURGBP", "EURJPY", "GBPJPY", "EURAUD", "EURCHF", "EURCAD", "EURNZD",
               "GBPCHF", "GBPAUD", "GBPCAD", "GBPNZD", "AUDJPY", "AUDCHF", "AUDCAD",
               "NZDJPY", "NZDCHF", "CADJPY", "CADCHF"],
    "Exotics": ["USDTRY", "EURTRY", "USDZAR", "USDSGD", "USDHKD", "USDSEK", "USDNOK", "USDDKK",
                "USDPLN", "USDHUF", "USDCZK", "USDMXN", "USDTHB", "USDIDR", "USDINR", "USDPHP",
                "USDMYR", "USDKRW", "USDCNY", "EURPLN", "EURRUB", "EURHUF", "EURCZK", "EURMXN",
                "EURZAR", "EURSGD", "GBPTRY", "GBPZAR", "AUDSGD", "AUDTRY", "NZDSGD", "CADSGD",
                "SGDJPY", "USDCNH", "ZARJPY", "TRYJPY", "EURDKK", "GBPGBX", "GBXUSD", "AUDUSD.sml",
                "EURGBP.sml", "EURUSD.sml", "GBPJPY.sml", "GBPUSD.sml", "USDJPY.sml", "AUDNZD",
                "CHFJPY", "EURHKD", "EURNOK", "EURSEK", "GBPCZK", "GBPDKK", "GBPHKD", "GBPHUF",
                "GBPNOK", "GBPPLN", "GBPSEK", "NZDCAD", "CHFSGD", "GBPMXN", "GBPSGD", "NOKJPY",
                "NOKSEK", "SEKJPY", "USDBRL", "USDCLP", "USDCOP", "USDTWD", "AUDDKK", "AUDHUF",
                "AUDNOK", "AUDPLN", "CADMXN", "CHFDKK", "CHFHUF", "CHFNOK", "USDSAR", "USDAED",
                "USDRON", "USDILS", "HKDJPY", "CADHKD", "USDRON", "USDILS", "EURILS", "PLNJPY",
                "NZDHUF", "NZDCNH", "MXNJPY", "GBPCNH", "EURCNH", "CHFSEK", "CHFPLN"]
}

COMMODITIES = {
    "Metales Preciosos (Bullion)": ["XAUCNH", "XAGSGD", "XPD.CMD", "XAUUSD", "XAGUSD", "XPTUSD", "XPDUSD",
                                    "XAUGBP", "XAUCHF", "XAUJPY", "XPT.CMD", "XAUSGD", "XAUTHB",
                                    "XAGEUR", "XAGAUD", "XAUAUD", "XAUEUR", "XAUUSD.sml"],
    "Metales Industriales": ["COPPER", "Aluminum/USD", "Nickel/USD", "Zinc/USD", "Copper",
                              "COPPER.CMD", "Lead", "Zinc", "Nickel", "Aluminium"],
    "Energía": ["USOIL.sml", "UKOIL.sml", "NATGAS", "SpotBrent", "SpotCrude", "NatGas",
                "BRENT.CMD", "DIESEL.CMD", "GAS.CMD", "LIGHT.CMD", "SoyOil", "Gasoline",
                "XTIUSD", "XNGUSD", "BRENT"],
    "Agrícolas": ["OJUICE.CMD", "SUGAR", "WHEAT", "SOYBN", "Cotton", "Soybeans", "Wheat", "OJ",
                  "LDSugar", "COCOA.CMD", "COFFEE.CMD", "SOYBEAN.CMD", "SUGAR.CMD", "RghRice",
                  "SoyMeal", "Oats", "Lumber", "Corn", "LeanHogs", "Cattle", "Sugar", "Coffee",
                  "Cocoa", "COTTON.CMD"],
    "Ganadería": ["Live Cattle", "Lean Hogs"]
}

INDICES = {
    "Europa": ["EU50", "FRA40", "NL25", "UK100", "CH20", "ES35", "DE40", "FR40",
               "FRANCE40", "ITALY40", "SPAIN35", "SWISS20", "WIG20", "DXY", "EUSTX50",
               "SPA35", "EURX", "NETH25", "NOR25", "SWI20", "GERTEC30", "MidDE50",
               "GER40", "FCHI40", "STOXX50E", "GDAXI", "SPA35", "ESP.IDX", "EUS.IDX",
               "FRA.IDX", "GBR.IDX", "NLD.IDX", "PLN.IDX"],
    "Asia": ["HK50", "JP225", "CHINAH", "CN50", "SG30", "CHINA50", "SG20", "JPN225",
             "SCI25", "JPYX", "CHINAH", "NI225"],
    "América": ["US100", "US2000", "US30", "US500", "STOXX50", "USTEC", "VIX",
                "NAS100", "USDX", "CA60", "NDX", "WS30", "SP500", "DOLLAR.IDX",
                "HKG.IDX", "JPN.IDX", "USA30.IDX", "USA500.IDX", "USATECH.IDX",
                "USSC2000.IDX", "VOL.IDX"],
    "Pacifico": ["AU200", "AUS200", "AUS.IDX", "CHE.IDX", "CHI.IDX", "DEU.IDX"],
    "Africa": ["ZAR40", "AFRICA40", "SA40", "SOA.IDX"]
}

# Tuplas para actualizaciones manuales: (Symbol, nuevo_mercado, nuevo_sector)
MANUAL_UPDATES = [
    ("PRNT.US", "ETFs", "Estados unidos"),
    ("XAGUSD", "Commodities", "Metales Preciosos (Bullion)"),
    ("XAUUSD", "Commodities", "Metales Preciosos (Bullion)"),
    ("NAT.GAS", "Commodities", "Energía"),
    ("USDBRL", "Forex", "Exotics"),
    ("ALUMINIUM", "Commodities", "Metales Industriales"),
    ("NICKEL", "Commodities", "Metales Industriales"),
    ("ZINC", "Commodities", "Metales Industriales"),
    ("XAUEUR", "Commodities", "Metales Preciosos (Bullion)"),
    ("COCOA", "Commodities", "Agrícolas"),
    ("COFFEE", "Commodities", "Agrícolas"),
    ("WHEAT", "Commodities", "Agrícolas"),
    ("PALLADIUM", "Commodities", "Metales Preciosos (Bullion)"),
    ("PLATINUM", "Commodities", "Metales Preciosos (Bullion)"),
    ("ARM", "Stocks", "Reino Unido"),
    ("ASTH", "Stocks", "Estados Unidos"),
    ("GEV", "Stocks", "Estados Unidos"),
    ("ARKG.US", "ETFs", "Estados Unidos"),
    ("SMIN.US", "ETFs", "India"),
    ("IGE.US", "ETFs", "Estados Unidos"),
    ("SVXY.US", "ETFs", "Estados Unidos"),
    ("VIXY.US", "ETFs", "Estados Unidos"),
    ("VNM.US", "ETFs", "Vietnam"),
    ("ECH.US", "ETFs", "Chile"),
    ("BUND.TR", "Government bonds", "Alemania"),
    ("UKGILT.TR", "Government bonds", "Reino Unido"),
    ("USTBOND.TR", "Government bonds", "Estados Unidos"),
    ("BIRK.US-24", "Stocks", "Estados Unidos"),
    ("ARKX.US", "ETFs", "Estados Unidos"),
    ("LEAD", "ETFs", "Estados Unidos"),
    ("SGD.IDX","Indices","Singapur")
]

def asignar_valor_mercado(row):
    cat = str(row.get("Category", "")).strip().lower()
    sec = str(row.get("Sector/Industry", "")).strip().lower()
    symbol = str(row.get("Symbol", "")).strip().lower()
    if cat == "cfd" and symbol in {"sugar", "copper"}:
        return "Commodities"
    return next((mercado for clave, mercado in MARKET_MAP.items() if cat == clave or sec == clave), "")

def asignar_sector(row):
    symbol = str(row.get("Symbol", "")).strip().lower()
    mercado = row.get("mercado", "")
    pais = row.get("Pais", "")
    if mercado == "Forex":
        for sector, symbols in CURRENCY_PAIRS.items():
            if symbol in {s.lower() for s in symbols}:
                return sector
    elif mercado == "Commodities":
        for sector, symbols in COMMODITIES.items():
            if symbol in {s.lower() for s in symbols}:
                return sector
    elif mercado == "Indices":
        for region, symbols in INDICES.items():
            if row.get("Symbol") in symbols:
                return region
    elif mercado == "Crypto":
        return "Crypto"
    elif pais and pais != "Desconocido":
        return pais
    elif mercado == "Derivatives":
        if row.get("Category") == "Treasuries":
            return "Futuros sobre bonos"
        if row.get("Sector/Industry") == "Forwards":
            return "Forwards"
    return ""

class ProcesadorMercado:
    def __init__(self, ruta_csv):
        self.ruta_csv = ruta_csv
        self.df = None

    def cargar_csv(self):
        try:
            self.df = pd.read_csv(self.ruta_csv)
        except Exception as e:
            raise RuntimeError(f"Error al cargar el CSV: {e}")

    def agregar_columnas(self):
        # Validación mínima de columnas para calcular "mercado"
        if not {"Category", "Sector/Industry"}.issubset(self.df.columns):
            raise ValueError("El CSV debe tener las columnas 'Category' y 'Sector/Industry'")
        self.df["mercado"] = self.df.apply(asignar_valor_mercado, axis=1)
        # Validación mínima para calcular "sector"
        if not {"Symbol", "Pais"}.issubset(self.df.columns):
            raise ValueError("El CSV debe tener las columnas 'Symbol' y 'Pais'")
        self.df["sector"] = self.df.apply(asignar_sector, axis=1)

    def actualizar_valores_manual(self):
        for symbol, nuevo_mercado, nuevo_sector in MANUAL_UPDATES:
            mask = self.df["Symbol"] == symbol
            self.df.loc[mask, "mercado"] = nuevo_mercado
            self.df.loc[mask, "sector"] = nuevo_sector

    def exportar_csv(self, ruta_export):
        try:
            self.df.to_csv(ruta_export, index=False)
            print(f"✅ CSV exportado a: {ruta_export}")
        except Exception as e:
            raise RuntimeError(f"❌ Error al exportar el CSV: {e}")

    def procesar(self, ruta_export=None):
        self.cargar_csv()
        self.agregar_columnas()
        self.actualizar_valores_manual()
        if ruta_export:
            self.exportar_csv(ruta_export)
        return self.df

# ============================
# Ejecución
# ============================
if __name__ == "__main__":
    ruta_entrada = r"C:\Users\spinz\OneDrive\Documentos\Portafolio oficial\HERMESDB\HERMESDB\test\data\external\symbol_info.csv"
    ruta_salida = r"C:\Users\spinz\OneDrive\Documentos\Portafolio oficial\HERMESDB\HERMESDB\test\data\processed\symbol_info_procesado.csv"
    
    procesador = ProcesadorMercado(ruta_entrada)
    df_resultado = procesador.procesar(ruta_export=ruta_salida)
    print(df_resultado.head())