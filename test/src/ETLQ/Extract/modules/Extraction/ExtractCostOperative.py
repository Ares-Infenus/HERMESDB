import os
import pandas as pd
from datetime import datetime
import MetaTrader5 as mt5

class SwapExtractor:
    def __init__(self, credentials_df: pd.DataFrame, output_dir: str = "output"):
        self.credentials = self._procesar_credenciales(credentials_df)
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

    def _procesar_credenciales(self, df: pd.DataFrame) -> dict:
        credenciales = {}
        df2 = df.set_index("Tipo")
        for broker in df2.columns:
            credenciales[broker] = {
                "user": df2.loc["user", broker],
                "password": df2.loc["password", broker],
                "investor": df2.loc["Investor", broker],
                "server": df2.loc["Server", broker]
            }
        return credenciales

    def _conectar_broker(self, broker: str, cred: dict) -> bool:
        try:
            login = int(cred["user"])
        except Exception as e:
            print(f"[{broker}] Error al convertir el login: {e}")
            return False
        
        if not mt5.initialize(login=login, server=cred["server"], password=cred["password"]):
            print(f"[{broker}] Error al inicializar: {mt5.last_error()}")
            return False
        return True

    def _extraer_swaps_broker(self, broker: str, cred: dict) -> pd.DataFrame:
        if not self._conectar_broker(broker, cred):
            return None

        print(f"[{broker}] Conexión exitosa. Extrayendo símbolos...")
        symbols = mt5.symbols_get()
        data = {}
        if symbols is None:
            print(f"[{broker}] No se pudo obtener la lista de símbolos: {mt5.last_error()}")
        else:
            for symbol in symbols:
                info = mt5.symbol_info(symbol.name)
                if info is not None:
                    data[symbol.name] = {
                        "swap_long": info.swap_long,
                        "swap_short": info.swap_short
                    }
        mt5.shutdown()
        print(f"[{broker}] Conexión cerrada.")

        df_swaps = pd.DataFrame.from_dict(data, orient="index")
        df_swaps.index.name = "Símbolo"
        return df_swaps

    def _actualizar_csv_por_activo(self, broker: str, df_swaps: pd.DataFrame):
        broker_dir = os.path.join(self.output_dir, broker)
        os.makedirs(broker_dir, exist_ok=True)
        hoy = datetime.today().strftime("%Y-%m-%d")

        for simbolo, row in df_swaps.iterrows():
            file_path = os.path.join(broker_dir, f"{simbolo}.csv")
            if os.path.exists(file_path):
                df_existente = pd.read_csv(file_path, index_col=0, parse_dates=True)
            else:
                df_existente = pd.DataFrame(columns=["swap_long", "swap_short"])
            
            df_existente.index.name = "date"
            df_existente.loc[hoy] = {"swap_long": row["swap_long"], "swap_short": row["swap_short"]}
            
            # Conversión segura del índice a datetime antes de ordenar
            df_existente.index = pd.to_datetime(df_existente.index, errors='coerce')
            df_existente.sort_index(inplace=True)
            df_existente.to_csv(file_path)
            print(f"[{broker}] Archivo actualizado para {simbolo}: {file_path}")

    def run(self):
        for broker, cred in self.credentials.items():
            if pd.isna(cred["user"]) or pd.isna(cred["password"]) or pd.isna(cred["server"]):
                print(f"[{broker}] Credenciales incompletas. Se omite este broker.")
                continue

            print(f"\nProcesando broker: {broker}")
            df_swaps = self._extraer_swaps_broker(broker, cred)
            if df_swaps is not None:
                self._actualizar_csv_por_activo(broker, df_swaps)
            else:
                print(f"[{broker}] No se pudo extraer la información de swaps.")

# Ejemplo de uso
#if __name__ == "__main__":
#    # Ejemplo de DataFrame con la estructura de credenciales:
#    data = {
#        "Tipo": ["user", "password", "Investor", "Server"],
#        "Oanda": [6364744, "WgRp-0Ny", "!uGpQi2p", "OANDA-Demo-1"],
#        "Tickmill": [25177464, "v7-!sL75Bo?m", ">foE1u?W]BP>", "demo.mt5tickmill.com"],
# #       "Pepperstone": [61324462, "efxbJ3m@lg", None, "mt5-demo01.pepperstone.com"],
#        "Darwinex": [3000076243, "@8CTdW@HRb", "3R8S387p2v@D", "demoUK-mt5.darwinex.com"],
#        "Dukascopy": [561753946, "=UcA9-:c", float("nan"), "Dukascopy-demo-mt5-1"]
#    }
#    df_cred = pd.DataFrame(data)
#    # Especificar la carpeta de destino, por ejemplo: "datos_swaps"
#    extractor = SwapExtractor(df_cred, output_dir=r"C:\Users\spinz\OneDrive\Documentos\Portafolio oficial\HERMESDB\HERMESDB\test\data\external\datos_sw")
#    extractor.run()
