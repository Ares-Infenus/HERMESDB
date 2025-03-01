import pandas as pd
import re
from typing import Dict

class ETLProcessor:
    """
    Clase ETLProcessor encargada de realizar procesos de extracción, transformación y carga (ETL)
    para diferentes tablas (sectores, activos). Cada método se encarga de una única responsabilidad,
    siguiendo el principio de responsabilidad única del SOLID.
    """

    def __init__(self) -> None:
        pass

    @staticmethod
    def _read_and_normalize_csv(path: str, usecols: list, normalize_cols: Dict[str, str] = None) -> pd.DataFrame:
        """
        Lee un CSV y normaliza (strip y lower) las columnas especificadas.

        Args:
            path (str): Ruta del archivo CSV.
            usecols (list): Lista de columnas a leer.
            normalize_cols (Dict[str, str], opcional): Diccionario de columnas a normalizar.
                La clave es el nombre de la columna en el DataFrame y el valor es el método de normalización.
                Por defecto normaliza aplicando str.strip() y str.lower().

        Returns:
            pd.DataFrame: DataFrame con los datos leídos y normalizados.
        """
        df = pd.read_csv(path, usecols=usecols)
        if normalize_cols:
            for col in normalize_cols:
                if col in df.columns:
                    df[col] = df[col].astype(str).str.strip().str.lower()
        return df

    def process_table_sector(self, symbol_info_path: str, market_path: str, output_path: str) -> pd.DataFrame:
        """
        Procesa y une la información de sectores con el mapeo de mercados para generar una tabla de sectores.

        Se realizan los siguientes pasos:
          1. Importa y normaliza los CSV de symbol_info y market.
          2. Elimina duplicados.
          3. Une (merge) los datos utilizando 'mercado' y 'nombre' como llave.
          4. Realiza transformaciones (renombrar columnas, eliminar columnas no necesarias y agregar índice).
          5. Exporta el resultado a CSV.

        Args:
            symbol_info_path (str): Ruta del CSV con columnas ['mercado', 'sector'].
            market_path (str): Ruta del CSV con columnas ['nombre', 'mercado_id'].
            output_path (str): Ruta donde se guardará el CSV resultado.

        Returns:
            pd.DataFrame: DataFrame final procesado.
        """
        # Paso 1: Importar y normalizar
        df_symbol = self._read_and_normalize_csv(
            symbol_info_path, 
            ['mercado', 'sector'], 
            normalize_cols={'mercado': 'normalize'}
        )
        df_market = self._read_and_normalize_csv(
            market_path, 
            ['nombre', 'mercado_id'], 
            normalize_cols={'nombre': 'normalize'}
        )

        # Paso 2: Eliminar duplicados
        df_symbol = df_symbol.drop_duplicates(subset=['mercado', 'sector'])
        df_market = df_market.drop_duplicates(subset=['nombre'])

        # Paso 3: Unir datos por llave
        df_merged = pd.merge(df_symbol, df_market, left_on='mercado', right_on='nombre', how='left')

        # Paso 4: Transformaciones: eliminar columnas innecesarias, renombrar y agregar índice
        df_merged.drop(columns=['nombre', 'mercado'], inplace=True)
        df_merged.rename(columns={'sector': 'nombre'}, inplace=True)
        df_merged.insert(0, 'sector_id', range(1, len(df_merged) + 1))
        df_result = df_merged[['sector_id', 'nombre', 'mercado_id']]

        # Paso 5: Exportar resultado
        df_result.to_csv(output_path, index=False)
        return df_result

    def clean_actives(self, actives_path: str) -> pd.DataFrame:
        """
        Limpia el CSV de activos eliminando columnas innecesarias y normalizando la columna 'Symbol'.

        Se eliminan sufijos específicos en la columna 'Symbol' para dejarla en un formato estandarizado.

        Args:
            actives_path (str): Ruta del CSV de entrada con datos de activos.

        Returns:
            pd.DataFrame: DataFrame con datos limpios.
        """
        df = pd.read_csv(actives_path)
        columns_to_drop = ["Broker", "Currency", "Category", "Sector/Industry", "ISIN", "Pais"]
        df.drop(columns=[col for col in columns_to_drop if col in df.columns], inplace=True, errors='ignore')

        if "Symbol" in df.columns:
            df["Symbol"] = df["Symbol"].apply(lambda x: re.sub(r"_CFD.*$", "", x))
            df["Symbol"] = df["Symbol"].str.replace(r"\.(?!IDX$|HK$)[A-Za-z0-9-]+$", "", regex=True)
        return df

    def replace_market_in_actives(self, df_actives: pd.DataFrame, market_path: str) -> pd.DataFrame:
        """
        Reemplaza la columna 'mercado' en el DataFrame de activos con su correspondiente 'mercado_id'
        utilizando el mapeo proporcionado en el CSV de mercados.

        Args:
            df_actives (pd.DataFrame): DataFrame de activos.
            market_path (str): Ruta del CSV que contiene el mapeo de mercados.

        Returns:
            pd.DataFrame: DataFrame con la columna 'mercado' actualizada a 'mercado_id'.
        """
        df_market = pd.read_csv(market_path)
        mapping = df_market.set_index('nombre')['mercado_id'].to_dict()
        df_actives['mercado'] = df_actives['mercado'].map(mapping)
        return df_actives

    def replace_sector_in_actives(self, df_actives: pd.DataFrame, sector_mapping_path: str) -> pd.DataFrame:
        """
        Reemplaza la columna 'sector' en el DataFrame de activos por el 'sector_id'
        utilizando el mapeo de sectores del CSV.

        Args:
            df_actives (pd.DataFrame): DataFrame de activos con la columna 'sector'.
            sector_mapping_path (str): Ruta del CSV de mapeo de sectores (con columnas ['sector_id', 'nombre', 'mercado_id']).

        Returns:
            pd.DataFrame: DataFrame con la columna 'sector' reemplazada por 'sector_id'.
        """
        df_sector = pd.read_csv(sector_mapping_path)
        df_actives['sector'] = df_actives['sector'].astype(str).str.strip().str.lower()
        df_sector['nombre'] = df_sector['nombre'].astype(str).str.strip().str.lower()
        mapping_sector = df_sector.set_index('nombre')['sector_id'].to_dict()
        df_actives['sector'] = df_actives['sector'].map(mapping_sector)
        return df_actives

    def fill_missing_nombre(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Reemplaza los valores NaN en la columna 'nombre' por "Desconocido".

        Args:
            df (pd.DataFrame): DataFrame que contiene la columna 'nombre'.

        Returns:
            pd.DataFrame: DataFrame con los NaN reemplazados por "Desconocido".
        """
        df['nombre'] = df['nombre'].fillna("Desconocido")
        return df

    def process_actives(self, actives_input_path: str, market_path: str, sector_mapping_path: str, assets_output_path: str) -> pd.DataFrame:
        """
        Ejecuta el proceso completo de transformación y exportación de la tabla de activos.

        El proceso incluye:
        1. Limpieza inicial del CSV de activos.
        2. Reemplazo de 'mercado' por 'mercado_id' usando el CSV de mercados.
        3. Reemplazo de 'sector' por 'sector_id' usando el mapeo de sectores.
        4. Eliminación de duplicados en la columna 'Symbol'.
        5. Modificaciones finales:
            - Agregar índice incremental 'activo_id'.
            - Renombrar columnas para mantener consistencia:
                'Symbol'   -> 'simbolo'
                'mercado'  -> 'mercado_id'
                'sector'   -> 'sector_id'
                'Description' -> 'nombre'
            - Agregar una columna 'contrato_size' con valor 0.
        6. Reemplazar valores NaN en la columna 'nombre' por "Desconocido".
        7. Eliminar comillas dobles de la columna 'nombre'.
        8. Exportación del CSV final de activos.

        Args:
            actives_input_path (str): Ruta del CSV de entrada con datos de activos.
            market_path (str): Ruta del CSV con mapeo de mercados.
            sector_mapping_path (str): Ruta del CSV con mapeo de sectores.
            assets_output_path (str): Ruta donde se exportará el CSV final de activos.

        Returns:
            pd.DataFrame: DataFrame final procesado.
        """
        # 1. Limpieza inicial
        df_actives = self.clean_actives(actives_input_path)

        # 2. Reemplazo de 'mercado' por 'mercado_id'
        df_actives = self.replace_market_in_actives(df_actives, market_path)

        # 3. Reemplazo de 'sector' por 'sector_id'
        df_actives = self.replace_sector_in_actives(df_actives, sector_mapping_path)

        # 4. Eliminar duplicados basados en 'Symbol'
        df_actives.drop_duplicates(subset=['Symbol'], inplace=True)

        # 5. Modificaciones finales:
        df_actives.insert(0, 'activo_id', range(1, len(df_actives) + 1))
        df_actives.rename(columns={
            "Symbol": "simbolo",
            "mercado": "mercado_id",
            "sector": "sector_id",
            "Description": "nombre"
        }, inplace=True)
        df_actives['contrato_size'] = 0

        # 6. Reemplazar valores NaN en la columna 'nombre'
        df_actives = self.fill_missing_nombre(df_actives)

        # 7. Eliminar comillas dobles de la columna 'nombre'
        df_actives['nombre'] = df_actives['nombre'].str.replace(r'[“”"]', '', regex=True)

        # 8. Exportar CSV final
        df_actives.to_csv(assets_output_path, index=False, encoding='utf-8')
        return df_actives


if __name__ == "__main__":
    etl = ETLProcessor()

    # Rutas de ejemplo (ajustar según el entorno)
    symbol_info_path   = r"C:\Users\spinz\OneDrive\Documentos\Portafolio oficial\HERMESDB\HERMESDB\test\data\processed\symbol_info_procesado.csv"
    market_path        = r"C:\Users\spinz\OneDrive\Documentos\Portafolio oficial\HERMESDB\HERMESDB\test\data\backup\Table_market.csv"
    sector_output_path = r"C:\Users\spinz\OneDrive\Documentos\Portafolio oficial\HERMESDB\HERMESDB\test\data\backup\Table_Sector.csv"
    actives_input_path = symbol_info_path  # Se puede ajustar si es diferente
    assets_output_path = r"C:\Users\spinz\OneDrive\Documentos\Portafolio oficial\HERMESDB\HERMESDB\test\data\backup\Table_Assets.csv"

    # Procesar la tabla de sectores
    df_sector = etl.process_table_sector(symbol_info_path, market_path, sector_output_path)
    print("Tabla Sector procesada:")
    print(df_sector.head())

    # Procesar la tabla de activos en una sola llamada
    df_actives_final = etl.process_actives(actives_input_path, market_path, sector_output_path, assets_output_path)
    print("\nTabla Assets final procesada:")
    print(df_actives_final.head())
