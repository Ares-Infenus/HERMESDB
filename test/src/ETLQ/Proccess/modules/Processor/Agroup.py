import pandas as pd
import re

class ETLProcessor:
    def __init__(self):
        pass

    def process_table_sector(self, ruta_symbol_info, ruta_market, ruta_salida):
        """
        Procesa la tabla de sectores:
          1. Importa y normaliza los CSV.
          2. Elimina duplicados.
          3. Realiza el merge entre los datos.
          4. Realiza transformaciones (renombrar, eliminar columnas y agregar índice).
          5. Exporta el resultado a CSV.
          
        Parámetros:
            ruta_symbol_info (str): Ruta del CSV con columnas ['mercado', 'sector'].
            ruta_market (str): Ruta del CSV con columnas ['nombre', 'mercado_id'].
            ruta_salida (str): Ruta donde se guardará el CSV resultado.
        """
        columnas_df1 = ['mercado', 'sector']
        columnas_df2 = ['nombre', 'mercado_id']
        
        # Importación y normalización
        df1 = pd.read_csv(ruta_symbol_info, usecols=columnas_df1)
        df2 = pd.read_csv(ruta_market, usecols=columnas_df2)
        df1['mercado'] = df1['mercado'].str.strip().str.lower()
        df2['nombre']  = df2['nombre'].str.strip().str.lower()
        
        # Eliminar duplicados
        df1_unicos = df1.drop_duplicates(subset=['mercado', 'sector'])
        df2_unicos = df2.drop_duplicates(subset=['nombre'])
        
        # Merge y transformación
        resultado = pd.merge(df1_unicos, df2_unicos, left_on='mercado', right_on='nombre', how='left')
        resultado.drop(columns=['nombre', 'mercado'], inplace=True)
        resultado = resultado.rename(columns={'sector': 'nombre'})
        resultado.insert(0, 'sector_id', range(1, len(resultado) + 1))
        resultado = resultado[['sector_id', 'nombre', 'mercado_id']]
        
        # Exportar el DataFrame de sectores
        resultado.to_csv(ruta_salida, index=False)
        return resultado

    def clean_actives(self, ruta_csv):
        """
        Importa un CSV, elimina columnas innecesarias y normaliza la columna 'Symbol'.
        Nota: Ya no se exporta el DataFrame intermedio (Table_Actives.csv).
        
        Parámetros:
            ruta_csv (str): Ruta del CSV de entrada.
            
        Retorna:
            pd.DataFrame: DataFrame procesado.
        """
        df = pd.read_csv(ruta_csv)
        columnas_a_eliminar = ["Broker", "Currency", "Category", "Sector/Industry", "ISIN", "Pais"]
        df = df.drop(columns=[col for col in columnas_a_eliminar if col in df.columns], errors='ignore')
        
        if "Symbol" in df.columns:
            # Eliminación de sufijos
            df["Symbol"] = df["Symbol"].apply(lambda x: re.sub(r"_CFD.*$", "", x))
            df["Symbol"] = df["Symbol"].str.replace(r"\.(?!IDX$|HK$)[A-Za-z0-9-]+$", "", regex=True)
        
        return df

    def replace_market_in_actives(self, df_activos, ruta_market):
        """
        Reemplaza en el DataFrame de activos la columna 'mercado' por su correspondiente 'mercado_id'
        usando como mapeo el CSV de mercados.
        
        Parámetros:
            df_activos (pd.DataFrame): DataFrame de activos.
            ruta_market (str): Ruta del CSV de mercados.
            
        Retorna:
            pd.DataFrame: DataFrame actualizado.
        """
        df_market = pd.read_csv(ruta_market)
        mapeo = df_market.set_index('nombre')['mercado_id'].to_dict()
        df_activos['mercado'] = df_activos['mercado'].map(mapeo)
        return df_activos

    def replace_sector_in_actives(self, df_activos, ruta_sector_mapping):
        """
        Reemplaza en el DataFrame de activos la columna 'sector' por su correspondiente 'sector_id'
        usando como mapeo el CSV de mapeo de sectores.
        
        Parámetros:
            df_activos (pd.DataFrame): DataFrame de activos (con columnas como 'Symbol', 'Description', 'mercado', 'sector').
            ruta_sector_mapping (str): Ruta del CSV de mapeo de sectores (con columnas ['sector_id', 'nombre', 'mercado_id']).
            
        Retorna:
            pd.DataFrame: DataFrame actualizado.
        """
        df_sector = pd.read_csv(ruta_sector_mapping)

        # Normalizar para evitar problemas de mayúsculas o espacios
        df_activos['sector'] = df_activos['sector'].str.strip().str.lower()
        df_sector['nombre'] = df_sector['nombre'].str.strip().str.lower()

        mapeo_sector = df_sector.set_index('nombre')['sector_id'].to_dict()
        df_activos['sector'] = df_activos['sector'].map(mapeo_sector)
        
        return df_activos


if __name__ == "__main__":
    etl = ETLProcessor()
    
    # Rutas de ejemplo (ajusta las rutas según tu entorno)
    ruta_symbol_info    = r"C:\Users\spinz\OneDrive\Documentos\Portafolio oficial\HERMESDB\HERMESDB\test\data\processed\symbol_info_procesado.csv"
    ruta_market         = r"C:\Users\spinz\OneDrive\Documentos\Portafolio oficial\HERMESDB\HERMESDB\test\data\backup\Table_market.csv"
    ruta_sector_salida  = r"C:\Users\spinz\OneDrive\Documentos\Portafolio oficial\HERMESDB\HERMESDB\test\data\backup\Table_Sector.csv"
    
    # Procesar la tabla Sector (se genera y exporta Table_Sector.csv)
    df_sector = etl.process_table_sector(ruta_symbol_info, ruta_market, ruta_sector_salida)
    print("Tabla Sector procesada:")
    print(df_sector.head())
    
    # Procesar la tabla Actives: limpiar (ya no se exporta Table_Actives.csv)
    ruta_actives_entrada = ruta_symbol_info  # Ajusta la ruta de entrada según corresponda
    df_actives = etl.clean_actives(ruta_actives_entrada)
    print("\nTabla Actives limpia:")
    print(df_actives.head())
    
    # Reemplazar 'mercado' en Actives por su 'mercado_id'
    df_activos_actualizado = etl.replace_market_in_actives(df_actives, ruta_market)
    print("\nTabla Actives actualizada (mercado):")
    print(df_activos_actualizado.head())
    
    # Reemplazar 'sector' en Actives por su 'sector_id' usando el mapeo de sectores
    ruta_sector_mapping = ruta_sector_salida  # Se utiliza el CSV ya generado de sectores
    df_activos_sect = etl.replace_sector_in_actives(df_activos_actualizado, ruta_sector_mapping)
    print("\nTabla Actives actualizada (sector):")
    print(df_activos_sect.head())
    
    # **** Exportar DataFrame intermedio ****
    # Se exporta después de la eliminación de sufijos y mapeo de mercado/sector, pero antes de eliminar duplicados.
    ruta_actives_intermedia = r"C:\Users\spinz\OneDrive\Documentos\Portafolio oficial\HERMESDB\HERMESDB\test\data\logs\Table_Actives_Intermedio.csv"
    df_activos_sect.to_csv(ruta_actives_intermedia, index=False)
    print(f"\nDataFrame intermedio exportado a: {ruta_actives_intermedia}")
    
    # Eliminar duplicados en la columna 'Symbol' para evitar duplicados en 'simbolo' en el resultado final
    df_activos_sect = df_activos_sect.drop_duplicates(subset=['Symbol'])
    
    # Realizar modificaciones finales en el DataFrame:
    # 1. Agregar la columna 'activo_id' como índice incremental.
    df_activos_sect.insert(0, 'activo_id', range(1, len(df_activos_sect) + 1))
    
    # 2. Renombrar las columnas:
    #    - 'Symbol'   -> 'simbolo'
    #    - 'mercado'  -> 'mercado_id'
    #    - 'sector'   -> 'sector_id'
    df_activos_sect = df_activos_sect.rename(columns={
        "Symbol": "simbolo",
        "mercado": "mercado_id",                
        "sector": "sector_id"
    })
    
    # Exportar solo el DataFrame final como Table_Assets.csv
    ruta_assets = r"C:\Users\spinz\OneDrive\Documentos\Portafolio oficial\HERMESDB\HERMESDB\test\data\backup\Table_Assets.csv"
    df_activos_sect.to_csv(ruta_assets, index=False)
    print("\nTabla Assets exportada:")
    print(df_activos_sect.head())
