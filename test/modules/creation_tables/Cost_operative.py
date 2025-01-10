import pandas as pd
import glob
import os

# Define la carpeta que contiene los archivos CSV
carpeta = r"C:\Users\spinz\Documents\Portafolio Oficial\HERMESDB\data\tables_data\data_table\data_cost_operative_organizada"

# Busca todos los archivos CSV en la carpeta
archivos_csv = glob.glob(os.path.join(carpeta, "*.csv"))

# Crea una lista para almacenar los DataFrames
dataframes = []

# Define las columnas requeridas
columnas_requeridas = ["MARGIN", "MIN_SIZE", "CONTRACT_SIZE", "PIP_VALUE", "SWAP_LONG", "SWAP_SHORT", "COMISSION_PER_ORDEN", "TICK_VALUE"]

# Itera sobre los archivos y léelos
for archivo in archivos_csv:
    df = pd.read_csv(archivo)

    # Verifica si falta PIP_VALUE y lo añade basado en TICK_VALUE si es necesario
    if "PIP_VALUE" not in df.columns:
        df["PIP_VALUE"] = df["TICK_VALUE"] if "TICK_VALUE" in df.columns else pd.NA

    # Asegura que las columnas requeridas estén presentes
    for col in columnas_requeridas:
        if col not in df.columns:
            df[col] = pd.NA

    # Filtra las columnas requeridas
    df_filtrado = df[columnas_requeridas]

    # Añade una columna para identificar el archivo
    df_filtrado["archivo"] = os.path.basename(archivo)

    dataframes.append(df_filtrado)

# Combina todos los DataFrames en uno solo
dataframe_unificado = pd.concat(dataframes, ignore_index=True)

# Guarda el DataFrame unificado en un archivo CSV
dataframe_unificado.to_csv("dataframe_unificado.csv", index=False)

print("DataFrame unificado creado exitosamente.")
