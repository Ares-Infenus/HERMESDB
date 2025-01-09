import pandas as pd

# Ruta al archivo CSV
archivo_csv = r"C:\Users\spinz\Documents\Portafolio Oficial\HERMESDB\data\tables_data\data_table\data_cost_operative\operative_costs.csv"

# Leer el archivo CSV permitiendo datos inconsistentes
df = pd.read_csv(archivo_csv, header=None, dtype=str, sep=";")  # Todas las columnas como cadenas

# Limpiar filas completamente vacías
df = df.dropna(how='all')

# Palabras clave para identificar tipos de activos
palabras_clave = {
    "forex": "forex",
    "ETFs": "ETFs",
    "indices": "indices",
    "commodities": "commodities",
    "stocks": "stocks",
}

# Crear un diccionario para almacenar los DataFrames por tipo de activo
dataframes_por_tipo = {tipo: [] for tipo in palabras_clave.values()}

# Recorrer las filas y clasificar
for index, row in df.iterrows():
    encontrado = False
    for col in row:
        if isinstance(col, str):  # Evitar errores con valores NaN
            for palabra, tipo in palabras_clave.items():
                if palabra.lower() in col.lower():  # Comparación insensible a mayúsculas
                    dataframes_por_tipo[tipo].append(row)
                    encontrado = True
                    break
        if encontrado:
            break

# Nombres de columnas para cada sector
columnas_por_sector = {
    "forex": ["ACTIVE", "SPREAD", "MARGIN", "MIN_SIZE", "CONTRACT_SIZE", "PIP_VALUE", "SWAP_LONG", "SWAP_SHORT", "ELIMINATED", "COMISSION_PER_ORDER", "SECTOR"],
    "indices": ["ACTIVE", "SPREAD", "MARGIN", "MIN_SIZE", "CONTRACT_SIZE", "PIP_VALUE", "SWAP_LONG", "SWAP_SHORT", "ELIMINATED", "COMISSION_PER_ORDER", "SECTOR"],
    "stocks": ["ACTIVE", "NAME", "PLATAFORM", "MARGIN", "MIN_SIZE", "CONTRACT_SIZE", "TICK_VALUE", "SWAP_LONG", "SWAP_SHORT", "ELIMINATED", "COMISSION_PER_ORDER", "AVAILABLE_TO_SELL_SHORT", "SECTOR"],
    "commodities": ["ACTIVE","NAME", "SPREAD", "MARGIN", "MIN_SIZE", "CONTRACT_SIZE", "TICK_VALUE", "SWAP_LONG", "SWAP_SHORT", "SWAP_ROLLOVER", "COMMISSION_PER_ORDER", "SECTOR"],
    "ETFs": ["ACTIVE", "NAME", "MARGIN", "MIN_SIZE", "CONTRACT_SIZE", "TICK_VALUE", "SWAP_LONG", "SWAP_SHORT", "ELIMINATED", "COMISSION_PER_ORDER", "SECTOR"],  # Agregado para "etf"
}

# Ajustar columnas para que coincidan con el número esperado
for tipo in dataframes_por_tipo:
    df_tipo = pd.DataFrame(dataframes_por_tipo[tipo])
    if not df_tipo.empty:
        # Eliminar columnas completamente vacías
        df_tipo = df_tipo.dropna(axis=1, how='all')
        
        # Eliminar columnas adicionales o agregar faltantes para coincidir con el número esperado
        columnas = columnas_por_sector.get(tipo, None)
        if columnas:
            num_columnas_esperadas = len(columnas)
            num_columnas_actuales = df_tipo.shape[1]
            
            if num_columnas_actuales > num_columnas_esperadas:
                # Cortar columnas sobrantes
                df_tipo = df_tipo.iloc[:, :num_columnas_esperadas]
            elif num_columnas_actuales < num_columnas_esperadas:
                # Agregar columnas faltantes
                for _ in range(num_columnas_esperadas - num_columnas_actuales):
                    df_tipo[f"Extra_{_}"] = None
            
            # Asignar nombres de columnas
            if df_tipo.shape[1] == num_columnas_esperadas:
                df_tipo.columns = columnas
            else:
                print(f"Advertencia: No se pudo ajustar completamente las columnas para {tipo}")
        
        dataframes_por_tipo[tipo] = df_tipo


# Guardar cada DataFrame en un archivo CSV separado (opcional)
for tipo, df_tipo in dataframes_por_tipo.items():
    if not df_tipo.empty:  # Asegurar que haya datos antes de guardar
        archivo_salida = f"operative_costs_{tipo}.csv"
        df_tipo.to_csv(archivo_salida, index=False)
        print(f"Guardado: {archivo_salida}")

# Mostrar un ejemplo de los DataFrames separados
for tipo, df_tipo in dataframes_por_tipo.items():
    print(f"\nDataFrame para '{tipo}':")
    print(df_tipo.head())