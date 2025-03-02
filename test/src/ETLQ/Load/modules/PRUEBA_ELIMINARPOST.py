import pandas as pd
from rich.progress import Progress

filename = r'C:\Users\spinz\OneDrive\Documentos\Portafolio oficial\HERMESDB\HERMESDB\test\data\backup\Table_Datos_historicos.csv'
output_file = r'C:\Users\spinz\OneDrive\Documentos\Portafolio oficial\HERMESDB\HERMESDB\test\data\backup\duplicados.csv'
chunksize = 100000  # Puedes ajustar el tamaño del chunk según la memoria disponible

# Función para generar clave como string
def generar_key(df):
    return (df['activo_id'].astype(str) + '_' +
            df['broker_id'].astype(str) + '_' +
            df['timestamp'].astype(str) + '_' +
            df['timeframe'].astype(str))

# Contar el número total de líneas para la barra de progreso
with open(filename, 'rb') as f:
    total_lines = sum(1 for _ in f) - 1  # Restar 1 por la cabecera

global_index = 0
duplicados_totales = []
key_counts = {}

with Progress() as progress:
    task = progress.add_task("[cyan]Contando ocurrencias...", total=total_lines)
    for chunk in pd.read_csv(filename, chunksize=chunksize, engine='c'):
        chunk_len = len(chunk)
        chunk.reset_index(drop=True, inplace=True)
        
        # Convertir las columnas clave a string y crear la clave concatenada
        chunk['key'] = generar_key(chunk)
        
        # Actualizar el conteo de claves
        key_counts_chunk = chunk['key'].value_counts().to_dict()
        for key, count in key_counts_chunk.items():
            key_counts[key] = key_counts.get(key, 0) + count

        global_index += chunk_len
        progress.update(task, advance=chunk_len)

# Identificar las claves duplicadas
dup_keys = {key for key, count in key_counts.items() if count > 1}

# Crear un DataFrame con las claves duplicadas
dup_keys_df = pd.DataFrame({'key': list(dup_keys)})

global_index = 0

with Progress() as progress:
    task = progress.add_task("[cyan]Extrayendo duplicados...", total=total_lines)
    for chunk in pd.read_csv(filename, chunksize=chunksize, engine='c'):
        chunk_len = len(chunk)
        chunk.reset_index(drop=True, inplace=True)
        chunk['global_index'] = range(global_index, global_index + chunk_len)
        
        # Generar la clave en el chunk
        chunk['key'] = generar_key(chunk)
        
        # Realizar un merge para filtrar solo las filas duplicadas
        chunk_dup = chunk.merge(dup_keys_df, on='key', how='inner')
        
        if not chunk_dup.empty:
            # Asignar el índice mínimo como 'duplicado_de' para cada grupo
            chunk_dup['duplicado_de'] = chunk_dup.groupby('key')['global_index'].transform('min')
            duplicados_totales.append(chunk_dup)
            
        global_index += chunk_len
        progress.update(task, advance=chunk_len)

# Guardar duplicados en un CSV
if duplicados_totales:
    df_duplicados = pd.concat(duplicados_totales)
    df_duplicados.sort_values(by=['key', 'global_index'], inplace=True)
    # (Opcional) Eliminar la columna 'key'
    df_duplicados.drop(columns=['key'], inplace=True)
    df_duplicados.to_csv(output_file, index=False)
    print(f"\nDuplicados encontrados y guardados en: {output_file}")
else:
    print("\nNo se encontraron duplicados.")
