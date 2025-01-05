import pandas as pd

file_path = 'C:\\Users\\spinz\\Documents\\Portafolio Oficial\\HERMESDB\\data\\tables_data\\MARKET_DATA_BID.csv'

# Leer las líneas específicas
with open(file_path, 'r') as file:
    lines = file.readlines()[50500:51000]  # Lee solo las líneas relevantes

# Guardarlas en un archivo temporal para inspección
with open('problematic_lines.csv', 'w') as out_file:
    out_file.writelines(lines)

print("Líneas problemáticas guardadas en 'problematic_lines.csv'")
