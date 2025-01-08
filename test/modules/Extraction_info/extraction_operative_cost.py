from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import pandas as pd
import time

# Configurar el driver
driver_path = r"C:\Users\spinz\Documents\Portafolio Oficial\HERMESDB\test\modules\Extraction_info\chrome_driver\chromedriver.exe"
# Configurar las opciones del driver
chrome_options = Options()
# chrome_options.add_argument("--headless")  # Opcional: ejecuta en segundo plano
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")

# Configurar el driver
service = Service(driver_path)
driver = webdriver.Chrome(service=service, options=chrome_options)

# Lista de URLs para extraer los datos y los sectores asociados
urls_and_sectors = [
    {"url": "https://www.darwinex.com/global/forex-cfds/indices", "sector": "indices"},
    {"url": "https://www.darwinex.com/global/forex-cfds/forex", "sector": "forex"},
    {"url": "https://www.darwinex.com/global/forex-cfds/commodities", "sector": "commodities"},
    {"url": "https://www.darwinex.com/global/forex-cfds/etfs-usd", "sector": "etf"},
    {"url": "https://www.darwinex.com/global/forex-cfds/stocks-usd", "sector": "stocks"},
    {"url": "https://www.darwinex.com/global/forex-cfds/stocks-usd", "sector": "stocks"}
]

# Función para extraer los datos de las tablas
def extract_table_data(url, sector):
    driver.get(url)
    time.sleep(5)  # Esperar a que la página cargue completamente (ajustar si es necesario)

    # Extraer todas las tablas en la página (sin especificar clase)
    tables = driver.find_elements(By.TAG_NAME, 'table')
    data = []

    for table in tables:
        # Extraer todas las filas de la tabla
        rows = table.find_elements(By.TAG_NAME, 'tr')
        for row in rows:
            # Extraer las celdas de cada fila
            cells = row.find_elements(By.TAG_NAME, 'td')
            row_data = [cell.text for cell in cells]
            if row_data:  # Evitar filas vacías
                # Agregar el sector como una columna adicional
                row_data.append(sector)
                data.append(row_data)
    
    return data

# Extraer y guardar los datos de cada URL
all_data = []
for entry in urls_and_sectors:
    url = entry["url"]
    sector = entry["sector"]
    all_data.extend(extract_table_data(url, sector))

# Guardar los datos en un archivo CSV
df = pd.DataFrame(all_data)
df.to_csv("tabla_darwinex.csv", index=False, header=False)  # Ajusta el nombre del archivo si es necesario

# Cerrar el driver
driver.quit()

print("Datos extraídos y guardados en 'tabla_darwinex.csv'")
