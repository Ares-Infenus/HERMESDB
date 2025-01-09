from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import pandas as pd
import time

class FinancialOperationalCostExtractor:
    def __init__(self, driver_path):
        self.driver_path = driver_path
        self.chrome_options = Options()
        self.chrome_options.add_argument("--no-sandbox")
        self.chrome_options.add_argument("--disable-dev-shm-usage")
        self.service = Service(driver_path)
        self.driver = None
        self.urls_and_sectors = [
            {"url": "https://www.darwinex.com/global/forex-cfds/indices", "sector": "indices"},
            {"url": "https://www.darwinex.com/global/forex-cfds/forex", "sector": "forex"},
            {"url": "https://www.darwinex.com/global/forex-cfds/commodities", "sector": "commodities"},
            {"url": "https://www.darwinex.com/global/forex-cfds/etfs-usd", "sector": "ETFs"},
            {"url": "https://www.darwinex.com/global/forex-cfds/stocks-usd", "sector": "stocks"},
            {"url": "https://www.darwinex.com/global/forex-cfds/stocks-usd", "sector": "stocks"}
        ]
    
    def start_driver(self):
        self.driver = webdriver.Chrome(service=self.service, options=self.chrome_options)
    
    def extract_table_data(self, url, sector):
        self.driver.get(url)
        time.sleep(5)
        
        tables = self.driver.find_elements(By.TAG_NAME, 'table')
        data = []
        
        for table in tables:
            rows = table.find_elements(By.TAG_NAME, 'tr')
            for row in rows:
                cells = row.find_elements(By.TAG_NAME, 'td')
                row_data = [cell.text for cell in cells]
                if row_data:
                    row_data.append(sector)
                    data.append(row_data)
        
        return data
    
    def extract_all_data(self, export_path):
        try:
            self.start_driver()
            all_data = []
            
            for entry in self.urls_and_sectors:
                url = entry["url"]
                sector = entry["sector"]
                all_data.extend(self.extract_table_data(url, sector))
            
            df = pd.DataFrame(all_data)
            df.to_csv(export_path, index=False, header=False, sep=";")  # Cambiar delimitador a ;
            print(f"Data successfully exported to: {export_path}")
            
        except Exception as e:
            print(f"Error during extraction: {str(e)}")
            
        finally:
            if self.driver:
                self.driver.quit()


# Usage example:
# extractor = FinancialOperationalCostExtractor("path/to/chromedriver.exe")
# extractor.extract_all_data("path/to/export/costs.csv")