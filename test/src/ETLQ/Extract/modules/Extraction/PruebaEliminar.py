import os
import threading
import time
import json
import psutil
import pandas as pd
from datetime import datetime


class SystemMonitor:
    """
    Monitorea el uso de CPU, memoria, disco y red, y almacena las métricas en una lista.
    Al finalizar, se exporta esa lista a un archivo CSV.
    """

    def __init__(self, csv_path: str, interval: float = 2.0):
        """
        Args:
            csv_path (str): Ruta donde se guardará el CSV con las métricas.
            interval (float): Intervalo en segundos entre cada captura.
        """
        self.csv_path = csv_path
        self.interval = interval
        self.running = False
        self.thread = None
        self.metrics = []  # Almacenará las métricas como una lista de diccionarios.
        self.lock = threading.Lock()
        # Captura inicial de bytes para calcular el uso de red.
        net_counters = psutil.net_io_counters()
        self.last_net_bytes = net_counters.bytes_sent + net_counters.bytes_recv

    def log_metrics(self):
        """Captura y almacena las métricas en cada intervalo."""
        while self.running:
            timestamp = datetime.now()
            # Uso de CPU total y por núcleo
            cpu_usage = psutil.cpu_percent()
            cpu_usage_per_core = json.dumps(psutil.cpu_percent(percpu=True))
            # Uso de memoria en MB
            memory_used_mb = psutil.virtual_memory().used / (1024**2)
            # Uso de disco: suma de bytes leídos y escritos, convertido a MB
            disk_counters = psutil.disk_io_counters()
            disk_usage_mb = (disk_counters.read_bytes + disk_counters.write_bytes) / (
                1024**2
            )
            # Uso de red: tasa en MB/s calculada a partir de la diferencia de bytes
            net_counters = psutil.net_io_counters()
            current_net_bytes = net_counters.bytes_sent + net_counters.bytes_recv
            delta_net_bytes = current_net_bytes - self.last_net_bytes
            network_usage_mbs = delta_net_bytes / self.interval / (1024**2)
            self.last_net_bytes = current_net_bytes

            # Preparar diccionario con todas las métricas
            metrics_dict = {
                "timestamp": timestamp,
                "cpu_usage": cpu_usage,
                "memory_usage": memory_used_mb,
                "disk_usage": disk_usage_mb,
                "network_usage": network_usage_mbs,
                "cpu_usage_per_core": cpu_usage_per_core,
            }

            with self.lock:
                self.metrics.append(metrics_dict)
            time.sleep(self.interval)

    def start(self):
        """Inicia el monitoreo en un hilo separado."""
        self.running = True
        self.thread = threading.Thread(target=self.log_metrics, daemon=True)
        self.thread.start()

    def stop(self):
        """Detiene el monitoreo y exporta las métricas a un archivo CSV."""
        self.running = False
        if self.thread:
            self.thread.join()
        # Convertir la lista de métricas a DataFrame y exportar a CSV.
        df = pd.DataFrame(self.metrics)
        df.to_csv(self.csv_path, index=False)


# Ejemplo de uso del SystemMonitor:
if __name__ == "__main__":
    LOG_FOLDER = r"C:\ruta\al\directorio\logs"  # Actualiza esta ruta según tu entorno
    os.makedirs(LOG_FOLDER, exist_ok=True)
    csv_path = os.path.join(
        LOG_FOLDER, f"system_metrics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    )

    monitor = SystemMonitor(csv_path=csv_path, interval=2.0)
    monitor.start()

    try:
        # Simulación de ejecución prolongada del código principal.
        for i in range(10):
            print(f"Procesando iteración {i+1}...")
            time.sleep(2)
    finally:
        monitor.stop()
        print(f"Monitoreo de sistema finalizado. CSV exportado en:\n{csv_path}")
