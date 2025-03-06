# ----------------------------
# Descripcion
# ----------------------------

"""
El código define la clase SystemMonitor, diseñada para monitorear en tiempo real el
uso de recursos del sistema, incluyendo CPU, memoria, disco y red, y almacenar las
métricas en un archivo CSV. Utiliza psutil para capturar información sobre el consumo
de CPU global y por núcleo, memoria utilizada en MB, actividad del disco y tráfico de
red en intervalos configurables. La monitorización se ejecuta en un hilo separado
mediante threading, asegurando que la captura de datos no afecte el rendimiento del
proceso principal. Los datos se almacenan en una lista protegida por un Lock para
evitar problemas de concurrencia, y al detener el monitoreo, se exportan a un archivo
CSV en la ruta especificada, garantizando la persistencia de la información.
"""

# ----------------------------
# librerias y dependencias
# ----------------------------

# Standard library imports
import os
import threading
import json
import time
from datetime import datetime

# Third-party imports
import psutil
import pandas as pd


# ----------------------------
# Conexiones
# ----------------------------


# ----------------------------
# Codigo
# ----------------------------


class SystemMonitor:
    """
    Monitorea los recursos del sistema (CPU, memoria, disco y red) y los almacena en un CSV.
    """

    def __init__(self, csv_path: str, interval: float = 2.0) -> None:
        """
        Inicializa la instancia del monitor del sistema.

        Args:
            csv_path (str): Ruta del archivo CSV donde se guardarán las métricas.
            interval (float, optional): Intervalo en segundos entre cada captura. Defaults a 2.0.
        """
        self.csv_path = csv_path
        self.interval = interval
        self.running = False
        self.thread = None
        self.metrics = []
        self.lock = threading.Lock()
        net_counters = psutil.net_io_counters()
        self.last_net_bytes = net_counters.bytes_sent + net_counters.bytes_recv

    def log_metrics(self) -> None:
        """
        Captura las métricas del sistema en intervalos regulares y las almacena en la
        lista de métricas.
        """
        while self.running:
            timestamp = datetime.now()
            cpu_usage = psutil.cpu_percent()
            cpu_usage_per_core = json.dumps(psutil.cpu_percent(percpu=True))
            memory_used_mb = psutil.virtual_memory().used / (1024**2)
            disk_counters = psutil.disk_io_counters()
            disk_usage_mb = (disk_counters.read_bytes + disk_counters.write_bytes) / (
                1024**2
            )
            net_counters = psutil.net_io_counters()
            current_net_bytes = net_counters.bytes_sent + net_counters.bytes_recv
            delta_net_bytes = current_net_bytes - self.last_net_bytes
            network_usage_mbs = delta_net_bytes / self.interval / (1024**2)
            self.last_net_bytes = current_net_bytes

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

    def start(self) -> None:
        """
        Inicia la monitorización del sistema en un hilo separado.
        """
        self.running = True
        self.thread = threading.Thread(target=self.log_metrics, daemon=True)
        self.thread.start()

    def stop(self) -> None:
        """
        Detiene la monitorización y exporta las métricas capturadas a un archivo CSV.
        """
        self.running = False
        if self.thread:
            self.thread.join()

        parent_dir = os.path.dirname(self.csv_path)
        os.makedirs(parent_dir, exist_ok=True)

        df = pd.DataFrame(self.metrics)
        df.to_csv(self.csv_path, index=False)
