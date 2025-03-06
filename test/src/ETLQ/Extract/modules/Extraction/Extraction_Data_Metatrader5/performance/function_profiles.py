# ----------------------------
# Descripcion
# ----------------------------
"""
Este código define la clase ProfileExporter, que encapsula la lógica para exportar
perfiles de rendimiento generados con cProfile a un archivo CSV, siguiendo el principio
de Single Responsibility (SRP). La clase recibe un objeto cProfile.Profile y opcionalmente
un mapeo de uso de memoria por función. Al ejecutar export_to_csv, extrae estadísticas
como el número de llamadas, tiempo total, tiempo promedio, tiempo en CPU y tiempo de
espera por función, además de asociar el uso de memoria si está disponible. Luego,
organiza estos datos en un DataFrame de pandas y los guarda en un archivo CSV, permitiendo
un análisis detallado del rendimiento del código.
"""
# ----------------------------
# librerias y dependencias
# ----------------------------

# Standard library imports
import os
import cProfile
import pstats

# Third-party imports
import pandas as pd

# ----------------------------
# Conexiones
# ----------------------------


# ----------------------------
# Codigo
# ----------------------------


class ProfileExporter:
    """
    Encapsula la lógica para exportar los perfiles de rendimiento a CSV.
    Cumple con el Single Responsibility Principle (SRP).
    """

    def __init__(self, profiler: cProfile.Profile, memory_mapping: dict = None):
        self.profiler = profiler
        self.memory_mapping = memory_mapping or {}

    def export_to_csv(self, csv_output: str):
        """
        Exporta el perfil de cProfile a un archivo CSV con las columnas requeridas.
        """
        stats_obj = pstats.Stats(self.profiler)
        stats_data = []

        for func_desc, func_stats in stats_obj.stats.items():
            filename, _, func_name = func_desc
            call_count = func_stats[0]
            cpu_time = func_stats[2]  # Tiempo en la función sin subllamadas
            total_time = func_stats[3]  # Tiempo total acumulado (incluye subllamadas)
            avg_time = total_time / call_count if call_count else 0
            wait_time = total_time - cpu_time
            file_name = os.path.basename(filename)

            key = (file_name, func_name)
            mem_used = self.memory_mapping.get(key, None)

            stats_data.append(
                {
                    "function_name": func_name,
                    "total_time": total_time,
                    "call_count": call_count,
                    "avg_time": avg_time,
                    "cpu_time": cpu_time,
                    "wait_time": wait_time,
                    "memory_usage": mem_used,
                    "file": file_name,
                }
            )

        df = pd.DataFrame(
            stats_data,
            columns=[
                "function_name",
                "total_time",
                "call_count",
                "avg_time",
                "cpu_time",
                "wait_time",
                "memory_usage",
                "file",
            ],
        )
        df.to_csv(csv_output, index=False)
        print(f"Resultados exportados a {csv_output}")
