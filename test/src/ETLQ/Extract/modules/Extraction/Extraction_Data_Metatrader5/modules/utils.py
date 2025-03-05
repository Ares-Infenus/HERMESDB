# ----------------------------
# Descripcion
# ----------------------------

""" Este código define una función generate_month_ranges que genera una lista de tuplas con
los rangos de fechas de inicio y fin de cada mes entre dos fechas dadas (initial_date y final_date).
Recorre los meses de forma iterativa, asegurando que el rango final no exceda la fecha límite."""

# ----------------------------
# librerias y dependencias
# ----------------------------

from datetime import datetime

# ----------------------------
# Codigo
# ----------------------------


def generate_month_ranges(initial_date: datetime, final_date: datetime) -> list:
    """
    Genera una lista de tuplas (inicio, fin) para cada mes entre initial_date y final_date.
    """
    ranges = []
    current_start = initial_date
    while current_start < final_date:
        if current_start.month == 12:
            next_month = datetime(current_start.year + 1, 1, 1)
        else:
            next_month = datetime(current_start.year, current_start.month + 1, 1)
        current_end = min(next_month, final_date)
        ranges.append((current_start, current_end))
        current_start = next_month
    return ranges
