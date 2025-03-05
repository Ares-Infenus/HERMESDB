# ----------------------------
# Descripcion
# ----------------------------
"""Este código gestiona la descarga de datos históricos de un símbolo financiero desde MetaTrader 5
en diferentes temporalidades. Define la clase WorkerState para almacenar el estado global de los
workers,la función worker_initializer para inicializar las variables y la conexión a MT5, y la 
función download_symbol_data, que descarga los datos por rangos mensuales, los procesa y los
guarda en un archivo CSV, manejando posibles errores."""
# ----------------------------
# librerias y dependencias
# ----------------------------

import os
import pandas as pd
import MetaTrader5 as mt5

# ----------------------------
# Conexiones
# ----------------------------

from .mt5_connection import MT5Connection
from .utils import generate_month_ranges

# ----------------------------
# Codigo
# ----------------------------


class WorkerState:
    """Gestiona el estado global de los workers."""

    credentials = None
    data_directory = None
    date_range = (None, None)


def worker_initializer(credentials: dict, data_directory: str, date_range: tuple):
    """
    Inicializa las variables necesarias en cada proceso worker.
    """
    WorkerState.credentials = credentials
    WorkerState.data_directory = data_directory
    WorkerState.date_range = date_range

    connection = MT5Connection(WorkerState.credentials)
    connection.initialize()


def download_symbol_data(symbol: str) -> dict:
    """
    Descarga los datos históricos para el símbolo dado en diferentes temporalidades.
    Los datos se descargan por rangos mensuales, se procesan y se guardan en un archivo CSV.
    """
    try:
        timeframes = {
            "H1": mt5.TIMEFRAME_H1,
            "H4": mt5.TIMEFRAME_H4,
            "D1": mt5.TIMEFRAME_D1,
            "W1": mt5.TIMEFRAME_W1,
            "MN1": mt5.TIMEFRAME_MN1,
        }
        collected_dfs = []
        timeframe_log = {}

        begin_date, fin_date = WorkerState.date_range
        month_ranges = generate_month_ranges(begin_date, fin_date)

        for tf_label, tf_value in timeframes.items():
            dfs = []
            for month_start, month_end in month_ranges:
                rates = mt5.copy_rates_range(  # pylint: disable=no-member
                    symbol, tf_value, month_start, month_end
                )
                if rates is not None and len(rates) > 0:
                    df_temp = pd.DataFrame(rates)
                    dfs.append(df_temp)
            if dfs:
                df_tf = pd.concat(dfs, ignore_index=True)
                df_tf["time"] = pd.to_datetime(df_tf["time"], unit="s")
                df_tf["timeframe"] = tf_label
                timeframe_log[tf_label] = {
                    "rows": len(df_tf),
                    "first_date": str(df_tf["time"].min()),
                    "last_date": str(df_tf["time"].max()),
                }
                collected_dfs.append(df_tf)
            else:
                timeframe_log[tf_label] = {
                    "rows": 0,
                    "first_date": None,
                    "last_date": None,
                }

        if collected_dfs:
            final_df = pd.concat(collected_dfs, ignore_index=True)
            final_df.drop_duplicates(subset=["time", "timeframe"], inplace=True)
        else:
            final_df = pd.DataFrame()

        safe_symbol = symbol.replace("/", "_")
        output_file = os.path.join(WorkerState.data_directory, f"{safe_symbol}.csv")
        final_df.to_csv(output_file, index=False)

        return {
            "symbol": symbol,
            "log": timeframe_log,
            "file": output_file,
            "error": None,
        }

    except (FileNotFoundError, PermissionError) as e:
        return {
            "symbol": symbol,
            "log": {},
            "file": None,
            "error": f"Error de archivo: {e}",
        }

    except (ValueError, TypeError) as e:
        return {
            "symbol": symbol,
            "log": {},
            "file": None,
            "error": f"Error de datos: {e}",
        }

    except ConnectionError as e:
        return {
            "symbol": symbol,
            "log": {},
            "file": None,
            "error": f"Error de conexión: {e}",
        }

    except OSError as e:
        return {
            "symbol": symbol,
            "log": {},
            "file": None,
            "error": f"Error del sistema: {e}",
        }
