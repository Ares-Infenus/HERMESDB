# ----------------------------
# Descripcion
# ----------------------------

""" Este código define la clase MT5Connection, que encapsula la gestión de conexión y
desconexión con MetaTrader 5 (MT5). Permite inicializar la conexión usando credenciales
proporcionadas y cerrarla cuando sea necesario. """

# ----------------------------
# Conexiones
# ----------------------------

# ----------------------------
# librerias y dependencias
# ----------------------------

import MetaTrader5 as mt5

# ----------------------------
# Codigo
# ----------------------------


class MT5Connection:
    """
    Encapsula la inicialización y desconexión de MetaTrader5.
    """

    def __init__(self, credentials: dict):
        """
        Inicializa la conexión con las credenciales requeridas.

        Args:
            credentials (dict): Credenciales con claves 'server', 'login', 'password'
                                y opcionalmente 'investor_password'.
        """
        self.credentials = credentials

    def initialize(self):
        """
        Inicializa la conexión a MetaTrader5 con las credenciales proporcionadas.

        Raises:
            RuntimeError: Si falla la inicialización, se lanza una excepción con el error.
        """
        init_args = {
            "server": self.credentials["server"],
            "login": self.credentials["login"],
            "password": self.credentials["password"],
        }
        if "investor_password" in self.credentials:
            init_args["investor_password"] = self.credentials["investor_password"]

        if not mt5.initialize(**init_args):  # pylint: disable=no-member
            error_msg = f"Falló la inicialización de MT5: {mt5.last_error()}"  # pylint: disable=no-member
            raise RuntimeError(error_msg)

    @staticmethod
    def shutdown():
        """
        Cierra la conexión a MetaTrader5.
        """
        mt5.shutdown()  # pylint: disable=no-member
