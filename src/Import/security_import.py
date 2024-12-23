import re

# Función principal para verificar la estructura y validez de un diccionario de archivos CSV.
# Este diccionario contiene los datos organizados por mercado, activo, intervalo y los archivos CSV asociados.
# Se realizan diversas validaciones para asegurar que los archivos cumplen con las expectativas de formato y organización.
def verificar_diccionario(diccionario):
    """
    Verifica la estructura y validez de los archivos CSV en un diccionario de datos.

    Esta función verifica las siguientes condiciones:
    - Si los archivos están presentes y no son de más.
    - Si los archivos ASK y BID están correctamente nombrados y no están duplicados.
    - Si los archivos están correctamente organizados dentro de las carpetas.
    - Si los archivos siguen el formato correcto de nombre (ACTIVO_INTERVALO_ASK/BID.csv).

    Parámetros:
        diccionario (dict): Un diccionario que contiene la estructura de mercados, activos, intervalos y los archivos CSV asociados.

    Retorna:
        dict: Un diccionario con los errores encontrados y un resumen de las validaciones.
              El resumen incluye el número de errores por tipo (archivos de más, duplicados, faltantes, errores de nombres y carpetas vacías).
    """
    # Diccionario para almacenar los errores encontrados en las validaciones
    errores = {}

    # Contadores de errores clasificados por tipo
    error_count = {
        "Archivos de más": 0,
        "Duplicados": 0,
        "Faltantes": 0,
        "Errores en nombres": 0,
        "Carpetas vacías": 0
    }
    
    # ID único para cada error encontrado
    error_id = 1

    # Función auxiliar para agregar un error al diccionario de errores
    def agregar_error(mensaje):
        """
        Agrega un mensaje de error al diccionario de errores.

        Esta función asigna un ID único a cada error y lo guarda en el diccionario de errores con una clave
        basada en el ID incrementado.
        
        Parámetros:
            mensaje (dict): Un diccionario con los detalles del error, como el mercado, activo, intervalo y el mensaje de error.
        """
        nonlocal error_id  # Usamos error_id de la función principal
        errores[f"Error{error_id}"] = mensaje  # Agregamos el mensaje de error al diccionario
        error_id += 1  # Incrementamos el ID para el siguiente error

    # Función para validar si un archivo tiene el formato correcto
    def validar_archivo(nombre_archivo, activo, intervalo):
        """
        Verifica si el nombre de un archivo cumple con el formato esperado.

        El formato esperado es "ACTIVO_INTERVALO_ASK/BID.csv", donde ACTIVO y INTERVALO son variables dinámicas.

        Parámetros:
            nombre_archivo (str): El nombre del archivo a validar.
            activo (str): El nombre del activo (por ejemplo, "EURUSD").
            intervalo (str): El intervalo de tiempo (por ejemplo, "1H").

        Retorna:
            bool: True si el archivo cumple con el formato, False en caso contrario.
        """
        patron = rf"^{activo}_{intervalo}_(ASK|BID)\.csv$"
        return re.match(patron, nombre_archivo) is not None  # Retorna True si el archivo coincide con el patrón

    # Iteramos sobre el diccionario de datos, verificando cada elemento
    for mercado, activos in diccionario.items():
        for activo, intervalos in activos.items():
            for intervalo, archivos in intervalos.items():
                # Verificamos si la carpeta está vacía, en cuyo caso se reporta un error
                if not archivos:
                    agregar_error({
                        "Mercado": mercado,
                        "Activo": activo,
                        "Horario": intervalo,
                        "Error": "Carpeta vacía o archivo no compatible, revise documentación"
                    })
                    error_count["Carpetas vacías"] += 1
                    continue  # Continuamos al siguiente intervalo si la carpeta está vacía

                # Filtramos los archivos por tipo: ASK, BID y otros desconocidos
                archivos_ask = [archivo for archivo in archivos if "ASK" in archivo]
                archivos_bid = [archivo for archivo in archivos if "BID" in archivo]
                archivos_desconocidos = [archivo for archivo in archivos if "ASK" not in archivo and "BID" not in archivo]

                # Si hay archivos desconocidos, los reportamos como "Archivos de más"
                for archivo in archivos_desconocidos:
                    agregar_error({
                        "Mercado": mercado,
                        "Activo": activo,
                        "Horario": intervalo,
                        "Error": f"Archivo de más: {archivo}"
                    })
                    error_count["Archivos de más"] += 1

                # Verificamos que no haya duplicados de archivos ASK
                if len(archivos_ask) > 1:
                    for archivo in archivos_ask:
                        agregar_error({
                            "Mercado": mercado,
                            "Activo": activo,
                            "Horario": intervalo,
                            "Error": f"Duplicación de archivo: {archivo}"
                        })
                        error_count["Duplicados"] += 1

                # Verificamos que no haya duplicados de archivos BID
                if len(archivos_bid) > 1:
                    for archivo in archivos_bid:
                        agregar_error({
                            "Mercado": mercado,
                            "Activo": activo,
                            "Horario": intervalo,
                            "Error": f"Duplicación de archivo: {archivo}"
                        })
                        error_count["Duplicados"] += 1

                # Si falta el archivo ASK, lo reportamos como faltante
                if len(archivos_ask) == 0:
                    agregar_error({
                        "Mercado": mercado,
                        "Activo": activo,
                        "Horario": intervalo,
                        "Error": "Falta el archivo ASK"
                    })
                    error_count["Faltantes"] += 1

                # Si falta el archivo BID, lo reportamos como faltante
                if len(archivos_bid) == 0:
                    agregar_error({
                        "Mercado": mercado,
                        "Activo": activo,
                        "Horario": intervalo,
                        "Error": "Falta el archivo BID"
                    })
                    error_count["Faltantes"] += 1

                # Verificamos que todos los archivos tengan el nombre correcto
                for archivo in archivos:
                    if not validar_archivo(archivo, activo, intervalo):
                        agregar_error({
                            "Mercado": mercado,
                            "Activo": activo,
                            "Horario": intervalo,
                            "Error": "Error en la declaración de intervalos y activos"
                        })
                        error_count["Errores en nombres"] += 1

    # Finalmente, agregamos el resumen con la cantidad de errores por tipo
    errores["Resumen"] = error_count
    return errores  # Retornamos el diccionario con los errores encontrados

# Llamar a la función con el diccionario de datos para generar el reporte de errores
reporte_errores = verificar_diccionario(diccionario_datos)
print(reporte_errores)
