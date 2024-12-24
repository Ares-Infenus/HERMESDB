def validar_diccionario(diccionario):
    """
    Función para validar la estructura y los valores de un diccionario de criterios predefinidos.

    Propósito:
    - Garantizar que el diccionario contiene únicamente las claves permitidas.
    - Verificar que todos los valores sean enteros mayores o iguales a 0.
    - Confirmar que todos los valores sean 0 para pasar la validación.

    Parámetros:
    - diccionario (dict): Un diccionario que debe contener exclusivamente las claves permitidas 
      y valores válidos según las especificaciones.

    Claves permitidas:
    - 'Archivos de más'
    - 'Duplicados'
    - 'Faltantes'
    - 'Errores en nombres'
    - 'Carpetas vacías'

    Retorna:
    - str: Un mensaje indicando el resultado de la validación:
        - "Verificación superada: Todos los criterios cumplen los requisitos establecidos." 
          si el diccionario es válido y todos los valores son 0.
        - "Verificación completada: Algunos criterios no cumplen los requisitos establecidos." 
          si el diccionario es válido pero tiene valores distintos de 0.

    Excepciones:
    - ValueError: Se lanza si el diccionario no cumple con las siguientes condiciones:
        - Tiene claves adicionales no permitidas.
        - Algún valor no es un entero o es menor que 0.

    Ejemplo de uso:
    diccionario_ejemplo = {
        'Archivos de más': 0,
        'Duplicados': 0,
        'Faltantes': 0,
        'Errores en nombres': 0,
        'Carpetas vacías': 0
    }
    resultado = validar_diccionario(diccionario_ejemplo)
    print(resultado)
    """
    # Definir las claves permitidas en el diccionario
    claves_permitidas = ['Archivos de más', 'Duplicados', 'Faltantes', 'Errores en nombres', 'Carpetas vacías']

    # Verificación 1: Validar que no existan claves adicionales en el diccionario
    for clave in diccionario.keys():
        if clave not in claves_permitidas:
            raise ValueError(
                f"Error crítico: Verificación 1 fallida. E001 - Estructura del diccionario incorrecta. "
                f"Las claves permitidas son: {claves_permitidas}. Clave no permitida detectada: {clave}"
            )

    # Verificación 2: Validar que todos los valores sean enteros mayores o iguales a 0
    for clave, valor in diccionario.items():
        if not isinstance(valor, int) or valor < 0:
            raise ValueError(
                f"Error crítico: Verificación 2 fallida. E002 - Valores inválidos detectados en la clave: {clave}. "
                f"Valor recibido: {valor}. Asegúrese de que todos los valores sean enteros mayores o iguales a 0."
            )

    # Verificación de éxito: Confirmar si todos los valores son 0
    if all(valor == 0 for valor in diccionario.values()):
        return "Verificación superada: Todos los criterios cumplen los requisitos establecidos."
    else:
        return "Verificación completada: Algunos criterios no cumplen los requisitos establecidos."


# Ejemplo de uso
#diccionario_ejemplo = {
#    'Archivos de más': 0,
#    'Duplicados': 0,
#    'Faltantes': 0,
#    'Errores en nombres': 0,
#    'Carpetas vacías': 0
#}

# Resultado de la validación
#resultado = validar_diccionario(diccionario_ejemplo)
#print(resultado)
