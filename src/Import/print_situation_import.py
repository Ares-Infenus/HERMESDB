def describir_estructura(diccionario, nivel=0):
    for clave, valor in diccionario.items():
        if isinstance(valor, pd.DataFrame):
            print('│   ' * nivel + '├── ' + clave + '.csv')  # Assuming CSV for simplicity
        else:
            print('│   ' * nivel + '├── ' + clave + '/')
            if isinstance(valor, dict):
                describir_estructura(valor, nivel + 1)

# Llamar a la función con el diccionario de datos
#describir_estructura(diccionario_datos)