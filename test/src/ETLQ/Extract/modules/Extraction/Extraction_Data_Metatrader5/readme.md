# Extraction_Data_Metatrader5

![UML Architecture](./uml_architecture.png)

## Tabla de modulos

- [main](#descripción)
  - [Descripción](#descripción)
  - [Objetivos](#objetivos)
  - [Entradas y Salidas](#entradas-y-salidas)
  - [Componentes del Código](#componentes-del-código)
  - [Ejemplo de Ejecución](#ejemplo-de-ejecución)
  - [Instalación y Dependencias](#instalación-y-dependencias)
  - [Consideraciones Técnicas](#consideraciones-técnicas)
- [historical_data_downloader](#objetivos)
  - [Descripción](#descripción)
  - [Objetivos](#objetivos)
  - [Entradas y Salidas](#entradas-y-salidas)
  - [Componentes del Código](#componentes-del-código)
  - [Ejemplo de Ejecución](#ejemplo-de-ejecución)
  - [Instalación y Dependencias](#instalación-y-dependencias)
  - [Consideraciones Técnicas](#consideraciones-técnicas)
- [logger_config](#entradas-y-salidas)
  - [Descripción](#descripción)
  - [Objetivos](#objetivos)
  - [Entradas y Salidas](#entradas-y-salidas)
  - [Componentes del Código](#componentes-del-código)
  - [Ejemplo de Ejecución](#ejemplo-de-ejecución)
  - [Instalación y Dependencias](#instalación-y-dependencias)
  - [Consideraciones Técnicas](#consideraciones-técnicas)
- [mt5_connection](#componentes-del-código)
  - [Descripción](#descripción)
  - [Objetivos](#objetivos)
  - [Entradas y Salidas](#entradas-y-salidas)
  - [Componentes del Código](#componentes-del-código)
  - [Ejemplo de Ejecución](#ejemplo-de-ejecución)
  - [Instalación y Dependencias](#instalación-y-dependencias)
  - [Consideraciones Técnicas](#consideraciones-técnicas)
- [utils](#ejemplo-de-ejecución)
  - [Descripción](#descripción)
  - [Objetivos](#objetivos)
  - [Entradas y Salidas](#entradas-y-salidas)
  - [Componentes del Código](#componentes-del-código)
  - [Ejemplo de Ejecución](#ejemplo-de-ejecución)
  - [Instalación y Dependencias](#instalación-y-dependencias)
  - [Consideraciones Técnicas](#consideraciones-técnicas)


## Descripción
El código implementa un monitor de sistema que captura y registra periódicamente métricas de rendimiento (uso de CPU, memoria, disco y red). Al finalizar la ejecución, las métricas se exportan a un archivo CSV para su análisis.

## Objetivos
- **Monitorear**: Captura datos de rendimiento en intervalos definidos.
- **Almacenar**: Guarda las métricas en una lista y las exporta a un CSV.
- **Ejecutar Concurrentemente**: Utiliza hilos para evitar bloquear el flujo principal del programa.

## Entradas y Salidas

| **Entradas**  | **Descripción**                                            |
|---------------|------------------------------------------------------------|
| `csv_path`    | Ruta donde se guardará el archivo CSV.                     |
| `interval`    | Intervalo en segundos entre cada captura de métricas.      |

| **Salidas**           | **Descripción**                                              |
|-----------------------|--------------------------------------------------------------|
| Archivo CSV           | Registro de todas las métricas capturadas.                   |
| Mensajes en consola   | Indicadores del progreso de la ejecución del programa.       |

## Componentes del Código

### 1. Importaciones y Configuración Inicial
Se importan librerías necesarias como `os`, `threading`, `time`, `json`, `psutil`, `pandas` y `datetime` para manejar operaciones del sistema, concurrencia, manipulación de datos y fechas.

### 2. Clase `SystemMonitor`
- **Constructor (`__init__`)**:  
  Inicializa las variables, establece la ruta del CSV, el intervalo de captura y prepara la lista donde se almacenarán las métricas.
  
- **Método `log_metrics`**:  
  Ejecuta un bucle mientras el monitor esté activo, capturando:
  - Uso de CPU (total y por núcleo)
  - Uso de memoria en MB
  - Uso de disco (bytes leídos/escritos convertidos a MB)
  - Uso de red (calculado como tasa en MB/s)
  
  Los datos se agregan a la lista `metrics` utilizando un `lock` para evitar condiciones de carrera.

- **Métodos `start` y `stop`**:  
  - `start`: Inicia el monitoreo en un hilo separado.
  - `stop`: Detiene el monitoreo, espera a que el hilo termine y exporta las métricas a un archivo CSV.

### 3. Ejemplo de Uso
El bloque `if __name__ == "__main__":` muestra cómo:
- Crear el directorio para los logs.
- Instanciar el monitor.
- Iniciar el monitoreo y ejecutar un proceso simulado.
- Detener el monitor y exportar los datos.

## Ejemplo de Ejecución
```python
if __name__ == "__main__":
    LOG_FOLDER = r"C:\ruta\al\directorio\logs"
    os.makedirs(LOG_FOLDER, exist_ok=True)
    csv_path = os.path.join(LOG_FOLDER, f"system_metrics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")

    monitor = SystemMonitor(csv_path=csv_path, interval=2.0)
    monitor.start()

    try:
        for i in range(10):
            print(f"Procesando iteración {i+1}...")
            time.sleep(2)
    finally:
        monitor.stop()
        print(f"Monitoreo finalizado. CSV exportado en:\n{csv_path}")
