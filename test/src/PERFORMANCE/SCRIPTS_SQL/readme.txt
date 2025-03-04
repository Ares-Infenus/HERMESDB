--Esta carpeta contiene el código SQL necesario para crear la base de datos y cargar los datos.


Archivos:

- arquitecture_DB_performance -

El documento define la estructura de una base de datos para el monitoreo y análisis de rendimiento en ejecuciones de código. Contiene tablas que registran información sobre ejecuciones (runs), perfiles de funciones y líneas de código (function_profiles, line_profiles), pruebas de rendimiento (benchmarks), métricas del sistema (system_metrics) y eventos relevantes (events). Su objetivo es proporcionar un almacenamiento detallado de datos para evaluar y optimizar el desempeño del software.

Update:

Se identifica que las tablas con datos en serie temporal o inserciones continuas en grandes volúmenes son las que más se benefician de convertirse en hypertables. Por ello, se recomienda transformar system_metrics y, opcionalmente, events en hypertables, ya que ambas registran datos con un timestamp y se insertan de forma frecuente. Esto permite aprovechar las optimizaciones de TimescaleDB en consultas, compresión, retención y escalabilidad, mientras que otras tablas de metadatos no requieren esta transformación.