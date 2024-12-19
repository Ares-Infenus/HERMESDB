# HERMESDB
Esta base de datos está diseñada para almacenar y gestionar de manera eficiente los datos históricos de activos financieros utilizados en trading. Su principal propósito es facilitar el análisis, modelado, simulación y toma de decisiones en operaciones financieras, tanto para traders como para analistas.

Estructura de la Carpeta
plaintext
Copy code
HERMESDB/
├── data/                  # Datos crudos y preprocesados
│   ├── raw/              # Datos crudos importados
│   ├── preprocessed/     # Datos preprocesados
│   └── cleaned/          # Datos limpios listos para análisis
├── notebooks/             # Jupyter Notebooks para informes
│   ├── analysis.ipynb    # Análisis de datos
│   ├── visualization.ipynb # Gráficos y resultados
│   └── report.ipynb      # Informe consolidado
├── src/                   # Código fuente modular
│   ├── __init__.py       # Archivo de inicialización (puede estar vacío)
│   ├── data_ingestion.py # Funciones para cargar datos
│   ├── preprocessing.py  # Limpieza y transformación de datos
│   ├── visualization.py  # Funciones para generar gráficos
│   ├── analysis.py       # Análisis y modelos predictivos
│   ├── database.py       # Funciones para interactuar con la base de datos
│   └── utils.py          # Funciones auxiliares
├── tests/                 # Pruebas unitarias
│   ├── test_preprocessing.py
│   ├── test_analysis.py
│   └── test_database.py
├── output/                # Resultados generados
│   ├── figures/          # Gráficos exportados
│   ├── reports/          # Informes en PDF o HTML
│   └── database/         # Base de datos consolidada
├── env/                   # Entorno virtual de Python
├── requirements.txt       # Dependencias del proyecto
├── README.md              # Documentación inicial del proyecto
└── .gitignore             # Archivos y carpetas a ignorar por Git
Explicación de los Componentes
data/:

Estructura clara para los datos en diferentes estados (crudos, preprocesados, limpios).
Ayuda a mantener el control de versiones y etapas de procesamiento.
notebooks/:

Informes interactivos: Cada Notebook aborda una etapa específica:
analysis.ipynb: Exploración de datos y análisis inicial.
visualization.ipynb: Gráficos y visualizaciones avanzadas.
report.ipynb: Informe final consolidado para presentar.
Importan funciones de los módulos en src/ para mantener la lógica separada.
src/:

Módulos organizados:
data_ingestion.py: Carga y validación de datos.
preprocessing.py: Limpieza y transformación de datos.
visualization.py: Funciones para gráficos interactivos y exportación.
analysis.py: Modelos predictivos, métricas y simulaciones.
database.py: Interacción con la base de datos (inserción, consultas).
utils.py: Funciones reutilizables (p. ej., manejo de fechas o formatos).
tests/:

Pruebas unitarias para garantizar la calidad y estabilidad del código.
output/:

Carpeta central para resultados generados:
Gráficos, informes y bases de datos finales.
requirements.txt:

Lista de dependencias para que el entorno sea reproducible.
Entorno virtual (env/):

Mantiene las dependencias aisladas del sistema.
