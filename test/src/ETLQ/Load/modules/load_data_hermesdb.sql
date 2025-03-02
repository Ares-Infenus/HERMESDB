-- Este script se encarga de importar datos desde archivos CSV a las tablas correspondientes de la base de datos.
-- La instrucción \copy se utiliza para leer datos de un archivo y copiarlos a una tabla.
-- Se especifica que los archivos están en formato CSV, que tienen una fila de encabezados y que se usa la coma (,) como separador.

-- Importa los datos del archivo "Table_Broker.csv" a la tabla "Brokers"
\copy Brokers FROM 'C:/Users/spinz/OneDrive/Documentos/Portafolio oficial/HERMESDB/HERMESDB/test/data/backup/Table_Broker.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',');

-- Importa los datos del archivo "Table_market.csv" a la tabla "Mercados"
\copy Mercados FROM 'C:/Users/spinz/OneDrive/Documentos/Portafolio oficial/HERMESDB/HERMESDB/test/data/backup/Table_market.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',');

-- Importa los datos del archivo "Table_Sector.csv" a la tabla "Sectores"
\copy Sectores FROM 'C:\Users\spinz\OneDrive\Documentos\Portafolio oficial\HERMESDB\HERMESDB\test\data\backup\Table_Sector.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',');

-- Importa los datos del archivo "Table_Assets.csv" a la tabla "Activos"
\copy Activos FROM 'C:\Users\spinz\OneDrive\Documentos\Portafolio oficial\HERMESDB\HERMESDB\test\data\backup\Table_Assets.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',');

-- Importa los datos del archivo "data_unificada.csv" a la tabla "Datos_Historicos"
\copy datos_historicos FROM 'C:\Users\spinz\OneDrive\Documentos\Portafolio oficial\HERMESDB\HERMESDB\test\data\backup\data_unificada.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',');