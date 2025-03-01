
\copy Brokers FROM 'C:/Users/spinz/OneDrive/Documentos/Portafolio oficial/HERMESDB/HERMESDB/test/data/backup/Table_Broker.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',');

\copy Mercados FROM 'C:/Users/spinz/OneDrive/Documentos/Portafolio oficial/HERMESDB/HERMESDB/test/data/backup/Table_market.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',');

\copy Activos FROM 'C:\Users\spinz\OneDrive\Documentos\Portafolio oficial\HERMESDB\HERMESDB\test\data\backup\Table_Assets.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',');