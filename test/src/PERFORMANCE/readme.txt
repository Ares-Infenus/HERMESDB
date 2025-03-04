-- Esta carpeta contiene el código para crear la base de datos que monitorizará el programa Herbesdb.
-- La base de datos, llamada "PERFORMANCE_HERMESDB", almacenará toda la información de monitoreo.

Archivos: 

- created_DB_performance_hermesdb -

El script de PowerShell define los parámetros de conexión (host, puerto, usuario, contraseña y nombre de la base de datos), configura la contraseña como variable de entorno para usarla en el comando psql, crea la base de datos mediante un comando SQL ejecutado a través del ejecutable psql, y finalmente ejecuta un script SQL que establece las tablas y configuraciones iniciales en la nueva base de datos.