# Parámetros de conexión
$dbHost     = "localhost"
$dbPort     = "5432"
$dbUser     = "postgres"
$dbPassword = "lQHMdUMUnENmze7pxREMtPx1OOVKl3kGc5gTXH3SCyz1zxEpu8"
$dbName     = "perfomance_hermesdb"

# Comando SQL para crear la base de datos
$sqlCommand = "CREATE DATABASE $dbName;"

# Configurar la variable de entorno para la contraseña
$env:PGPASSWORD = $dbPassword

# Especifica la ruta completa a psql (descomentar y ajustar si es necesario)
$psqlPath = "C:\Program Files\PostgreSQL\17\bin\psql.exe"

# Crear la base de datos
Write-Host "Ejecutando comando: $psqlPath -h $dbHost -p $dbPort -U $dbUser -c `"$sqlCommand`""
& "$psqlPath" -h $dbHost -p $dbPort -U $dbUser -c "$sqlCommand"

# Ejecutar el script para crear las tablas y configuraciones en la base de datos HermesDB
$scriptPath = "C:\Users\spinz\OneDrive\Documentos\Portafolio oficial\HERMESDB\HERMESDB\test\src\PERFORMANCE\SCRIPTS_SQL\arquitecture_DB_performance.sql"
Write-Host "Ejecutando script de creación de tablas en la base de datos $dbName..."
& "$psqlPath" -h $dbHost -p $dbPort -U $dbUser -d $dbName -f $scriptPath