-- Tabla runs: Registra cada ejecución del benchmark
CREATE TABLE runs (
    id SERIAL PRIMARY KEY,                -- Identificador único de la ejecución
    start_time TIMESTAMP,                 -- Fecha y hora de inicio de la ejecución
    end_time TIMESTAMP,                   -- Fecha y hora de finalización, para calcular la duración total (end_time - start_time)
    environment JSONB,                    -- Información del entorno en formato JSON (versión de Python, SO, hardware, etc.)
    notes TEXT                            -- Observaciones o comentarios opcionales sobre la ejecución
);

-- Tabla function_profiles: Almacena perfiles de funciones ejecutadas en una ejecución específica
CREATE TABLE function_profiles (
    id SERIAL PRIMARY KEY,                -- Identificador único del perfil de función
    run_id INT REFERENCES runs(id),       -- Clave foránea que vincula este perfil con una ejecución en la tabla runs
    function_name TEXT,                   -- Nombre de la función perfilada (ej. "calculate_metrics")
    total_time NUMERIC,                   -- Tiempo total de ejecución de la función en segundos
    call_count INT,                       -- Número de veces que se llamó a la función
    avg_time NUMERIC,                     -- Tiempo promedio por llamada (total_time / call_count)
    cpu_time NUMERIC,                     -- Tiempo de CPU dedicado a cálculos en la función
    wait_time NUMERIC,                    -- Tiempo de espera por I/O, bloqueos u otros retrasos
    memory_usage NUMERIC                  -- Uso máximo de memoria en MB durante la ejecución de la función
);

-- Tabla line_profiles: Detalla el rendimiento por línea dentro de una función
CREATE TABLE line_profiles (
    id SERIAL PRIMARY KEY,                -- Identificador único del perfil de línea
    function_profile_id INT REFERENCES function_profiles(id), -- Vincula esta línea con un perfil de función
    line_number INT,                      -- Número de línea dentro del código de la función
    time NUMERIC,                         -- Tiempo de ejecución de esta línea en segundos
    memory_usage NUMERIC,                 -- Uso de memoria en MB atribuido a esta línea (si es medible)
    bytes_read NUMERIC,                   -- Bytes leídos en operaciones de I/O en esta línea
    bytes_written NUMERIC                 -- Bytes escritos en operaciones de I/O en esta línea
);

-- Tabla benchmarks: Guarda información sobre fragmentos de código evaluados
CREATE TABLE benchmarks (
    id SERIAL PRIMARY KEY,                -- Identificador único del benchmark
    run_id INT REFERENCES runs(id),       -- Vincula este benchmark con una ejecución en la tabla runs
    snippet TEXT,                         -- Código o referencia al fragmento evaluado
    total_time NUMERIC,                   -- Tiempo total de ejecución del fragmento en segundos
    avg_time NUMERIC,                     -- Tiempo promedio por iteración (si aplica)
    std_dev_time NUMERIC                  -- Desviación estándar del tiempo de ejecución, para medir variabilidad
);

-- Tabla system_metrics: Captura métricas del sistema durante una ejecución
CREATE TABLE system_metrics (
    id SERIAL PRIMARY KEY,                -- Identificador único de la métrica
    run_id INT REFERENCES runs(id),       -- Vincula esta métrica con una ejecución en la tabla runs
    timestamp TIMESTAMP,                  -- Momento exacto en que se capturó la métrica
    cpu_usage NUMERIC,                    -- Porcentaje de uso de CPU durante la ejecución
    memory_usage NUMERIC,                 -- Uso de memoria del sistema en MB o porcentaje
    disk_usage NUMERIC,                   -- Uso de disco en MB (lectura/escritura)
    network_usage NUMERIC,                -- Tráfico de red en MB/s
    cpu_usage_per_core JSONB,             -- Uso de CPU por núcleo en formato JSON (ej. [25.0, 50.0, 0.0])
    gpu_usage NUMERIC                     -- Porcentaje de uso de GPU, para sistemas con procesamiento gráfico
);

-- Tabla events: Registra eventos específicos como errores o advertencias
CREATE TABLE events (
    id SERIAL PRIMARY KEY,                -- Identificador único del evento
    run_id INT REFERENCES runs(id),       -- Vincula este evento con una ejecución en la tabla runs
    timestamp TIMESTAMP,                  -- Momento en que ocurrió el evento
    event_type TEXT,                      -- Tipo de evento (ej. "error", "warning", "info")
    message TEXT                          -- Descripción detallada del evento (ej. "Memoria insuficiente")
);

-- Convertir la tabla system_metrics en hypertable usando 'timestamp' como columna de tiempo
SELECT create_hypertable('system_metrics', 'timestamp');

-- (Opcional) Convertir la tabla events en hypertable si se espera un alto volumen de eventos
SELECT create_hypertable('events', 'timestamp');
