-- Activar la extensión TimescaleDB (se omite si ya existe)
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- Usar IF NOT EXISTS para evitar errores en la creación de tablas
CREATE TABLE IF NOT EXISTS runs (
    id SERIAL PRIMARY KEY,
    start_time TIMESTAMPTZ,
    end_time TIMESTAMPTZ,
    environment JSONB,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS function_profiles (
    id SERIAL PRIMARY KEY,
    run_id INT REFERENCES runs(id),
    function_name TEXT,
    total_time NUMERIC,
    call_count INT,
    avg_time NUMERIC,
    cpu_time NUMERIC,
    wait_time NUMERIC,
    memory_usage NUMERIC
);

CREATE TABLE IF NOT EXISTS line_profiles (
    id SERIAL PRIMARY KEY,
    function_profile_id INT REFERENCES function_profiles(id),
    line_number INT,
    time NUMERIC,
    memory_usage NUMERIC,
    bytes_read NUMERIC,
    bytes_written NUMERIC
);

CREATE TABLE IF NOT EXISTS benchmarks (
    id SERIAL PRIMARY KEY,
    run_id INT REFERENCES runs(id),
    snippet TEXT,
    total_time NUMERIC,
    avg_time NUMERIC,
    std_dev_time NUMERIC
);

-- Se modifica la tabla system_metrics para incluir timestamp en la clave primaria y usar TIMESTAMPTZ
CREATE TABLE IF NOT EXISTS system_metrics (
    id SERIAL,
    run_id INT REFERENCES runs(id),
    timestamp TIMESTAMPTZ NOT NULL,
    cpu_usage NUMERIC,
    memory_usage NUMERIC,
    disk_usage NUMERIC,
    network_usage NUMERIC,
    cpu_usage_per_core JSONB,
    gpu_usage NUMERIC,
    PRIMARY KEY (id, timestamp)
);

-- Se modifica la tabla events para incluir timestamp en la clave primaria y usar TIMESTAMPTZ
CREATE TABLE IF NOT EXISTS events (
    id SERIAL,
    run_id INT REFERENCES runs(id),
    timestamp TIMESTAMPTZ NOT NULL,
    event_type TEXT,
    message TEXT,
    PRIMARY KEY (id, timestamp)
);

-- Convertir system_metrics en hypertable
SELECT create_hypertable('system_metrics', 'timestamp', if_not_exists => TRUE);

-- Convertir events en hypertable (opcional)
SELECT create_hypertable('events', 'timestamp', if_not_exists => TRUE);
