-----------------------------------------------------------
-- Cargar la extensión TimescaleDB (si aún no está instalada)
CREATE EXTENSION IF NOT EXISTS timescaledb;

-----------------------------------------------------------
-- Tabla Mercados
CREATE TABLE Mercados (
  mercado_id SERIAL PRIMARY KEY,
  nombre VARCHAR(50) NOT NULL UNIQUE,
  tipo VARCHAR(20) NOT NULL -- Tipo: Normal/Sintético/ETF
);

-----------------------------------------------------------
-- Tabla Sectores
CREATE TABLE Sectores (
  sector_id SERIAL PRIMARY KEY,
  nombre VARCHAR(50) NOT NULL,
  mercado_id INTEGER NOT NULL,
  CONSTRAINT unique_sector UNIQUE (nombre, mercado_id)
);

-----------------------------------------------------------
-- Tabla Activos
CREATE TABLE Activos (
  activo_id SERIAL PRIMARY KEY,
  simbolo VARCHAR(15) NOT NULL UNIQUE,
  nombre VARCHAR(100) NOT NULL,
  mercado_id INTEGER NOT NULL,
  sector_id INTEGER,
  contrato_size NUMERIC DEFAULT 100000
);

CREATE INDEX idx_activo_mercado ON Activos(mercado_id);

-----------------------------------------------------------
-- Tabla Brokers
CREATE TABLE Brokers (
  broker_id SERIAL PRIMARY KEY,
  nombre VARCHAR(50) NOT NULL UNIQUE,
  mercado_base VARCHAR(50), -- Ej: NASDAQ, LME
  tipo_ejecucion VARCHAR(20)  -- STP/ECN/Market Maker
);

CREATE INDEX idx_broker_mercado ON Brokers(mercado_base);

-----------------------------------------------------------
-- Tabla Datos_Historicos (a convertir en hypertable)
-- Se define la tabla sin particionamiento manual; TimescaleDB la gestionará.
-----------------------------------------------------------
CREATE TABLE Datos_Historicos (
  registro_id BIGSERIAL NOT NULL,
  activo_id INTEGER NOT NULL,
  broker_id INTEGER NOT NULL,
  mercado_id INTEGER NOT NULL,
  timestamp TIMESTAMPTZ NOT NULL,
  timeframe VARCHAR(3) NOT NULL, -- Ej: 1m/5m/15m/30m/1h/4h/1d/1w/1M

  bid_open NUMERIC NOT NULL,
  bid_high NUMERIC NOT NULL,
  bid_low NUMERIC NOT NULL,
  bid_close NUMERIC NOT NULL,

  ask_open NUMERIC NOT NULL,
  ask_high NUMERIC NOT NULL,
  ask_low NUMERIC NOT NULL,
  ask_close NUMERIC NOT NULL,

  volumen_contratos BIGINT NOT NULL,
  spread_promedio NUMERIC NOT NULL,
  
  CONSTRAINT pk_datos_historicos PRIMARY KEY (registro_id, timestamp),
  CONSTRAINT idx_main_query UNIQUE (activo_id, broker_id, timestamp, timeframe)
);

-- Índices adicionales en la tabla de series temporales
CREATE INDEX idx_timeframe ON Datos_Historicos(timeframe);
CREATE INDEX idx_timestamp ON Datos_Historicos(timestamp);

-----------------------------------------------------------
-- Convertir Datos_Historicos en un Hypertable con chunk trimestral
-----------------------------------------------------------
SELECT create_hypertable('Datos_Historicos', 'timestamp', chunk_time_interval => interval '3 months', migrate_data => true);

-----------------------------------------------------------
-- Aplicar política de compresión para datos mayores a 90 días
-----------------------------------------------------------
ALTER TABLE Datos_Historicos SET (timescaledb.compress, timescaledb.compress_segmentby = 'activo_id');
SELECT add_compression_policy('Datos_Historicos', INTERVAL '90 days');

-----------------------------------------------------------
-- Tabla Costos_Operacionales
-----------------------------------------------------------
CREATE TABLE Costos_Operacionales (
  costo_id BIGSERIAL PRIMARY KEY,
  activo_id INTEGER NOT NULL,
  broker_id INTEGER NOT NULL,
  fecha DATE NOT NULL,
  
  swap_largo NUMERIC NOT NULL,
  swap_corto NUMERIC NOT NULL,
  comision_por_lote NUMERIC, -- USD por lote
  margen_requerido NUMERIC NOT NULL,
  
  CONSTRAINT unique_costo UNIQUE (activo_id, broker_id, fecha)
);

CREATE INDEX idx_fecha ON Costos_Operacionales(fecha);

-----------------------------------------------------------
-- Relaciones (FOREIGN KEYS)
-----------------------------------------------------------
-- Sectores.mercado_id referencia a Mercados
ALTER TABLE Sectores
  ADD CONSTRAINT fk_sectores_mercados FOREIGN KEY (mercado_id)
  REFERENCES Mercados(mercado_id);

-- Activos.mercado_id referencia a Mercados
ALTER TABLE Activos
  ADD CONSTRAINT fk_activos_mercados FOREIGN KEY (mercado_id)
  REFERENCES Mercados(mercado_id);

-- Activos.sector_id referencia a Sectores
ALTER TABLE Activos
  ADD CONSTRAINT fk_activos_sectores FOREIGN KEY (sector_id)
  REFERENCES Sectores(sector_id);

-- Datos_Historicos.activo_id referencia a Activos
ALTER TABLE Datos_Historicos
  ADD CONSTRAINT fk_dh_activos FOREIGN KEY (activo_id)
  REFERENCES Activos(activo_id);

-- Datos_Historicos.broker_id referencia a Brokers
ALTER TABLE Datos_Historicos
  ADD CONSTRAINT fk_dh_brokers FOREIGN KEY (broker_id)
  REFERENCES Brokers(broker_id);

-- Datos_Historicos.mercado_id referencia a Mercados
ALTER TABLE Datos_Historicos
  ADD CONSTRAINT fk_dh_mercados FOREIGN KEY (mercado_id)
  REFERENCES Mercados(mercado_id);

-- Costos_Operacionales.activo_id referencia a Activos
ALTER TABLE Costos_Operacionales
  ADD CONSTRAINT fk_costos_activos FOREIGN KEY (activo_id)
  REFERENCES Activos(activo_id);

-- Costos_Operacionales.broker_id referencia a Brokers
ALTER TABLE Costos_Operacionales
  ADD CONSTRAINT fk_costos_brokers FOREIGN KEY (broker_id)
  REFERENCES Brokers(broker_id);
