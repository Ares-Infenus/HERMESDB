--Inicialmente, establecí conexión con la base de datos predeterminada de Oracle. Posteriormente, me conecté al PDB denominado xepb1,
--donde procedí a crear un usuario llamado HERMES_DB. Después de crear el usuario, asigné los permisos necesarios. Para garantizar pr-
--ecisión y evitar errores, opté por asignar los privilegios manualmente a través de la ventana de privilegios, uno por uno. Esta deci-
--sión se tomó debido a los múltiples inconvenientes que experimenté al intentar realizar esta tarea mediante SQL*Plus, ya que la herr-
--amienta presentaba problemas y carecía de los permisos requeridos por razones desconocidas.

--Estos son los privilegios que le asigne:
-- Permisos básicos de conexión y recursos (ya incluidos en el script)
GRANT CONNECT, RESOURCE TO HERMESDB;

-- Permisos adicionales necesarios
GRANT CREATE VIEW TO HERMESDB;
GRANT CREATE MATERIALIZED VIEW TO HERMESDB;
GRANT CREATE TRIGGER TO HERMESDB;
GRANT CREATE TABLE TO HERMESDB;
GRANT CREATE SEQUENCE TO HERMESDB;
GRANT CREATE PROCEDURE TO HERMESDB;
GRANT CREATE ANY INDEX TO HERMESDB;
GRANT ALTER ANY INDEX TO HERMESDB;
GRANT CREATE SESSION TO HERMESDB;
GRANT UNLIMITED TABLESPACE TO HERMESDB;

-- Para las vistas materializadas
GRANT CREATE ANY MATERIALIZED VIEW TO HERMESDB;
GRANT ALTER ANY MATERIALIZED VIEW TO HERMESDB;
GRANT DROP ANY MATERIALIZED VIEW TO HERMESDB;
GRANT QUERY REWRITE TO HERMESDB;
GRANT GLOBAL QUERY REWRITE TO HERMESDB;

-- Para los logs de las vistas materializadas
GRANT CREATE MATERIALIZED VIEW LOG TO HERMESDB;
GRANT ALTER ANY MATERIALIZED VIEW LOG TO HERMESDB;
GRANT DROP ANY MATERIALIZED VIEW LOG TO HERMESDB;

-- Permisos adicionales recomendados
GRANT DEBUG CONNECT SESSION TO HERMESDB;
GRANT DEBUG ANY PROCEDURE TO HERMESDB;
GRANT SELECT ANY TABLE TO HERMESDB;
GRANT INSERT ANY TABLE TO HERMESDB;
GRANT UPDATE ANY TABLE TO HERMESDB;
GRANT DELETE ANY TABLE TO HERMESDB;


--Una vez asignados los permisos de forma manual mediante la interfaz gráfica de SQL Developer, logré compilar exitosamente el código para la cre-
--ación de tablas, índices y triggers de mi base de datos. Cabe mencionar que primero establecí conexión con mi usuario, y a partir de ahí comenc-
--é con la creación de las tablas. A continuación, presento el código utilizado:


-- Creación de tablas
CREATE TABLE market (
    market_id INTEGER PRIMARY KEY,
    market_name VARCHAR2(255) UNIQUE NOT NULL,
    description CLOB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
);

CREATE TABLE asset_metadata (
    metadata_id INTEGER PRIMARY KEY,
    sector VARCHAR2(100) NOT NULL,
    instrument_type VARCHAR2(50) NOT NULL,
    country VARCHAR2(50) NOT NULL,
    description CLOB
);

CREATE TABLE assets (
    assets_id INTEGER PRIMARY KEY,
    assets_name VARCHAR2(255) UNIQUE NOT NULL,
    market_id INTEGER NOT NULL,
    metadata_id INTEGER NOT NULL,
    is_active CHAR(1) CHECK (is_active IN ('Y', 'N')) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT fk_market FOREIGN KEY (market_id) REFERENCES HERMESDB.market(market_id) ON DELETE CASCADE,
    CONSTRAINT fk_metadata FOREIGN KEY (metadata_id) REFERENCES HERMESDB.asset_metadata(metadata_id) ON DELETE CASCADE
);

CREATE TABLE timeframe (
    timeframe_id INTEGER PRIMARY KEY,
    timeframe_name VARCHAR2(255) UNIQUE NOT NULL,
    minutes_interval INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
);

CREATE TABLE data_sources (
    source_id INTEGER PRIMARY KEY,
    source_name VARCHAR2(100) NOT NULL,
    description CLOB
);

CREATE TABLE technical_indicators (
    indicator_id INTEGER PRIMARY KEY,
    indicator_name VARCHAR2(50) NOT NULL,
    description CLOB,
    calculation_method CLOB,
    category VARCHAR2(50),
    is_composite CHAR(1) DEFAULT 'N' CHECK (is_composite IN ('Y', 'N'))
);

CREATE TABLE market_data (
    spread FLOAT CHECK (spread >= 0) NOT NULL,
    tick_volume FLOAT CHECK (tick_volume >= 0) NOT NULL
    data_id INTEGER PRIMARY KEY,
    assets_id INTEGER NOT NULL,
    timeframe_id INTEGER NOT NULL,
    price_type VARCHAR2(3) CHECK (price_type IN ('BID', 'ASK', 'MID')) NOT NULL,
    date_recorded TIMESTAMP NOT NULL,
    open FLOAT CHECK (open > 0) NOT NULL,
    high FLOAT NOT NULL,
    low FLOAT NOT NULL,
    close FLOAT CHECK (close > 0) NOT NULL,
    volume FLOAT CHECK (volume >= 0) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT fk_assets_new FOREIGN KEY (assets_id) REFERENCES assets(assets_id) ON DELETE CASCADE,
    CONSTRAINT fk_timeframe FOREIGN KEY (timeframe_id) REFERENCES timeframe(timeframe_id) ON DELETE CASCADE
)
PARTITION BY RANGE (date_recorded) (
    PARTITION p0 VALUES LESS THAN (TO_DATE('2000-01-01', 'YYYY-MM-DD')),
    PARTITION p1 VALUES LESS THAN (TO_DATE('2001-01-01', 'YYYY-MM-DD')),
    PARTITION p2 VALUES LESS THAN (TO_DATE('2002-01-01', 'YYYY-MM-DD')),
    PARTITION p3 VALUES LESS THAN (TO_DATE('2003-01-01', 'YYYY-MM-DD')),
    PARTITION p4 VALUES LESS THAN (TO_DATE('2004-01-01', 'YYYY-MM-DD')),
    PARTITION p5 VALUES LESS THAN (TO_DATE('2005-01-01', 'YYYY-MM-DD')),
    PARTITION p6 VALUES LESS THAN (TO_DATE('2006-01-01', 'YYYY-MM-DD')),
    PARTITION p7 VALUES LESS THAN (TO_DATE('2007-01-01', 'YYYY-MM-DD')),
    PARTITION p8 VALUES LESS THAN (TO_DATE('2008-01-01', 'YYYY-MM-DD')),
    PARTITION p9 VALUES LESS THAN (TO_DATE('2009-01-01', 'YYYY-MM-DD')),
    PARTITION p10 VALUES LESS THAN (TO_DATE('2010-01-01', 'YYYY-MM-DD')),
    PARTITION p11 VALUES LESS THAN (TO_DATE('2011-01-01', 'YYYY-MM-DD')),
    PARTITION p12 VALUES LESS THAN (TO_DATE('2012-01-01', 'YYYY-MM-DD')),
    PARTITION p13 VALUES LESS THAN (TO_DATE('2013-01-01', 'YYYY-MM-DD')),
    PARTITION p14 VALUES LESS THAN (TO_DATE('2014-01-01', 'YYYY-MM-DD')),
    PARTITION p15 VALUES LESS THAN (TO_DATE('2015-01-01', 'YYYY-MM-DD')),
    PARTITION p16 VALUES LESS THAN (TO_DATE('2016-01-01', 'YYYY-MM-DD')),
    PARTITION p17 VALUES LESS THAN (TO_DATE('2017-01-01', 'YYYY-MM-DD')),
    PARTITION p18 VALUES LESS THAN (TO_DATE('2018-01-01', 'YYYY-MM-DD')),
    PARTITION p19 VALUES LESS THAN (TO_DATE('2019-01-01', 'YYYY-MM-DD')),
    PARTITION p20 VALUES LESS THAN (TO_DATE('2020-01-01', 'YYYY-MM-DD')),
    PARTITION p21 VALUES LESS THAN (TO_DATE('2021-01-01', 'YYYY-MM-DD')),
    PARTITION p22 VALUES LESS THAN (TO_DATE('2022-01-01', 'YYYY-MM-DD')),
    PARTITION p23 VALUES LESS THAN (TO_DATE('2023-01-01', 'YYYY-MM-DD')),
    PARTITION p24 VALUES LESS THAN (TO_DATE('2024-01-01', 'YYYY-MM-DD')),
    PARTITION p25 VALUES LESS THAN (TO_DATE('2025-01-01', 'YYYY-MM-DD')),
    PARTITION p26 VALUES LESS THAN (TO_DATE('2026-01-01', 'YYYY-MM-DD'))

);

CREATE OR REPLACE TRIGGER trg_check_values
BEFORE INSERT OR UPDATE ON market_data
FOR EACH ROW
BEGIN
    IF :NEW.high < :NEW.open OR :NEW.high < :NEW.close THEN
        RAISE_APPLICATION_ERROR(-20001, 'El valor de "high" debe ser mayor o igual a "open" y "close"');
    END IF;

    IF :NEW.low > :NEW.open OR :NEW.low > :NEW.close THEN
        RAISE_APPLICATION_ERROR(-20002, 'El valor de "low" debe ser menor o igual a "open" y "close"');
    END IF;
    -- Validar que "spread" sea mayor o igual a 0
    IF :NEW.spread < 0 THEN
        RAISE_APPLICATION_ERROR(-20003, 'El valor de "spread" debe ser mayor o igual a 0');
    END IF;

    -- Validar que "tick_volume" sea mayor o igual a 0
    IF :NEW.tick_volume < 0 THEN
        RAISE_APPLICATION_ERROR(-20004, 'El valor de "tick_volume" debe ser mayor o igual a 0');
    END IF;
END;
/

CREATE TABLE operational_costs (
    cost_id INTEGER PRIMARY KEY,
    assets_id INTEGER NOT NULL,
    margin FLOAT NOT NULL CHECK (margin >= 0),
    contract_size FLOAT NOT NULL CHECK (contract_size > 0),
    contract_currency VARCHAR2(10) NOT NULL,
    spread FLOAT NOT NULL CHECK (spread >= 0),
    swap_long FLOAT NOT NULL,
    swap_short FLOAT NOT NULL,
    commission_per_order FLOAT NOT NULL CHECK (commission_per_order >= 0),
    date_recorded TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL, -- Agregado para trackear cambios
    CONSTRAINT fk_assets_costs FOREIGN KEY (assets_id) REFERENCES assets(assets_id) ON DELETE CASCADE
);

-- Índices para mejorar el rendimiento
CREATE INDEX idx_operational_costs_assets ON operational_costs (assets_id);


CREATE TABLE indicator_parameters (
    param_id INTEGER PRIMARY KEY,
    indicator_id INTEGER NOT NULL,
    param_name VARCHAR2(100) NOT NULL,
    param_value VARCHAR2(100) NOT NULL,
    UNIQUE (indicator_id, param_name, param_value),
    CONSTRAINT fk_indicator_param FOREIGN KEY (indicator_id) REFERENCES technical_indicators(indicator_id) ON DELETE CASCADE
);

CREATE TABLE calculated_indicators (
    calc_id INTEGER PRIMARY KEY,
    assets_id INTEGER NOT NULL,
    timeframe_id INTEGER NOT NULL,
    indicator_id INTEGER NOT NULL,
    param_id INTEGER NOT NULL,
    value FLOAT NOT NULL,
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    date_recorded TIMESTAMP NOT NULL,
    CONSTRAINT fk_assets FOREIGN KEY (assets_id) REFERENCES assets(assets_id) ON DELETE CASCADE,
    CONSTRAINT fk_timeframe FOREIGN KEY (timeframe_id) REFERENCES timeframe(timeframe_id) ON DELETE CASCADE,
    CONSTRAINT fk_indicator FOREIGN KEY (indicator_id) REFERENCES technical_indicators(indicator_id) ON DELETE CASCADE,
    CONSTRAINT fk_param FOREIGN KEY (param_id) REFERENCES indicator_parameters(param_id) ON DELETE CASCADE
)
PARTITION BY RANGE (date_recorded) (
     PARTITION i_0 VALUES LESS THAN (TO_DATE('2000-01-01', 'YYYY-MM-DD')),
    PARTITION i_1 VALUES LESS THAN (TO_DATE('2001-01-01', 'YYYY-MM-DD')),
    PARTITION i_2 VALUES LESS THAN (TO_DATE('2002-01-01', 'YYYY-MM-DD')),
    PARTITION i_3 VALUES LESS THAN (TO_DATE('2003-01-01', 'YYYY-MM-DD')),
    PARTITION i_4 VALUES LESS THAN (TO_DATE('2004-01-01', 'YYYY-MM-DD')),
    PARTITION i_5 VALUES LESS THAN (TO_DATE('2005-01-01', 'YYYY-MM-DD')),
    PARTITION i_6 VALUES LESS THAN (TO_DATE('2006-01-01', 'YYYY-MM-DD')),
    PARTITION i_7 VALUES LESS THAN (TO_DATE('2007-01-01', 'YYYY-MM-DD')),
    PARTITION i_8 VALUES LESS THAN (TO_DATE('2008-01-01', 'YYYY-MM-DD')),
    PARTITION i_9 VALUES LESS THAN (TO_DATE('2009-01-01', 'YYYY-MM-DD')),
    PARTITION i_10 VALUES LESS THAN (TO_DATE('2010-01-01', 'YYYY-MM-DD')),
    PARTITION i_11 VALUES LESS THAN (TO_DATE('2011-01-01', 'YYYY-MM-DD')),
    PARTITION i_12 VALUES LESS THAN (TO_DATE('2012-01-01', 'YYYY-MM-DD')),
    PARTITION i_13 VALUES LESS THAN (TO_DATE('2013-01-01', 'YYYY-MM-DD')),
    PARTITION i_14 VALUES LESS THAN (TO_DATE('2014-01-01', 'YYYY-MM-DD')),
    PARTITION i_15 VALUES LESS THAN (TO_DATE('2015-01-01', 'YYYY-MM-DD')),
    PARTITION i_16 VALUES LESS THAN (TO_DATE('2016-01-01', 'YYYY-MM-DD')),
    PARTITION i_17 VALUES LESS THAN (TO_DATE('2017-01-01', 'YYYY-MM-DD')),
    PARTITION i_18 VALUES LESS THAN (TO_DATE('2018-01-01', 'YYYY-MM-DD')),
    PARTITION i_19 VALUES LESS THAN (TO_DATE('2019-01-01', 'YYYY-MM-DD')),
    PARTITION i_20 VALUES LESS THAN (TO_DATE('2020-01-01', 'YYYY-MM-DD')),
    PARTITION i_21 VALUES LESS THAN (TO_DATE('2021-01-01', 'YYYY-MM-DD')),
    PARTITION i_22 VALUES LESS THAN (TO_DATE('2022-01-01', 'YYYY-MM-DD')),
    PARTITION i_23 VALUES LESS THAN (TO_DATE('2023-01-01', 'YYYY-MM-DD')),
    PARTITION i_24 VALUES LESS THAN (TO_DATE('2024-01-01', 'YYYY-MM-DD')),
    PARTITION i_25 VALUES LESS THAN (TO_DATE('2025-01-01', 'YYYY-MM-DD'))
);


CREATE INDEX idx_calc_indicators_assets ON calculated_indicators (assets_id, timeframe_id, indicator_id, param_id, date_recorded);
CREATE INDEX idx_market_data_assets_date ON market_data (assets_id, date_recorded);
CREATE INDEX idx_market_data_timeframe_date ON market_data (timeframe_id, date_recorded);
CREATE INDEX idx_market_data_assets_timeframe_date ON market_data (assets_id, timeframe_id, date_recorded);

-- Vista materializada para resúmenes diarios
CREATE MATERIALIZED VIEW mv_daily_summary
BUILD IMMEDIATE
REFRESH COMPLETE ON DEMAND
AS
SELECT
    assets_id,
    timeframe_id,
    source_id,
    TRUNC(date_recorded) AS day,
    AVG(open) AS avg_open,
    AVG(high) AS avg_high,
    AVG(low) AS avg_low,
    AVG(close) AS avg_close,
    SUM(volume) AS total_volume
FROM
    market_data
GROUP BY
    assets_id,
    timeframe_id,
    source_id,
    TRUNC(date_recorded);

CREATE MATERIALIZED VIEW LOG ON market_data
WITH ROWID, PRIMARY KEY (assets_id, timeframe_id, source_id, date_recorded)
INCLUDING NEW VALUES;

-- Compresión de columnas en tablas históricas
ALTER TABLE market_data COMPRESS FOR OLTP;
ALTER TABLE calculated_indicators COMPRESS FOR OLTP;

-- Auditoría de cambios
CREATE OR REPLACE TRIGGER trg_market_data_update
BEFORE UPDATE ON market_data
FOR EACH ROW
BEGIN
    :NEW.updated_at := CURRENT_TIMESTAMP;
END;
/

CREATE OR REPLACE TRIGGER trg_assets_update
BEFORE UPDATE ON assets
FOR EACH ROW
BEGIN
    :NEW.updated_at := CURRENT_TIMESTAMP;
END;
/
