# Documentación Técnica: Sistema de Base de Datos Financiera

## 1. Descripción General
Este sistema de base de datos está diseñado para gestionar datos financieros del mercado, con capacidad para manejar grandes volúmenes de información, optimizado para consultas de alto rendimiento y diseñado con un enfoque en la integridad y seguridad de los datos.

## 2. Arquitectura del Sistema

### 2.1 Estructura de Tablas Principales

#### Market (Mercados)
- **Propósito**: Almacena información sobre los diferentes mercados financieros.
- **Campos Principales**:
  - `market_id`: Identificador único del mercado
  - `market_name`: Nombre único del mercado
  - `description`: Descripción detallada del mercado
- **Consideraciones**: Tabla fundamental que sirve como punto de entrada para la jerarquía de datos.

#### Asset_Metadata (Metadatos de Activos)
- **Propósito**: Contiene información descriptiva sobre los tipos de activos.
- **Campos Principales**:
  - `metadata_id`: Identificador único de metadatos
  - `sector`: Sector económico
  - `instrument_type`: Tipo de instrumento financiero
  - `country`: País de origen
- **Uso**: Proporciona contexto y categorización para los activos financieros.

#### Assets (Activos)
- **Propósito**: Registro principal de todos los activos financieros.
- **Campos Principales**:
  - `assets_id`: Identificador único del activo
  - `assets_name`: Nombre único del activo
  - `market_id`: Referencia al mercado
  - `metadata_id`: Referencia a los metadatos
  - `is_active`: Estado actual del activo
- **Relaciones**:
  - Vinculado con Market (market_id)
  - Vinculado con Asset_Metadata (metadata_id)

#### Market_Data (Datos de Mercado)
- **Propósito**: Almacena datos históricos de precios y volúmenes.
- **Campos Principales**:
  - `data_id`: Identificador único del registro
  - `price_type`: Tipo de precio (BID/ASK/MID)
  - Datos OHLCV (Open, High, Low, Close, Volume)
- **Optimizaciones**:
  - Particionamiento por año en columna 'date'
  - Índices compuestos para consultas frecuentes
  - Compresión de columnas para datos históricos

## 3. Optimizaciones y Rendimiento

### 3.1 Índices Estratégicos
- **Índices Compuestos**:
  ```sql
  CREATE INDEX idx_market_data_asset_date ON market_data(assets_id, date);
  CREATE INDEX idx_market_data_timeframe_date ON market_data(timeframe_id, date);
  CREATE INDEX idx_market_data_asset_timeframe_date ON market_data(assets_id, timeframe_id, date);
  ```

### 3.2 Particionamiento
- Implementación de particiones por año en Market_Data
- Beneficios:
  - Mejor rendimiento en consultas por rango de fechas
  - Mantenimiento más eficiente
  - Mejora en la gestión de backups

### 3.3 Vistas Materializadas
```sql
CREATE MATERIALIZED VIEW mv_daily_summary AS
SELECT 
    assets_id,
    DATE_TRUNC('day', date) as trade_date,
    AVG(close) as avg_price,
    MAX(high) as max_price,
    MIN(low) as min_price,
    SUM(volume) as total_volume
FROM market_data
GROUP BY assets_id, DATE_TRUNC('day', date);
```

## 4. Integridad y Validación de Datos

### 4.1 Restricciones de Integridad
- **Claves Foráneas**: Implementadas con ON DELETE CASCADE y ON UPDATE CASCADE
- **Restricciones CHECK**:
  - Validación de precios positivos
  - Validación de relaciones HIGH/LOW
  - Validación de tipos de precio

### 4.2 Validaciones Implementadas
```sql
CONSTRAINT valid_price_type CHECK (price_type IN ('BID', 'ASK', 'MID')),
CONSTRAINT valid_price_values CHECK (
    open > 0 AND
    close > 0 AND
    high >= open AND
    high >= close AND
    low <= open AND
    low <= close AND
    volume >= 0
)
```

## 5. Seguridad y Respaldo

### 5.1 Plan de Respaldo
- Backups incrementales automatizados
- Replicación de datos en tiempo real
- Snapshots diarios de la base de datos

### 5.2 Auditoría
- Seguimiento automático de cambios mediante timestamps
- Registro de modificaciones en columnas created_at y updated_at

## 6. Mejores Prácticas de Uso

### 6.1 Consultas Optimizadas
```sql
-- Ejemplo de consulta optimizada para datos históricos
SELECT 
    a.assets_name,
    md.date,
    md.close
FROM market_data md
JOIN assets a ON md.assets_id = a.assets_id
WHERE md.date BETWEEN :start_date AND :end_date
    AND a.is_active = true
```

### 6.2 Mantenimiento
- Actualización regular de estadísticas
- Reconstrucción periódica de índices
- Refrescamiento de vistas materializadas

## 7. Consideraciones de Escalabilidad

### 7.1 Capacidad de Crecimiento
- Diseño preparado para grandes volúmenes de datos
- Estructura optimizada para consultas concurrentes
- Particionamiento preparado para expansión

### 7.2 Limitaciones y Recomendaciones
- Monitorear el crecimiento de las particiones
- Evaluar periódicamente el rendimiento de los índices
- Mantener actualizadas las estadísticas de la base de datos

## 8. Apéndice

### 8.1 Convenciones de Nombres
- Todas las tablas y columnas utilizan snake_case
- Nombres descriptivos y consistentes
- Prefijos y sufijos estandarizados

### 8.2 Diagrama de Relaciones
├── Market
│   ├── market_id (PK)
│   ├── market_name (UNIQUE)
│   ├── description
│   ├── created_at
│   └── updated_at
│
├── Asset_Metadata
│   ├── metadata_id (PK)
│   ├── sector
│   ├── instrument_type
│   ├── country
│   └── description
│
├── Assets
│   ├── assets_id (PK)
│   ├── assets_name (UNIQUE)
│   ├── market_id (FK -> Market)
│   ├── metadata_id (FK -> Asset_Metadata)
│   ├── is_active
│   ├── created_at
│   └── updated_at
│
├── Timeframe
│   ├── timeframe_id (PK)
│   ├── timeframe_name (UNIQUE)
│   ├── minutes_interval
│   └── created_at
│
├── Data_Sources
│   ├── source_id (PK)
│   ├── source_name
│   └── description
│
├── Technical_Indicators
│   ├── indicator_id (PK)
│   ├── indicator_name
│   ├── description
│   └── calculation_method
│
├── Market_Data
│   ├── data_id (PK)
│   ├── assets_id (FK -> Assets)
│   ├── timeframe_id (FK -> Timeframe)
│   ├── source_id (FK -> Data_Sources)
│   ├── price_type (CHECK: BID,ASK,MID)
│   ├── date
│   ├── open (CHECK > 0)
│   ├── high (CHECK >= open,close)
│   ├── low (CHECK <= open,close)
│   ├── close (CHECK > 0)
│   ├── volume (CHECK >= 0)
│   └── created_at
│
└── Calculated_Indicators
    ├── calc_id (PK)
    ├── data_id (FK -> Market_Data)
    ├── indicator_id (FK -> Technical_Indicators)
    ├── value
    └── calculated_at

