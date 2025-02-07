// Hay que aclarar que este codigo es el diseño y esta echo para correr en dbdiagram.io
Table Mercados {
  mercado_id integer [pk, increment]
  nombre varchar(50) [not null, unique]
  tipo varchar(20) [not null, note: 'Tipo: Normal/Sintético/ETF']
  padre_id integer [note: 'Jerarquía']
}

Table Sectores {
  sector_id integer [pk, increment]
  nombre varchar(50) [not null]
  mercado_id integer [not null]
  region varchar(50) [note: 'Región: EEUU/Europa/Asia/Latam']
  indexes {
    (nombre, mercado_id, region) [name: 'unique_sector', unique]
  }
}

Table Activos {
  activo_id integer [pk, increment]
  simbolo varchar(15) [not null, unique]
  nombre varchar(100) [not null]
  mercado_id integer [not null]
  sector_id integer
  pip_size numeric [note: 'Ej: 0.0001 para EURUSD']
  contrato_size numeric [default: 100000]
  indexes {
    mercado_id [name: 'idx_activo_mercado']
  }
}

Table Brokers {
  broker_id integer [pk, increment]
  nombre varchar(50) [not null, unique]
  mercado_base varchar(50) [note: 'Ej: NASDAQ, LME']
  tipo_ejecucion varchar(20) [note: 'STP/ECN/Market Maker']
  indexes {
    mercado_base [name: 'idx_broker_mercado']
  }
}

Table Datos_Historicos {
  registro_id bigint [pk, increment]
  activo_id integer [not null]
  broker_id integer [not null]
  mercado_id integer [not null]
  timestamp timestamptz [not null]
  timeframe varchar(3) [note: '1m/5m/15m/30m/1h/4h/1d/1w/1M']
  
  bid_open numeric [not null]
  bid_high numeric [not null]
  bid_low numeric [not null]
  bid_close numeric [not null]
  
  ask_open numeric [not null]
  ask_high numeric [not null]
  ask_low numeric [not null]
  ask_close numeric [not null]
  
  volumen_contratos bigint [not null]
  spread_promedio numeric [not null]
  
  indexes {
    (activo_id, broker_id, timestamp) [unique, name: 'idx_main_query']
    timeframe [name: 'idx_timeframe']
    timestamp [name: 'idx_timestamp']
  }
}

Table Costos_Operacionales {
  costo_id bigint [pk, increment]
  activo_id integer [not null]
  broker_id integer [not null]
  fecha date [not null]
  
  swap_largo numeric [not null]
  swap_corto numeric [not null]
  comision_por_lote numeric [note: 'USD por lote']
  margen_requerido numeric [not null]
  
  indexes {
    (activo_id, broker_id, fecha) [unique, name: 'unique_costo']
    fecha [name: 'idx_fecha']
  }
}

Ref: Mercados.mercado_id < Sectores.mercado_id
Ref: Mercados.mercado_id < Activos.mercado_id
Ref: Activos.sector_id > Sectores.sector_id
Ref: Datos_Historicos.activo_id > Activos.activo_id
Ref: Datos_Historicos.broker_id > Brokers.broker_id
Ref: Datos_Historicos.mercado_id > Mercados.mercado_id
Ref: Costos_Operacionales.activo_id > Activos.activo_id
Ref: Costos_Operacionales.broker_id > Brokers.broker_id