install.packages("progressr")
library(disk.frame)
library(dplyr)        # Operaciones tipo dplyr
library(data.table)   # Para data.table
library(doParallel)   # Para paralelización
library(foreach)      # Para ejecutar iteraciones en paralelo
library(progressr)    # Para mostrar la barra de progreso

# Configurar disk.frame usando todos los cores disponibles
n_cores <- parallel::detectCores()
setup_disk.frame(workers = n_cores)

# Convertir el archivo CSV a disk.frame (ajusta la ruta, separador y columnas según corresponda)
data_df <- csv_to_disk.frame(
  infile = "C:\\Users\\spinz\\OneDrive\\Documentos\\Portafolio oficial\\HERMESDB\\HERMESDB\\test\\data\\backup\\data_unificada.csv",
  outdir = "C:\\Users\\spinz\\OneDrive\\Documentos\\Portafolio oficial\\HERMESDB\\HERMESDB\\test\\data\\reports\\partition_report_r",
  header = TRUE,
  sep = ",",
  select = c("activo_id", "broker_id", "timeframe", "timestamp", "bid_close")
)

# Convertir la columna timestamp a Date y filtrar por el período 2023-2024
data_df <- data_df %>% 
  mutate(fecha = as.Date(timestamp)) %>%
  filter(fecha >= as.Date("2023-01-01") & fecha <= as.Date("2024-12-31"))

# Persistir el disk.frame filtrado
data_df <- rechunk(data_df, nchunks = nchunks(data_df),
                     outdir = "C:\\Users\\spinz\\OneDrive\\Documentos\\Portafolio oficial\\HERMESDB\\HERMESDB\\test\\data\\reports\\data_disk_filtrado",
                     overwrite = TRUE)

# Obtener las combinaciones únicas de activo y timeframe
activos_timeframes <- data_df %>% 
  select(activo_id, timeframe) %>%
  chunk_distinct() %>%
  collect()

# Lista de brokers a comparar (ejemplo con 5 brokers)
brokers <- c(1, 2, 3, 4, 5)

# Configurar el clúster paralelo
cl <- makeCluster(n_cores)
registerDoParallel(cl)

# Configurar los handlers de progressr (globales)
handlers(global = TRUE)

# Procesar cada combinación de activo y timeframe en paralelo
resultados <- with_progress({
  # Crear un progressor que abarque el total de iteraciones
  p <- progressor(along = 1:nrow(activos_timeframes))
  
  foreach(i = 1:nrow(activos_timeframes), .combine = rbind,
          .packages = c("disk.frame", "data.table", "dplyr", "progressr"),
          .export = "p") %dopar% {
    
    # Actualizar el progreso en cada iteración
    p(sprintf("Procesando combinación %d de %d", i, nrow(activos_timeframes)))
    
    act <- activos_timeframes$activo_id[i]
    tf  <- activos_timeframes$timeframe[i]
    
    # Extraer el subconjunto de datos para este activo y timeframe
    data_act_tf <- data_df %>% filter(activo_id == act, timeframe == tf) %>% collect()
    
    # Solo continuar si el activo tiene datos de todos los brokers
    if(uniqueN(data_act_tf$broker_id) == length(brokers)) {
      
      # Generar todas las combinaciones posibles (todos contra todos)
      combs <- combn(brokers, 2, simplify = FALSE)
      temp_results <- list()
      
      for(par in combs) {
        broker1 <- par[[1]]
        broker2 <- par[[2]]
        
        # Extraer datos para cada broker
        data_broker1 <- data_act_tf[broker_id == broker1]
        data_broker2 <- data_act_tf[broker_id == broker2]
        
        if(nrow(data_broker1) > 1 && nrow(data_broker2) > 1) {
          test <- t.test(data_broker1$bid_close, data_broker2$bid_close, var.equal = TRUE)
          dif_media <- test$estimate[1] - test$estimate[2]
          n_obs     <- paste(nrow(data_broker1), nrow(data_broker2), sep = "-")
          
          temp_results[[length(temp_results) + 1]] <- data.table(
            activo_id         = act,
            timeframe         = tf,
            broker_comparador = broker1,
            broker_test       = broker2,
            n_obs             = n_obs,
            dif_media         = dif_media,
            ci_lower          = test$conf.int[1],
            ci_upper          = test$conf.int[2],
            t_value           = as.numeric(test$statistic),
            p_value           = test$p.value
          )
        }
      }
      
      if(length(temp_results) > 0) {
        return(rbindlist(temp_results))
      }
    }
    
    # Retorna NULL si no se cumplen las condiciones
    return(NULL)
  }
})

# Detener el clúster paralelo
stopCluster(cl)

# Combinar y guardar todos los resultados en un archivo CSV
output_file <- "C:\\Users\\spinz\\OneDrive\\Documentos\\Portafolio oficial\\HERMESDB\\HERMESDB\\test\\data\\reports\\resultados_comparaciones.csv"
fwrite(resultados, output_file)

cat("El archivo con los resultados de las comparaciones se ha guardado en:", output_file)
