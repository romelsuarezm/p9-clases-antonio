//Cuando la fila 1 tiene el nombre de las columnas

USE ctic;
CREATE EXTERNAL TABLE IF NOT EXISTS ctic.vuelos2(
destino STRING COMMENT 'País de origen',
origen STRING COMMENT 'País de destino',
vuelos STRING COMMENT 'Cantidad de vuelos'
)
COMMENT 'tabla de vuelos mensuales entre países'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/landing/trip'
tblproperties("skip.header.line.count" = "1")
;

//Ejercicio 1
1. Indicar
1.1 Cuantos vuelos tienen origen en EEUU
1.2 Cuantos vuelos tienen destino Argentina
1.3 Total de vuelos que salen y llegan a Francia
1.4 Existen vuelos que llegan o salen de Perú

//Mostrar las tablas de una base de datos
show tables;

//Describir con detalles una tabla
describe formatted ctic.vuelos2;

//Crear una tabla gestionada que incluye el uso de SerDe
//Ser = serialization De = deserialization
use ctic;
CREATE TABLE IF NOT EXISTS ctic.tabla_gestionada2
( 
    FID String,
    geom String,
    longitud String,
    latitud String,
    profundida String,
    magnitud__ String,
    fecha String,
    hora_utc String,
    clasif String
)
COMMENT 'tabla_gestionada details'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = ",",
"escapeChar"="\\"
) 
STORED AS TEXTFILE
LOCATION '/landing/mediciongestionada'
TBLPROPERTIES ("skip.header.line.count"="1")
; 

// Tabla Externa
use ctic;
CREATE EXTERNAL TABLE IF NOT EXISTS  ctic.tablaexterna
( 
    FID String,
    geom String,
    longitud String,
    latitud String,
    profundida String,
    magnitud__ String,
    fecha String,
    hora_utc String,
    clasif String
)
COMMENT 'tabla_tabla_externa details'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = ",",
"escapeChar"="\\"
) 
STORED AS TEXTFILE
LOCATION '/landing/mediciongtabla_externa'
TBLPROPERTIES ("skip.header.line.count"="1")
; 

use ctic;
CREATE EXTERNAL TABLE IF NOT EXISTS  ctic.tablaexternainpath
( 
    FID String,
    geom String,
    longitud String,
    latitud String,
    profundida String,
    magnitud__ String,
    fecha String,
    hora_utc String,
    clasif String
)
COMMENT 'tabla_tabla_externa details'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = ",",
"escapeChar"="\\"
) 
STORED AS TEXTFILE
LOCATION '/landing/tablaexternainpath'
TBLPROPERTIES ("skip.header.line.count"="1")
; 

//Creando una tabla temporal
CREATE TEMPORARY TABLE ctic.tabla_temporal
( 
    FID String,
    geom String,
    longitud String,
    latitud String,
    profundida String,
    magnitud__ String,
    fecha String,
    hora_utc String,
    clasif String
);

//Insertar data

// Load inpath

load data inpath '/landing/mediciongtabla_externa/sis_igp_2017.csv' into table ctic.tablaexternainpath;

// Insert 

insert into ctic.tabla_temporal select * from ctic.tablaexternainpath;

Ejercicio 2

1. Cargar el archivo sis_igp_2017 en la carpeta 
/landing/datosigp

2. Crear la tabla externa igp_sis apuntando a la ruta
/landing/igp_sis

3. crear la tabla teamporal igp_temp

4. Cargar mediante Loadinpath a la tabla igp_sis

5. Cargar mediante Insert from Select a la tabla igp_temp
desde la tabla igp_sis



