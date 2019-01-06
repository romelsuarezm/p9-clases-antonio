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
1. Cargar el archivo sis_igp_2017.csv en la carpeta 
/landing/datosigp
2. Crear la tabla externa igp_sis apuntando a la ruta
/landing/igp_sis
3. Crear la tabla temporal igp_temp
4. Cargar mediante LoadInpath a la tabla igp_sis
5. Cargar mediante Insert from Select a la tabla igp_temp
desde la tabla igp_sis

//Tipos de archivos
/*
Recordemos: 
- TEXTFILE:Es el formato genérico de hadoop, fácil de lectura pero lento en procesamiento 
- AVRO: Este formato trabaja en esquemas, trabaja con estructuras complejas y su procesamiento es rápido. 
- PARQUET: su procesamiento es mucho más rápido que avro, es el formato estándar para trabajar en un clúster de cloudera 
- ORC: este formato es el más rápido de todos por su composición, no tiene soporte en todas las herramientas de BigData.
*/

//TEXTFILE
use ctic;
CREATE EXTERNAL TABLE IF NOT EXISTS ctic.texttable
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

//AVRO
CREATE EXTERNAL TABLE ctic.twitterctic2
(
    username String,
    tweet String,
    fecha bigint
)
STORED AS AVRO
LOCATION '/landing/twitter';

CREATE EXTERNAL TABLE ctic.twitteresquema2
(
    username String,
    tweet String,
    fecha bigint
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '/landing/twitter'
TBLPROPERTIES ('avro.schema.url'='hdfs:///landing/schemas/twitter.avsc');

//GENERAR ARCHIVO AVRO O GENERAR .AVSC
//Instrucciones para el jar
1. Descargar https://mvnrepository.com/artifact/org.apache.avro/avro-tools/1.8.2
2. Ingestar en Linux
3. Validar instalación java -jar avro-tools-1.8.2.jar
4. Escribo el .avsc
5. Si tengo el .avro y quiero conseguir el .avsc 
	java -jar avro-tools-1.8.2.jar getschema episodes.avro > episodes.avsc
	java -jar avro-tools-1.8.2.jar getschema facebook.avro > facebook.avsc
6. Si tengo el json y el .avsc y quiero el .avro
	java -jar avro-tools-1.8.2.jar fromjson --schema-file facebook.avsc facebook.json > facebook2.avro

java -jar avro-tools-1.8.2.jar fromjson --schema-file facebook.avsc facebook.json > facebook.avro

CREATE EXTERNAL TABLE ctic.facebook2
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '/landing/facebook'
TBLPROPERTIES ('avro.schema.url'='hdfs:///landing/schemas/facebook.avsc');


//PARQUET

create external table ctic.facebookparquet (id string, name string, birthday string, email string) stored as parquet LOCATION '/landing/facebookparquet';

MSCK REPAIR TABLE ctic.facebookparquet;
	
create external table ctic.parquetout (id string, name string, birthday string, email string) stored as parquet LOCATION '/landing/parquetout';

insert into ctic.parquetout select id, name, birthday, email from ctic.facebookparquet;


//Parquet desde un archivo
CREATE external table ctic.vuelosparquet LIKE PARQUET `/landing/vuelosparquet2/part-r-00000-1a9822ba-b8fb-4d8e-844a-ea30d0801b9e.gz.parquet` STORED AS PARQUET LOCATION '/landing/vuelosparquet2';

//PARTICIONES
//ESTATICAS


//DINAMICAS



//BUCKETING


//Lanzar Código archivos hql Jupyter
beeline -u jdbc:hive2://localhost:10000/ctic -e "select * from ctic.facebook;"
//Cargar el archivo validar.hql a la ruta /landing/querys
beeline -u jdbc:hive2://localhost:10000/ctic -f "validar.hql"
