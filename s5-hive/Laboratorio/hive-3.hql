//PARQUET

create external table ctic.facebookparquet (id string, name string, birthday string, email string) stored as parquet LOCATION '/landing/facebookparquet';

MSCK REPAIR TABLE ctic.facebookparquet;
	
create external table ctic.parquetout (id string, name string, birthday string, email string) stored as parquet LOCATION '/landing/parquetout';

insert into ctic.parquetout select id, name, birthday, email from ctic.facebookparquet;


//Parquet desde un archivo
CREATE external table ctic.vuelosparquet LIKE PARQUET `/landing/vuelosparquet2/part-r-00000-1a9822ba-b8fb-4d8e-844a-ea30d0801b9e.gz.parquet` STORED AS PARQUET LOCATION '/landing/vuelosparquet2';

//PARTICIONES

/*Pre requisitos
CREATE DATABASE IF NOT EXISTS ctic;

USE ctic;
CREATE EXTERNAL TABLE IF NOT EXISTS  ctic.tabla_externa
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
COMMENT 'tabla_externa details'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = ",",
"escapeChar"="\\"
) 
STORED AS TEXTFILE
LOCATION '/landing/tabla_externa'
TBLPROPERTIES ("skip.header.line.count"="1")
; 

USE ctic;
load data inpath '/landing/data/sis_igp_2017.csv' into table ctic.tabla_externa;

*/

/*
Recordemos: 
- Particiones:: Particionar una tabla significa guardar los datos en subdirectorios categorizados por los valores de una columna, esto permite a Hive excluir datos innecesarios cuando tiene que realizar una consult 
- Estaticas: Cuando se cree la tabla con particiones, éstas pueden ser estáticas o dinámicas. Las estáticas indican que la partición ya se ha realizado en los directorios adecuados y pueden pedir a Hive alguna de esas particiones. 
- Dinámicas:Utilizamos particiones dinámicas cuando solo sabemos los valores que va a tener una columna en tiempo de ejecución. Dejamos a Hive que particione los datos automáticamente. Para disponer de particiones dinámicas es necesario habilitarlo. 
*/

//ESTATICAS
CREATE TABLE ctic.tabla_particion_estatica
(
    FecRegistro String,
    profundida String,
    medidaprod String,
    geom String,
    magnitud__ String
)
PARTITIONED BY (mensual STRING)
STORED AS PARQUET
LOCATION '/landing/tabla_particion_estatica';

//Agregamos data
insert into ctic.tabla_particion_estatica partition(mensual ='201807')
SELECT
  from_unixtime(unix_timestamp(CONCAT(SUBSTRING(Fecha, 7, 4), SUBSTRING(Fecha, 4, 2), SUBSTRING(Fecha, 1, 2)), 'yyyyMMdd'), 'yyyy-MM-dd') AS FecRegistro,
  UPPER(profundida) AS profundida,
  CASE
    WHEN profundida < 10 THEN 'BAJA'
    WHEN profundida BETWEEN 10 AND 50 THEN 'MEDIA'
    ELSE 'ALTA'
  END AS medidaprod,
  geom,
  magnitud__
FROM ctic.tabla_externa;

//DINAMICAS
CREATE TABLE ctic.tabla_particion_dinamica
(
    profundida String,
    medidaprod String,
    geom String,
    magnitud__ String
)
PARTITIONED BY (FecRegistro STRING)
STORED AS PARQUET
LOCATION '/landing/tabla_particion_dinamica';

set hive.exec.dynamic.partition.mode=nonstrict;

insert into table ctic.tabla_particion_dinamica partition (FecRegistro)
SELECT
  UPPER(profundida) AS profundida,
  CASE
    WHEN profundida < 10 THEN 'BAJA'
    WHEN profundida BETWEEN 10 AND 50 THEN 'MEDIA'
    ELSE 'ALTA'
  END AS medidaprod,
  geom,
  magnitud__,
  from_unixtime(unix_timestamp(CONCAT(SUBSTRING(Fecha, 7, 4), SUBSTRING(Fecha, 4, 2), SUBSTRING(Fecha, 1, 2)), 'yyyyMMdd'), 'yyyy-MM-dd') AS FecRegistro
FROM ctic.tabla_externa limit 10;

//BUCKETING
Por ejemplo, podemos agrupar la tabla employee_partitioned utilizando employee_id como una columna de agrupación. El valor de esta columna estará marcado por un número de buckets definido por el usuario. Los registros con el mismo employee_id siempre se almacenarán en el mismo contenedor (segmento de archivos) otra idea es hacer bucketing por sucursal si estas no difieren mucho en % de ventas. Es recomendable que tengan un tamaño cercano a 256 MB 

/*
Pre requisitos

USE ctic;
CREATE EXTERNAL TABLE IF NOT EXISTS ctic.employee_id (
name STRING,
employee_id INT, 
 work_place ARRAY<STRING>,
 gender_age STRUCT<gender:STRING,age:INT>,
 skills_score MAP<STRING,INT>,
 depart_title MAP<STRING,ARRAY<STRING>>
 )
 ROW FORMAT DELIMITED
 FIELDS TERMINATED BY '|'
 COLLECTION ITEMS TERMINATED BY ','
 MAP KEYS TERMINATED BY ':'
 LOCATION '/landing/tabla_bucketing';

 LOAD DATA INPATH 
 '/landing/data/employee_id.txt' 
 OVERWRITE INTO TABLE employee_id;

*/

 CREATE EXTERNAL TABLE IF NOT EXISTS ctic.employee_id_buckets (
 name STRING,
 employee_id INT, 
 work_place ARRAY<STRING>,
 gender_age STRUCT<gender:string,age:int>,
 skills_score MAP<string,int>,
 depart_title MAP<string,ARRAY<string >>
 )
 CLUSTERED BY (employee_id) INTO 2 BUCKETS 
 ROW FORMAT DELIMITED
 FIELDS TERMINATED BY '|'
 COLLECTION ITEMS TERMINATED BY ','
 MAP KEYS TERMINATED BY ':'
LOCATION '/landing/employee_id_buckets';

set map.reduce.tasks = 2;
set hive.enforce.bucketing = true;
INSERT OVERWRITE TABLE ctic.employee_id_buckets SELECT * FROM ctic.employee_id;

//Verificamos
hdfs dfs -ls /landing/employee_id_bucket

//Lanzar Código archivos hql Jupyter
beeline -u jdbc:hive2://localhost:10000/ctic -e "select * from ctic.facebook;"
//Cargar el archivo validar.hql a la ruta /landing/querys
beeline -u jdbc:hive2://localhost:10000/ctic -f "validar.hql"
