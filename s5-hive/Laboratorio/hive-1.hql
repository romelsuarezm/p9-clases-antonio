//Clase de Hive
show databases; //permite ver las bases de datos en Hive disponible

//Crear una base de dato
create database ctic;
create database if not exists ctic2;

//Crear una tabla en base a un directorio en HDFS
//Indicarle siempre un directorio y formato del archivo
//Tabla gestionada -> si borras la tabla se borra la data
use ctic;
create table ctic.mundo(
inicio string,
fin string,
total int
)
COMMENT 'Tabla mundo del noveno grupo CTIC'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/landing/viajes';

//Tablas externas: si borras la tabla NO se borra la data
use ctic;
create external if not exists table ctic.trip(
inicio string,
fin string,
total int
)
COMMENT 'Tabla trip del noveno grupo CTIC'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/landing/trip';