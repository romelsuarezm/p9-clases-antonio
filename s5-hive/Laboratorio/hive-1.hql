//Clase de Hive
show databases; //permite ver las bases de datos en Hive disponible

//Crear una base de dato
create database ctic;
create database if not exists ctic2;

//Crear una tabla en base a un directorio en HDFS
//Indicarle siempre un directorio y formato del archivo
use ctic;
create table ctic.vuelos(
origen string,
destino string,
cantidad int
)
COMMENT 'Tabla vuelos del noveno grupo CTIC'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/landing/vuelos';