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