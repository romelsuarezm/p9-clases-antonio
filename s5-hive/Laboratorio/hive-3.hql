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
