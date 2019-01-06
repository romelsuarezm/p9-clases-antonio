Diferentes formas de solución
1.1 Cuantos vuelos tienen origen en EEUU
    SELECT origen, COUNT(*) as total FROM CTIC.VUELOS WHERE origen = 'United States' GROUP BY origen;
    select sum(vuelos.cantidad) from ctic3.vuelos where vuelos.origen='United States';
    select sum(vuelos) from ctic.vuelos2 group by origen = "United States";

    1.2 Cuantos vuelos tienen destino Argentina
    SELECT destino, COUNT(*) as total FROM CTIC.VUELOS WHERE destino = 'Argentina' GROUP BY destino;
    select sum(vuelos.cantidad) from ctic3.vuelos where vuelos.destino='Argentina';
    select sum(vuelos) from ctic.vuelos2 where destino = "Argentina";

    1.3 Total de vuelos que salen y llegan a Francia
    select sum(vuelos.cantidad) from ctic3.vuelos where vuelos.destino='France' or vuelos.origen='France';
    SELECT COUNT(*) as TotalVuelosFrancia FROM CTIC.VUELOS WHERE origen = 'France' OR destino = 'France';
    select sum(vuelos) where destino = "France";

    1.4 Existen vuelos que llegan o salen de Perú