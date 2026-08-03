-- QUERIES BÁSICAS (3 queries)
-- =====================================================

-- Query 1: Contar vehículos por tipo
-- Problema de negocio: Conocer la composición de la flota

EXPLAIN ANALYZE 
SELECT vehicle_type AS tipo_de_vehiculo,
		COUNT(vehicle_type) AS Cantidad
FROM vehicles
GROUP BY vehicle_type;

-- Query 2: Listar conductores con licencia próxima a vencer (30 días o 60 dias)
-- Problema de negocio: Prevenir problemas legales por licencias vencidas
EXPLAIN ANALYZE 
SELECT first_name AS nombre,
		last_name AS Apellido,
		license_expiry AS Expiracion_de_licencia,
		(license_expiry - CURRENT_DATE) AS dias_restantes
FROM drivers
WHERE license_expiry <= CURRENT_DATE + INTERVAL '60 days'
AND license_expiry >= CURRENT_DATE
ORDER BY dias_restantes ASC;

-- Query 3: Total de viajes por estado
-- Problema de negocio: Monitorear operaciones en curso
EXPLAIN ANALYZE 
SELECT status,
		COUNT(status) 
FROM trips
GROUP BY status;

-- Query 4: Total de entregas por ciudad destino en los últimos 2 meses
-- Problema de negocio: Identificar demanda por ciudad para planificación de recursos 
EXPLAIN ANALYZE 
SELECT r.destination_city,
       COUNT(DISTINCT t.trip_id) AS total_entregas
FROM trips AS t
JOIN routes AS r
  ON t.route_id = r.route_id
WHERE t.arrival_datetime >= CURRENT_DATE - INTERVAL '2 months'
GROUP BY r.destination_city
ORDER BY total_entregas DESC;

-- Query 5: Conductores activos con cantidad de viajes completados
-- Problema de negocio: Evaluar carga de trabajo por conductor
EXPLAIN ANALYZE
SELECT d.driver_id,
		d.first_name AS nombre,
		d.last_name AS apellido,
		COUNT(t.status = 'complete') AS estado_viaje
FROM drivers AS d
LEFT JOIN trips AS t 
ON d.driver_id = t.driver_id
WHERE d.status = 'active'
GROUP BY d.driver_id, d.status;

-- Query 6: Promedio de entregas por conductor en los últimos 6 meses
-- Problema de negocio: Medir productividad individual de conductores
EXPLAIN ANALYZE
SELECT 
    d.first_name,
    d.last_name,
    COUNT(t.trip_id) AS total_viajes,
    COUNT(t.trip_id) * 100.0 / SUM(COUNT(t.trip_id)) OVER () AS porcentaje
FROM trips t
JOIN drivers d ON t.driver_id = d.driver_id
WHERE t.arrival_datetime >= CURRENT_DATE - INTERVAL '6 months'
GROUP BY d.first_name, d.last_name;

-- Query 7: Rutas con mayor consumo de combustible por kilómetro
-- Problema de negocio: Identificar rutas ineficientes para optimización
EXPLAIN ANALYZE
SELECT
		route_code, 
		SUM(toll_cost),
		SUM(distance_km) AS total_km,
		1.0 * SUM(toll_cost) / sum(distance_km) AS precio_por_km
FROM routes
GROUP BY  route_code
ORDER BY precio_por_km DESC
LIMIT 10;

-- Query 8: Análisis de entregas retrasadas por día de la semana
-- Problema de negocio: Identificar patrones de retraso para mejorar planificación
EXPLAIN ANALYZE
SELECT 
    TO_CHAR(t.arrival_datetime, 'Day') AS dia_semana,
    COUNT(*) FILTER (WHERE t.arrival_datetime > d.delivered_datetime) AS retrasadas,
    COUNT(*) AS total_entregas,
    COUNT(*) FILTER (WHERE t.arrival_datetime > d.delivered_datetime) * 100.0 
        / COUNT(*) AS porcentaje_retraso
FROM deliveries d
JOIN trips t ON d.trip_id = t.trip_id
GROUP BY dia_semana
ORDER BY porcentaje_retraso DESC;

--QUERIES COMPLEJAS
-- Query 9: Costo de mantenimiento por kilómetro recorrido 
-- Problema de negocio: Evaluar costo-beneficio de cada tipo de vehículo

EXPLAIN ANALYZE
WITH metricas_vehiculos as
	(SELECT v.vehicle_id, 
			v.vehicle_type,
			COUNT(t.trip_id) AS cant_viajes, 
			SUM(r.distance_km) AS km_recorridos
	FROM vehicles AS v
	INNER JOIN trips AS t ON v.vehicle_id = t.vehicle_id 
	INNER JOIN routes AS r ON t.route_id = r.route_id
	WHERE t.status = 'completed' 
	GROUP BY v.vehicle_id, v.vehicle_type 
	ORDER BY v.vehicle_id ASC),
metricas_mantenimiento as
	(SELECT COUNT(maintenance_id) AS mantenimientos, 
			vehicle_id, 
			SUM(COST) AS costo
	FROM maintenance
	GROUP BY vehicle_id)
	
SELECT	mv.vehicle_type,
		SUM(mv.km_recorridos) AS km_recorridos_totales,
		COUNT(mm.mantenimientos) AS cantidad_mantenimientos,
		1.00 * SUM(mm.costo) / SUM(mv.km_recorridos) AS costo_por_km,
		SUM(mm.costo) AS costos_totales
FROM metricas_vehiculos AS mv
LEFT JOIN metricas_mantenimiento AS mm
	ON mv.vehicle_id = mm.vehicle_id
GROUP BY vehicle_type;
	

-- Query 10: Ranking de conductores por eficiencia usando Window Functions
-- Problema de negocio: Identificar top performers para incentivos
EXPLAIN ANALYZE
	SELECT 	d.driver_id,
			d.first_name AS nombre_conductor, 
			d.last_name AS apellido_conductor, 
			COUNT(t.trip_id) AS cantidad_viajes,
			ROW_NUMBER() OVER (ORDER BY COUNT(t.trip_id) DESC) AS ranking
	FROM drivers AS d
	INNER JOIN trips AS t ON d.driver_id = t.driver_id 
	WHERE t.status = 'completed'
	GROUP BY d.driver_id, d.first_name, d.last_name 
	ORDER BY cantidad_viajes DESC LIMIT 10

-- Query 11: Análisis de tendencia de viajes con LAG y LEAD
-- Problema de negocio: Proyectar necesidades futuras basadas en tendencias

--variacion x mes de cada conductor
EXPLAIN ANALYZE
WITH viajes_por_mes AS (
    SELECT 
        d.driver_id,
        d.last_name,
        d.first_name,
        TO_CHAR(t.departure_datetime, 'YYYY-MM') AS mes,
        COUNT(t.trip_id) AS cantidad_viajes
    FROM drivers d
    JOIN trips t 
        ON d.driver_id = t.driver_id
    WHERE t.status = 'completed' 
    GROUP BY d.driver_id, TO_CHAR(t.departure_datetime, 'YYYY-MM')
)

SELECT 
    driver_id,
    last_name,
    first_name,
    mes,
    cantidad_viajes,
    
    LAG(cantidad_viajes) OVER (
        PARTITION BY driver_id 
        ORDER BY mes
    ) AS viajes_mes_anterior,
    
    cantidad_viajes - LAG(cantidad_viajes) OVER (
        PARTITION BY driver_id 
        ORDER BY mes
    ) AS variacion
FROM viajes_por_mes;


-- Query 12: Pivot de entregas por hora y día de la semana
-- Problema de negocio: Optimizar horarios de operación y personal

--columas: driver | dias de la semana en columnas 
--filas : id driver | horas en cada dia de la semana


SELECT
    dr.driver_id,
    EXTRACT(HOUR FROM de.delivered_datetime) AS hora,

    COUNT(*) FILTER (
        WHERE EXTRACT(DOW FROM t.arrival_datetime) = 1
    ) AS lunes,

    COUNT(*) FILTER (
        WHERE EXTRACT(DOW FROM t.arrival_datetime) = 2
    ) AS martes,

    COUNT(*) FILTER (
        WHERE EXTRACT(DOW FROM t.arrival_datetime) = 3
    ) AS miercoles,

    COUNT(*) FILTER (
        WHERE EXTRACT(DOW FROM t.arrival_datetime) = 4
    ) AS jueves,

    COUNT(*) FILTER (
        WHERE EXTRACT(DOW FROM t.arrival_datetime) = 5
    ) AS viernes,

    COUNT(*) FILTER (
        WHERE EXTRACT(DOW FROM t.arrival_datetime) = 6
    ) AS sabado,

    COUNT(*) FILTER (
        WHERE EXTRACT(DOW FROM t.arrival_datetime) = 0
    ) AS domingo

FROM deliveries de
JOIN trips t
    ON de.trip_id = t.trip_id
JOIN drivers dr
    ON t.driver_id = dr.driver_id

GROUP BY
    dr.driver_id,
    hora

ORDER BY
    dr.driver_id,
    hora;

