# synth-data-logistics
El objetivo principal fue poblar una base de datos PostgreSQL con más de 500.000 registros sintéticos manteniendo integridad referencial, coherencia temporal y reglas de negocio realistas.

# FleetLogix – Avance 1

## Generación de Datos Sintéticos para PostgreSQL

Este proyecto corresponde al primer avance del PI de FleetLogix, una empresa de transporte y logística enfocada en entregas de última milla.

El objetivo principal fue poblar una base de datos PostgreSQL con más de 500.000 registros sintéticos manteniendo integridad referencial, coherencia temporal y reglas de negocio realistas.

# Funcionalidades implementadas

- Generación masiva de datos sintéticos
- Inserciones batch optimizadas
- Relaciones válidas entre tablas
- Simulación realista de operaciones logísticas
- Validaciones automáticas de calidad de datos
- Logging de ejecución
- Reporte resumen de generación

---

# Tablas pobladas

- vehicles
- drivers
- routes
- trips
- deliveries
- maintenance

---

# Cantidad de registros generados

Total aproximado: 505000+ registros.

---

# Archivos principales

- `fleetlogix_db_schema.sql`
- `A1-01_data_generation_estudiantes.py`

---

# Ejecución

## 1. Crear la base de datos

Ejecutar:

```sql
fleetlogix_db_schema.sql

----



#### **Sistema de Gestión Logística**

Este proyecto corresponde al análisis y optimización de consultas SQL sobre una base de datos orientada a logística y transporte utilizando PostgreSQL.

El trabajo se centra en la ejecución de queries de distintos niveles de complejidad, el análisis de performance mediante EXPLAIN ANALYZE y la optimización del rendimiento utilizando índices.



**Objetivos**

* Ejecutar y analizar 12 consultas SQL
* Documentar técnicamente cada query
* Analizar planes de ejecución
* Optimizar performance utilizando índices
* Comparar tiempos de ejecución antes y después de la optimización

Contenido

**Queries básicas**

* Conteo de vehículos por tipo
* Conductores con licencia próxima a vencer
* Total de viajes por estado

**Queries intermedias**

* Entregas por ciudad destino
* Conductores activos y cantidad de viajes
* Productividad por conductor
* Rutas con mayor costo por kilómetro
* Entregas retrasadas por día de la semana

**Queries complejas**

* Costos de mantenimiento por kilómetro
* Ranking de conductores
* Tendencia de viajes con LAG
* Pivot de entregas por hora y día



**Técnicas SQL utilizadas**

INNER JOIN

LEFT JOIN

GROUP BY

ORDER BY

HAVING

COUNT

SUM

AVG

FILTER

EXTRACT

TO\_CHAR

INTERVAL

Common Table Expressions (CTE)

Window Functions

ROW\_NUMBER

LAG

Pivot manual

EXPLAIN ANALYZE

Optimización de performance



Se realizó un análisis de los planes de ejecución utilizando EXPLAIN ANALYZE para identificar consultas con mayor costo operativo.



Posteriormente, se ejecutó un **script en Python encargado de crear índices** sobre:

* claves foráneas
* columnas utilizadas en filtros
* campos utilizados en JOINs
* columnas usadas en agrupamientos y ordenamientos



Luego de aplicar los índices, se observaron mejoras en los tiempos de ejecución de las consultas y una reducción en los costos de procesamiento reportados por PostgreSQL.



**Tecnologías utilizadas**

PostgreSQL

SQL

Python

pgAdmin / DBeaver

Objetivo de negocio



Las consultas desarrolladas permiten analizar:

* productividad de conductores
* eficiencia operativa
* costos logísticos
* demanda por ciudades
* retrasos en entregas
* planificación de recursos
* tendencias operativas

#### **# FleetLogix – Data Warehouse \& ETL Pipeline**



Este proyecto implementa un \*\*Data Warehouse analítico en Snowflake\*\* para la empresa ficticia \*FleetLogix\*, orientado al análisis de operaciones logísticas y entregas.  

El sistema transforma datos operacionales provenientes de PostgreSQL en un modelo dimensional optimizado para reporting y análisis estratégico.



Incluye:

- Modelo en estrella (Kimball)

- Pipeline ETL automático en Python

- Cálculo de métricas operativas y financieras

- Seguridad por roles y auditoría histórica

---

## Arquitectura del sistema



\*\*Origen (OLTP):\*\*

- PostgreSQL  

- Tablas operacionales: deliveries, trips, drivers, vehicles, routes

*\*Proceso:\*\*

- ETL desarrollado en Python

- Transformaciones con pandas

- Automatización diaria con `schedule`



*\*Destino (OLAP):\*\*

- Snowflake Data Warehouse

- Esquema ANALYTICS



---



## Modelo Dimensional



### Tabla de Hechos

*\*fact\_deliveries\*\*

- Cada fila representa una entrega completada

- Métricas principales:

&#x20; - Tiempo de entrega

&#x20; - Retraso

&#x20; - Distancia recorrida

&#x20; - Combustible consumido

&#x20; - Ingresos y costos

&#x20; - Eficiencia operativa



### Dimensiones

- \*\*dim\_date\*\*: calendario completo (días, meses, trimestres, fines de semana)

- \*\*dim\_time\*\*: análisis por hora, turnos y franjas horarias

- \*\*dim\_vehicle\*\*: información del vehículo (SCD Type 2)

- \*\*dim\_driver\*\*: datos y experiencia del conductor (SCD Type 2)

- \*\*dim\_route\*\*: rutas, distancia y dificultad

- \*\*dim\_customer\*\*: clientes y segmentación



---



## Pipeline ETL



### Extracción

- Datos diarios desde PostgreSQL

- Límite de \*\*400 registros\*\* para pruebas de rendimiento



### Transformación

- Cálculo de métricas:

&#x20; - Entregas por hora

&#x20; - Eficiencia de combustible

&#x20; - Costos e ingresos por entrega

- Validaciones de calidad:

&#x20; - No se permiten tiempos negativos

&#x20; - Control de valores nulos y divisiones por cero

- Manejo de históricos (SCD Type 2)

- Pre-cálculo de totales diarios para reporting



### Carga

- Inserción en Snowflake

- Uso de claves sustitutas reales

- Control transaccional (commit / rollback)



---



## Automatización



- Ejecución diaria programada a las \*\*02:00 AM\*\*

- Implementada con la librería `schedule`

- Registro de logs y métricas de ejecución

- Batch ID para auditoría del proceso



---



## Seguridad y Gobernanza



- \*\*Time Travel\*\* activado (15 días)

- Vistas seguras por rol:

&#x20; - Ventas: solo información de clientes permitidos

&#x20; - Operaciones: acceso completo

- Separación de responsabilidades y control de accesos



---



## Tecnologías utilizadas



- \*\*Snowflake\*\* (Data Warehouse en la nube)

- \*\*PostgreSQL\*\* (Base de datos operacional)

- \*\*Python\*\*

&#x20; - pandas

&#x20; - snowflake-connector

&#x20; - psycopg2

&#x20; - schedule

- \*\*Jupyter Notebooks\*\*

- \*\*DBeaver\*\* (verificación y consultas)



---



## Notas finales



Este proyecto demuestra el pasaje de un modelo transaccional a uno analítico, aplicando buenas prácticas de modelado dimensional, automatización y control de calidad de datos, sentando las bases para análisis avanzados y visualización futura.

Arquitectura Cloud AWS (Avance 4)



Descripción del proyecto



Este proyecto corresponde al \*\*Avance 4\*\* y tiene como objetivo diseñar una *\*arquitectura en la nube con AWS\*\* para la ingesta, procesamiento y almacenamiento de datos de entregas en tiempo real de una flota de vehículos.

La solución se basa en una arquitectura \*\*serverless\*\*, priorizando bajo costo, escalabilidad y simplicidad, alineada con el uso de AWS Free Tier.



---



##Objetivos



- Diseñar una arquitectura cloud utilizando servicios fundamentales de AWS  

- Procesar eventos de entregas en tiempo real  

- Almacenar datos históricos y estados actuales de entregas  

- Implementar monitoreo y alertas básicas  

- Aplicar buenas prácticas de seguridad y control de costos  



---



## Arquitectura propuesta



La arquitectura incluye los siguientes servicios de AWS:



- \*\*API Gateway\*\*: recibe los eventos enviados por las aplicaciones móviles de los conductores.  

- \*\*AWS Lambda\*\*: procesa los eventos mediante funciones serverless.  

- \*\*Amazon S3\*\*: almacena datos históricos organizados por fecha.  

- \*\*Amazon RDS (PostgreSQL)\*\*: base de datos relacional administrada.  

- \*\*Amazon DynamoDB\*\*: almacena el estado actual de las entregas.  

- \*\*Amazon CloudWatch\*\*: monitoreo y generación de alertas.  

- \*\*IAM y KMS\*\*: gestión de accesos y encriptación de datos.  



El diagrama de arquitectura se encuentra en la carpeta `/diagrams`.



---



## Flujo de datos en tiempo real



Los eventos relevantes (entrega completada, retrasos, desvíos de ruta) se procesan en tiempo real mediante funciones Lambda.



---



## Funciones Lambda



Se implementan las siguientes funciones:



- Verificación de entrega completada  

- Cálculo del tiempo estimado de llegada (ETA)  

- Detección de desvíos de ruta y generación de alertas  



---



## Monitoreo y alertas



Se configura \*\*Amazon CloudWatch\*\* para:



- Visualizar métricas clave:

&#x20; - Entregas completadas

&#x20; - Vehículos activos

&#x20; - Tiempo promedio de entrega

&#x20; - Alertas generadas

&#x20; - Estado del sistema

- Enviar alertas automáticas por correo electrónico ante eventos críticos



---



## Seguridad y backups



- Uso de usuarios \*\*IAM\*\* con permisos limitados  

- Encriptación de datos en reposo mediante \*\*AWS KMS\*\*  

- Backups automáticos de la base de datos en \*\*Amazon RDS\*\*  



Estos mecanismos actúan de forma transversal a toda la arquitectura.



---



## Tecnologías utilizadas



- AWS: API Gateway, Lambda, S3, RDS, DynamoDB, CloudWatch, IAM  

- Python (boto3)  

- Postman (pruebas de endpoints)  

- draw.io (diagramas)  



---



## Notas finales



Este avance puede implementarse de forma real o teórica.  

Para pruebas reales se recomienda apagar los servicios luego de la validación para evitar costos adicionales.



---





