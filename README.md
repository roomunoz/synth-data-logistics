# FleetLogix - Proyecto Integrador

Este repositorio documenta el desarrollo del proyecto **FleetLogix**, una empresa ficticia de transporte y logística enfocada en entregas de última milla. El proyecto abarca el ciclo completo de ingeniería de datos, desde la generación de datos sintéticos hasta el diseño de una arquitectura cloud en AWS.

---

## Índice

- [Avance 1: Generación de Datos Sintéticos](#avance-1-generación-de-datos-sintéticos)
- [Avance 2 y 3: Gestión Logística y Optimización SQL](#avance-2-y-3-gestión-logística-y-optimización-sql)
- [Data Warehouse y Pipeline ETL](#data-warehouse-y-pipeline-etl)
- [Avance 4: Arquitectura Cloud (AWS)](#avance-4-arquitectura-cloud-aws)
- [Tecnologías Utilizadas](#tecnologías-utilizadas)

---

# Avance 1: Generación de Datos Sintéticos

El objetivo de esta etapa fue poblar una base de datos PostgreSQL con más de **500.000 registros sintéticos**, manteniendo la integridad referencial, la coherencia temporal y reglas de negocio realistas.

## Funcionalidades

- Generación masiva de datos sintéticos mediante inserciones batch optimizadas.
- Modelado de relaciones consistentes entre entidades.
- Simulación de operaciones logísticas realistas.
- Validaciones automáticas de calidad de datos.
- Registro (*logging*) del proceso de generación.

## Tablas pobladas

- `vehicles`
- `drivers`
- `routes`
- `trips`
- `deliveries`
- `maintenance`

## Archivos principales

| Archivo | Descripción |
|----------|-------------|
| `fleetlogix_db_schema.sql` | Esquema completo de la base de datos. |
| `A1-01_data_generation_estudiantes.py` | Script para la generación de datos sintéticos. |

---

# Avance 2 y 3: Gestión Logística y Optimización SQL

En esta etapa se analizaron y optimizaron consultas SQL sobre PostgreSQL con foco en rendimiento y eficiencia.

## Objetivos

- Desarrollo de 12 consultas SQL (básicas, intermedias y avanzadas).
- Análisis de planes de ejecución utilizando `EXPLAIN ANALYZE`.
- Optimización mediante índices y mejoras en consultas.

## Técnicas utilizadas

### Consultas

- INNER JOIN
- LEFT JOIN
- GROUP BY
- HAVING
- COUNT
- SUM
- AVG

### Funciones SQL

- Window Functions
- LAG
- ROW_NUMBER
- EXTRACT

### Optimización

- Common Table Expressions (CTE)
- Pivot manual
- Índices B-Tree
- Optimización de JOINs
- Optimización de filtros y agrupamientos

---

# Data Warehouse y Pipeline ETL

Se implementó un **Data Warehouse analítico en Snowflake**, transformando los datos operacionales en un modelo dimensional orientado al análisis y la generación de reportes.

## Arquitectura

| Etapa | Tecnología |
|--------|------------|
| Origen (OLTP) | PostgreSQL |
| ETL | Python (pandas) + schedule |
| Destino (OLAP) | Snowflake |

## Modelo dimensional

### Tabla de hechos

- `fact_deliveries`

Incluye métricas de:

- Tiempo
- Distancia
- Consumo de combustible
- Costos

### Dimensiones

- `dim_date`
- `dim_time`
- `dim_vehicle`
- `dim_driver`
- `dim_route`
- `dim_customer`

## Seguridad y gobernanza

- Implementación de Slowly Changing Dimensions (SCD Tipo 2).
- Configuración de Time Travel (15 días).
- Creación de vistas seguras según el rol del usuario.

---

# Avance 4: Arquitectura Cloud (AWS)

Se diseñó una arquitectura serverless para la ingesta y procesamiento de eventos logísticos en tiempo real.

## Componentes de AWS

- API Gateway
- AWS Lambda
- Amazon S3
- Amazon RDS (PostgreSQL)
- Amazon DynamoDB
- Amazon CloudWatch
- IAM
- AWS KMS

## Funcionalidades

- Recepción de eventos logísticos.
- Verificación de entregas completadas.
- Cálculo del ETA.
- Detección de desvíos.
- Generación de alertas automáticas.

---

# Tecnologías Utilizadas

| Categoría | Tecnologías |
|------------|-------------|
| Bases de datos | PostgreSQL · Snowflake · DynamoDB |
| Lenguajes | SQL · Python |
| Librerías | boto3 · psycopg2 · pandas |
| Cloud | AWS Lambda · API Gateway · Amazon S3 · Amazon RDS · DynamoDB · CloudWatch |
| Herramientas | DBeaver · pgAdmin · Postman · draw.io |

---

## Autores

Proyecto desarrollado como parte del Proyecto Integrador **FleetLogix**.

```
© Todos los derechos reservados.
```