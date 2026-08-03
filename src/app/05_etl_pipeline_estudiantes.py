"""
FleetLogix - Pipeline ETL Automático
Extrae de PostgreSQL, Transforma y Carga en Snowflake
Ejecución diaria automatizada
"""

import psycopg2
import snowflake.connector
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
import logging
import schedule
import time
import json
from typing import Dict, List
from dotenv import load_dotenv
import os


# Cargar las variables de entorno del archivo .env
load_dotenv()

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_pipeline.log'),
        logging.StreamHandler()
    ]
)

# Configuración de conexiones
POSTGRES_CONFIG = {
    'host': os.getenv('postgres_host'),
    'database': os.getenv('postgres_db'),
    'user': os.getenv('postgres_usuario'),
    'password': os.getenv('postgres_pw'),
    'port': os.getenv('postgres_port')
}

SNOWFLAKE_CONFIG = {
    'user': os.getenv('sf_user'),
    'password': os.getenv('sf_pw'),
    'account': os.getenv('sf_acc'),
    'warehouse': os.getenv('sf_warehouse'),
    'database': os.getenv('sf_db'),
    'schema': os.getenv('sf_schema'),
    'autocommit': False  # FIX: control explícito de transacciones

}

# SNOWFLAKE_CONFIG = {
#     'user': 'your_user',
#     'password': 'your_password',
#     'account': 'your_account',
#     'warehouse': 'FLEETLOGIX_WH',
#     'database': 'FLEETLOGIX_DW',
#     'schema': 'ANALYTICS',
# }


class FleetLogixETL:
    def __init__(self):
        self.pg_conn = None
        self.sf_conn = None
        self.batch_id = int(datetime.now().timestamp())
        self.metrics = {
            'records_extracted': 0,
            'records_transformed': 0,
            'records_loaded': 0,
            'errors': 0
        }

    def connect_databases(self):
        """Establecer conexiones con PostgreSQL y Snowflake"""
        try:
            self.pg_conn = psycopg2.connect(**POSTGRES_CONFIG)
            logging.info("Conectado a PostgreSQL")

            self.sf_conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
            logging.info("Conectado a Snowflake")

            return True
        except Exception as e:
            logging.error(f"Error en conexión: {e}")
            return False

    def extract_daily_data(self) -> pd.DataFrame:
        """Extraer datos 1 meses anterior de PostgreSQL"""
        logging.info(" Iniciando extracción de datos...")
        
        query = """SELECT
            d.delivery_id,
            d.tracking_number,
            d.customer_name,
            d.delivery_address,
            d.package_weight_kg,
            d.scheduled_datetime,
            d.delivered_datetime,
            d.delivery_status,
            d.recipient_signature,

            t.trip_id,
            t.departure_datetime,
            t.arrival_datetime,
            t.fuel_consumed_liters,
            t.total_weight_kg,

            dr.driver_id,
            dr.first_name,
            dr.last_name,
            dr.hire_date,
            dr.employee_code,
            dr.license_number,
            dr.license_expiry,
            dr.phone,
            dr.status as driver_status,

            v.vehicle_id,
            v.license_plate,
            v.vehicle_type,
            v.capacity_kg,
            v.fuel_type,
            v.acquisition_date,
            v.status as vehicle_status,

            r.route_id,
            r.route_code,
            r.origin_city,
            r.destination_city,
            r.distance_km,
            r.toll_cost,
            r.estimated_duration_hours

        FROM deliveries d
        JOIN trips t ON d.trip_id = t.trip_id
        JOIN drivers dr ON t.driver_id = dr.driver_id
        JOIN vehicles v ON t.vehicle_id = v.vehicle_id
        JOIN routes r ON t.route_id = r.route_id
        LIMIT 400;"""

        try:
            df = pd.read_sql(query, self.pg_conn)
            self.metrics['records_extracted'] = len(df)
            logging.info(f"Extraídos {len(df)} registros")
            return df
        except Exception as e:
            logging.error(f"Error en extracción: {e}")
            self.metrics['errors'] += 1
            return pd.DataFrame()

    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transformar datos para el modelo dimensional"""
        logging.info("Iniciando transformación de datos...")

        try:
            # Parse de datetimes
            for col in ['scheduled_datetime', 'delivered_datetime',
                        'departure_datetime', 'arrival_datetime']:
                df[col] = pd.to_datetime(df[col])

            # Descartar filas sin fecha de entrega real
            df = df.dropna(subset=['delivered_datetime', 'scheduled_datetime'])

            # Tiempo total desde programado hasta entregado
            df['delivery_time_minutes'] = (
                (df['delivered_datetime'] - df['scheduled_datetime'])
                .dt.total_seconds() / 60
            ).round(2)

            # Retraso: positivo = tarde; negativo (adelantado) se recorta a 0
            df['delay_minutes'] = df['delivery_time_minutes'].clip(lower=0).round(2)
            df['is_on_time'] = df['delay_minutes'] <= 30

            # FIX: eliminado el filtro que descartaba entregas adelantadas
            df = df[(df['package_weight_kg'] > 0) & (df['package_weight_kg'] < 10000)]

            # Duración del viaje en horas
            df['trip_duration_hours'] = (
                (df['arrival_datetime'] - df['departure_datetime'])
                .dt.total_seconds() / 3600
            ).round(2)

            # FIX: evitar división por cero reemplazando 0 con NaN
            df['trip_duration_hours'] = df['trip_duration_hours'].replace(0, np.nan)
            df['fuel_consumed_liters'] = df['fuel_consumed_liters'].replace(0, np.nan)

            deliveries_per_trip = df.groupby('trip_id').size()
            df['deliveries_in_trip'] = df['trip_id'].map(deliveries_per_trip)

            df['deliveries_per_hour'] = (
                df['deliveries_in_trip'] / df['trip_duration_hours']
            ).round(2)

            df['fuel_efficiency_km_per_liter'] = (
                df['distance_km'] / df['fuel_consumed_liters']
            ).round(2)

            df['cost_per_delivery'] = (
                (df['fuel_consumed_liters'].fillna(0) * 5000 + df['toll_cost']) /
                df['deliveries_in_trip']
            ).round(2)

            df['revenue_per_delivery'] = (20000 + df['package_weight_kg'] * 500).round(2)

            # FIX #6: has_signature debe ser BOOLEAN, no el texto de la firma
            df['has_signature'] = (
                df['recipient_signature'].notna() & (df['recipient_signature'] != '')
            )

            # Campos SCD Type 2
            df['valid_from'] = df['scheduled_datetime'].dt.date
            df['valid_to'] = date(2099, 12, 31)
            df['is_current'] = True

            # Nombre completo del conductor
            df['full_name'] = df['first_name'].str.strip() + ' ' + df['last_name'].str.strip()

            # Antigüedad en meses (vehículo y conductor)
            today = date.today()
            df['age_months'] = df['acquisition_date'].apply(
                lambda x: (today.year - x.year) * 12 + (today.month - x.month)
                if pd.notna(x) else None
            )
            df['experience_months'] = df['hire_date'].apply(
                lambda x: (today.year - x.year) * 12 + (today.month - x.month)
                if pd.notna(x) else None
            )

            self.metrics['records_transformed'] = len(df)
            logging.info(f"Transformados {len(df)} registros")
            return df

        except Exception as e:
            logging.error(f"Error en transformación: {e}")
            self.metrics['errors'] += 1
            return pd.DataFrame()

    # -------------------------------------------------------------------------
    # FIX #4: Métodos auxiliares para poblar dim_date y dim_time
    # -------------------------------------------------------------------------

    def _ensure_dim_date(self, cursor, unique_dates):
        """Insertar fechas en dim_date si todavía no existen."""
        for d in unique_dates:
            if pd.isna(d):
                continue
            # Aceptar tanto date como Timestamp
            if hasattr(d, 'date'):
                d = d.date()
            date_key = int(d.strftime('%Y%m%d'))
            day_of_week = d.isoweekday()   # 1=Lunes … 7=Domingo
            is_weekend = day_of_week >= 6
            quarter = (d.month - 1) // 3 + 1

            cursor.execute("""
                MERGE INTO dim_date t
                USING (SELECT %s AS date_key) s ON t.date_key = s.date_key
                WHEN NOT MATCHED THEN INSERT (
                    date_key, full_date, day_of_week, day_name, day_of_month,
                    day_of_year, week_of_year, month_num, month_name,
                    quarter, year, is_weekend, is_holiday, holiday_name,
                    fiscal_quarter, fiscal_year
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, FALSE, NULL, %s, %s
                )
            """, (
                date_key,
                date_key, d, day_of_week, d.strftime('%A'),
                d.day, d.timetuple().tm_yday, d.isocalendar()[1],
                d.month, d.strftime('%B'),
                quarter, d.year, is_weekend,
                quarter, d.year
            ))

    def _ensure_dim_time(self, cursor, time_keys):
        """Insertar registros en dim_time (agrupados por hora) si no existen."""
        def time_of_day(h):
            if h < 6:
                return 'Madrugada'
            elif h < 12:
                return 'Mañana'
            elif h < 18:
                return 'Tarde'
            return 'Noche'

        for tk in set(time_keys):
            if pd.isna(tk):
                continue
            hour = int(tk) // 100
            is_business = 8 <= hour < 18
            if 6 <= hour < 14:
                shift = 'Turno 1'
            elif 14 <= hour < 22:
                shift = 'Turno 2'
            else:
                shift = 'Turno 3'
            hour_12 = (hour % 12) or 12
            am_pm = 'AM' if hour < 12 else 'PM'

            cursor.execute("""
                MERGE INTO dim_time t
                USING (SELECT %s AS time_key) s ON t.time_key = s.time_key
                WHEN NOT MATCHED THEN INSERT (
                    time_key, hour, minute, second, time_of_day,
                    hour_24, hour_12, am_pm, is_business_hour, shift
                ) VALUES (%s, %s, 0, 0, %s, %s, %s, %s, %s, %s)
            """, (
                tk,
                tk, hour, time_of_day(hour),
                f"{hour:02d}:00",
                f"{hour_12:02d}:00 {am_pm}",
                am_pm, is_business, shift
            ))

    # -------------------------------------------------------------------------
    # FIX #2 / #3 / #4 / #5: load_dimensions corregido
    # -------------------------------------------------------------------------

    def load_dimensions(self, df: pd.DataFrame) -> Dict:
        """
        Cargar o actualizar todas las dimensiones en Snowflake.
        Retorna un dict con los mapeos {source_id -> surrogate_key} para
        que load_facts pueda resolver las claves correctamente.
        """
        logging.info("Cargando dimensiones...")
        cursor = self.sf_conn.cursor()
        key_maps: Dict = {}

        try:
            # --- dim_vehicle (SCD Type 2 simplificado) ---
            vehicles = df[['vehicle_id', 'license_plate', 'vehicle_type', 'capacity_kg',
                           'fuel_type', 'acquisition_date', 'age_months',
                           'vehicle_status']].drop_duplicates('vehicle_id')

            for _, row in vehicles.iterrows():
                cursor.execute("""
                    MERGE INTO dim_vehicle t
                    USING (SELECT %s AS vehicle_id) s
                          ON t.vehicle_id = s.vehicle_id AND t.is_current = TRUE
                    WHEN NOT MATCHED THEN INSERT (
                        vehicle_key, vehicle_id, license_plate, vehicle_type,
                        capacity_kg, fuel_type, acquisition_date, age_months,
                        status, valid_from, valid_to, is_current
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, '2099-12-31', TRUE)
                """, (
                    int(row['vehicle_id']),
                    int(row['vehicle_id']), int(row['vehicle_id']),
                    row['license_plate'], row['vehicle_type'],
                    float(row['capacity_kg']) if pd.notna(row['capacity_kg']) else None,
                    row['fuel_type'],
                    row['acquisition_date'] if pd.notna(row['acquisition_date']) else None,
                    int(row['age_months']) if pd.notna(row['age_months']) else None,
                    row['vehicle_status'],
                    date.today()
                ))

            vids = [int(v) for v in df['vehicle_id'].unique()]
            placeholders = ','.join(['%s'] * len(vids))
            cursor.execute(
                f"SELECT vehicle_id, vehicle_key FROM dim_vehicle "
                f"WHERE vehicle_id IN ({placeholders}) AND is_current = TRUE",
                vids
            )
            key_maps['vehicle'] = {row[0]: row[1] for row in cursor.fetchall()}

            # --- dim_driver (SCD Type 2 simplificado) ---
            drivers = df[['driver_id', 'employee_code', 'full_name', 'license_number',
                          'license_expiry', 'phone', 'hire_date', 'experience_months',
                          'driver_status']].drop_duplicates('driver_id')

            for _, row in drivers.iterrows():
                cursor.execute("""
                    MERGE INTO dim_driver t
                    USING (SELECT %s AS driver_id) s
                          ON t.driver_id = s.driver_id AND t.is_current = TRUE
                    WHEN NOT MATCHED THEN INSERT (
                        driver_key, driver_id, employee_code, full_name,
                        license_number, license_expiry, phone, hire_date,
                        experience_months, status, performance_category,
                        valid_from, valid_to, is_current
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'Medio', %s, '2099-12-31', TRUE)
                """, (
                    int(row['driver_id']),
                    int(row['driver_id']), int(row['driver_id']),
                    row['employee_code'], row['full_name'],
                    row['license_number'],
                    row['license_expiry'] if pd.notna(row['license_expiry']) else None,
                    row['phone'],
                    row['hire_date'] if pd.notna(row['hire_date']) else None,
                    int(row['experience_months']) if pd.notna(row['experience_months']) else None,
                    row['driver_status'],
                    date.today()
                ))

            dids = [int(d) for d in df['driver_id'].unique()]
            placeholders = ','.join(['%s'] * len(dids))
            cursor.execute(
                f"SELECT driver_id, driver_key FROM dim_driver "
                f"WHERE driver_id IN ({placeholders}) AND is_current = TRUE",
                dids
            )
            key_maps['driver'] = {row[0]: row[1] for row in cursor.fetchall()}

            # --- dim_route ---
            routes = df[['route_id', 'route_code', 'origin_city', 'destination_city',
                         'distance_km', 'estimated_duration_hours',
                         'toll_cost']].drop_duplicates('route_id')

            for _, row in routes.iterrows():
                dist = float(row['distance_km']) if pd.notna(row['distance_km']) else 0
                difficulty = 'Fácil' if dist < 50 else ('Medio' if dist < 200 else 'Difícil')
                route_type = 'Urbana' if dist < 30 else ('Interurbana' if dist < 300 else 'Rural')

                cursor.execute("""
                    MERGE INTO dim_route t
                    USING (SELECT %s AS route_id) s ON t.route_id = s.route_id
                    WHEN NOT MATCHED THEN INSERT (
                        route_key, route_id, route_code, origin_city, destination_city,
                        distance_km, estimated_duration_hours, toll_cost,
                        difficulty_level, route_type
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    int(row['route_id']),
                    int(row['route_id']), int(row['route_id']),
                    row['route_code'], row['origin_city'], row['destination_city'],
                    dist,
                    float(row['estimated_duration_hours']) if pd.notna(row['estimated_duration_hours']) else None,
                    float(row['toll_cost']) if pd.notna(row['toll_cost']) else 0,
                    difficulty, route_type
                ))

            rids = [int(r) for r in df['route_id'].unique()]
            placeholders = ','.join(['%s'] * len(rids))
            cursor.execute(
                f"SELECT route_id, route_key FROM dim_route WHERE route_id IN ({placeholders})",
                rids
            )
            key_maps['route'] = {row[0]: row[1] for row in cursor.fetchall()}

            # --- dim_customer ---
            # customer_key es PRIMARY KEY sin IDENTITY: debemos proveer el valor explícitamente.
            # Consultamos el máximo actual y asignamos claves secuenciales para los nuevos clientes.
            cursor.execute("SELECT COALESCE(MAX(customer_key), 0) FROM dim_customer")
            next_customer_key = cursor.fetchone()[0] + 1

            customers = df[['customer_name', 'destination_city']].drop_duplicates('customer_name')
            for _, row in customers.iterrows():
                cursor.execute("""
                    MERGE INTO dim_customer c
                    USING (SELECT %s AS customer_name) s ON c.customer_name = s.customer_name
                    WHEN NOT MATCHED THEN INSERT (
                        customer_key, customer_name, customer_type, city,
                        first_delivery_date, total_deliveries, customer_category
                    ) VALUES (%s, %s, 'Individual', %s, CURRENT_DATE(), 0, 'Regular')
                """, (row['customer_name'], next_customer_key, row['customer_name'], row['destination_city']))
                next_customer_key += 1

            cnames = list(df['customer_name'].unique())
            placeholders = ','.join(['%s'] * len(cnames))
            cursor.execute(
                f"SELECT customer_name, customer_key FROM dim_customer "
                f"WHERE customer_name IN ({placeholders})",
                cnames
            )
            key_maps['customer'] = {row[0]: row[1] for row in cursor.fetchall()}

            # --- dim_date y dim_time ---
            unique_dates = df['scheduled_datetime'].dt.date.unique()
            self._ensure_dim_date(cursor, unique_dates)

            sched_tks = (df['scheduled_datetime'].dt.hour * 100).unique().tolist()
            deliv_tks = (df['delivered_datetime'].dt.hour * 100).unique().tolist()
            self._ensure_dim_time(cursor, sched_tks + deliv_tks)

            self.sf_conn.commit()
            logging.info("Dimensiones actualizadas")
            return key_maps

        except Exception as e:
            logging.error(f"Error cargando dimensiones: {e}")
            self.sf_conn.rollback()
            self.metrics['errors'] += 1
            return {}

    def load_facts(self, df: pd.DataFrame, key_maps: Dict):
        """Cargar hechos en Snowflake usando los surrogate keys reales de las dimensiones."""
        logging.info("Cargando tabla de hechos...")
        cursor = self.sf_conn.cursor()

        try:
            fact_data = []
            for _, row in df.iterrows():
                sched_dt = row['scheduled_datetime']
                deliv_dt = row['delivered_datetime']

                date_key = int(sched_dt.strftime('%Y%m%d'))
                scheduled_time_key = sched_dt.hour * 100
                delivered_time_key = deliv_dt.hour * 100

                # FIX #2 / #3: resolver surrogate keys reales desde los mapeos de dimensión
                vehicle_key = key_maps.get('vehicle', {}).get(int(row['vehicle_id']))
                driver_key = key_maps.get('driver', {}).get(int(row['driver_id']))
                route_key = key_maps.get('route', {}).get(int(row['route_id']))
                customer_key = key_maps.get('customer', {}).get(row['customer_name'])

                if any(k is None for k in [vehicle_key, driver_key, route_key, customer_key]):
                    logging.warning(
                        f"Surrogate key faltante para delivery_id={row['delivery_id']} "
                        f"(vehicle={vehicle_key}, driver={driver_key}, "
                        f"route={route_key}, customer={customer_key}) — fila omitida"
                    )
                    self.metrics['errors'] += 1
                    continue

                fact_data.append((
                    date_key,
                    scheduled_time_key,
                    delivered_time_key,
                    vehicle_key,
                    driver_key,
                    route_key,
                    customer_key,
                    int(row['delivery_id']),
                    int(row['trip_id']),
                    str(row['tracking_number']),
                    float(row['package_weight_kg']),
                    float(row['distance_km']),
                    float(row['fuel_consumed_liters']) if pd.notna(row['fuel_consumed_liters']) else None,
                    float(row['delivery_time_minutes']),
                    float(row['delay_minutes']),
                    float(row['deliveries_per_hour']) if pd.notna(row['deliveries_per_hour']) else None,
                    float(row['fuel_efficiency_km_per_liter']) if pd.notna(row['fuel_efficiency_km_per_liter']) else None,
                    float(row['cost_per_delivery']),
                    float(row['revenue_per_delivery']),
                    bool(row['is_on_time']),
                    False,                       # is_damaged: no disponible en origen
                    bool(row['has_signature']),  # FIX #6: boolean correcto
                    str(row['delivery_status']),
                    self.batch_id
                ))

            cursor.executemany("""
                INSERT INTO fact_deliveries (
                    date_key, scheduled_time_key, delivered_time_key,
                    vehicle_key, driver_key, route_key, customer_key,
                    delivery_id, trip_id, tracking_number,
                    package_weight_kg, distance_km, fuel_consumed_liters,
                    delivery_time_minutes, delay_minutes, deliveries_per_hour,
                    fuel_efficiency_km_per_liter, cost_per_delivery, revenue_per_delivery,
                    is_on_time, is_damaged, has_signature, delivery_status,
                    etl_batch_id
                ) VALUES (
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s
                )
            """, fact_data)

            self.sf_conn.commit()
            self.metrics['records_loaded'] = len(fact_data)
            logging.info(f"Cargados {len(fact_data)} registros en fact_deliveries")

        except Exception as e:
            logging.error(f"Error cargando hechos: {e}")
            self.sf_conn.rollback()
            self.metrics['errors'] += 1

    def run_etl(self):
        """Ejecutar pipeline ETL completo"""
        start_time = datetime.now()
        logging.info(f"Iniciando ETL - Batch ID: {self.batch_id}")

        try:
            if not self.connect_databases():
                return

            df = self.extract_daily_data()
            if df.empty:
                logging.warning("No hay registros para procesar.")
                self.close_connections()
                return

            df_transformed = self.transform_data(df)
            if df_transformed.empty:
                logging.warning("El DataFrame quedó vacío tras la transformación.")
                self.close_connections()
                return

            # load_dimensions retorna los mapeos; load_facts los consume
            key_maps = self.load_dimensions(df_transformed)
            if key_maps:
                self.load_facts(df_transformed, key_maps)

            self._calculate_daily_totals()
            self.close_connections()

            duration = (datetime.now() - start_time).total_seconds()
            logging.info(f"ETL completado en {duration:.2f} segundos")
            logging.info(f"Métricas: {json.dumps(self.metrics, indent=2)}")

        except Exception as e:
            logging.error(f"Error fatal en ETL: {e}")
            self.metrics['errors'] += 1
            self.close_connections()

    def _calculate_daily_totals(self):
        """Pre-calcular totales para reportes rápidos"""
        cursor = self.sf_conn.cursor()
        
        try:
            # Crear tabla de totales si no existe
            cursor.execute(""" CREATE TABLE IF NOT EXISTS daily_delivery_totals (
            date_key INTEGER,
            total_deliveries INTEGER,
            total_revenue NUMBER(18,2),
            total_cost NUMBER(18,2),
            avg_delivery_time_minutes NUMBER(10,2),
            on_time_percentage NUMBER(5,2),
            etl_batch_id INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);""")
            
            # Insertar totales del día
            cursor.execute(""" INSERT INTO daily_delivery_totals (
            date_key,
            total_deliveries,
            total_revenue,
            total_cost,
            avg_delivery_time_minutes,
            on_time_percentage,
            etl_batch_id
            )
            SELECT
                date_key,
                COUNT(*),
                SUM(revenue_per_delivery),
                SUM(cost_per_delivery),
                AVG(delivery_time_minutes),
                AVG(CASE WHEN is_on_time THEN 1 ELSE 0 END) * 100,
                %s
            FROM fact_deliveries
            WHERE etl_batch_id = %s
            GROUP BY date_key """, (self.batch_id, self.batch_id))
            
            self.sf_conn.commit()
            logging.info(" Totales diarios calculados")
            
        except Exception as e:
            logging.error(f" Error calculando totales: {e}")

    def close_connections(self):
        """Cerrar conexiones a bases de datos"""
        if self.pg_conn:
            self.pg_conn.close()
        if self.sf_conn:
            self.sf_conn.close()
        logging.info("Conexiones cerradas")


def job():
    etl = FleetLogixETL()
    etl.run_etl()


def main():
    """Función principal - Automatización diaria"""
    logging.info("Pipeline ETL FleetLogix iniciado")

    schedule.every().day.at("02:00").do(job)

    logging.info("ETL programado para ejecutarse diariamente a las 2:00 AM")
    logging.info("Presiona Ctrl+C para detener")

    # Ejecutar una vez al inicio para pruebas
    job()

    while True:
        schedule.run_pending()
        time.sleep(60)


if __name__ == "__main__":
    main()
