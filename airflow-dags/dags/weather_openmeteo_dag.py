from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import io
import openmeteo_requests
import requests_cache
from retry_requests import retry

with DAG(
    dag_id="weather_openmeteo_etl_fast",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=True,
    max_active_runs=1,
    tags=["weather", "fast"],
) as dag:

    @task
    def fetch_and_load(**context):

        logical_date = context["logical_date"].date()
        today = datetime.utcnow().date()
        past_days = max((today - logical_date).days, 1)

        # =============================
        # API CALL
        # =============================

        cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
        retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
        openmeteo = openmeteo_requests.Client(session=retry_session)

        params = {
            "latitude": 43.15,
            "longitude": 76.54,
            "daily": [
                "weather_code","temperature_2m_max","temperature_2m_min",
                "uv_index_max","daylight_duration","sunshine_duration",
                "sunrise","sunset","snowfall_sum","rain_sum",
                "wind_speed_10m_max"
            ],
            "hourly": [
                "temperature_2m","relative_humidity_2m",
                "rain","snowfall","wind_speed_10m","visibility"
            ],
            "timezone": "auto",
            "past_days": past_days
        }

        responses = openmeteo.weather_api(
            "https://api.open-meteo.com/v1/forecast",
            params=params
        )

        response = responses[0]

        # =============================
        # DATAFRAME BUILD
        # =============================

        hourly = response.Hourly()

        hourly_df = pd.DataFrame({
            "date": pd.date_range(
                start=pd.to_datetime(hourly.Time() + response.UtcOffsetSeconds(), unit="s", utc=True),
                end=pd.to_datetime(hourly.TimeEnd() + response.UtcOffsetSeconds(), unit="s", utc=True),
                freq=pd.Timedelta(seconds=hourly.Interval()),
                inclusive="left"
            ),
            "temperature_2m": hourly.Variables(0).ValuesAsNumpy(),
            "relative_humidity_2m": hourly.Variables(1).ValuesAsNumpy(),
            "rain": hourly.Variables(2).ValuesAsNumpy(),
            "snowfall": hourly.Variables(3).ValuesAsNumpy(),
            "wind_speed_10m": hourly.Variables(4).ValuesAsNumpy(),
            "visibility": hourly.Variables(5).ValuesAsNumpy(),
        })

        hourly_df["city"] = "Almaty"

        # =============================
        # COPY TO POSTGRES
        # =============================

        hook = PostgresHook(postgres_conn_id="postgres_weather")
        conn = hook.get_conn()
        cursor = conn.cursor()

        buffer = io.StringIO()
        hourly_df.to_csv(buffer, index=False, header=False)
        buffer.seek(0)

        cursor.copy_expert(
            """
            COPY pet_project.weather_hourly
            (date, temperature_2m, relative_humidity_2m, rain,
             snowfall, wind_speed_10m, visibility, city)
            FROM STDIN WITH CSV
            """,
            buffer
        )

        conn.commit()
        cursor.close()
        conn.close()

    fetch_and_load()
