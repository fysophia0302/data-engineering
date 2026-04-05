import os
import requests
from datetime import datetime
from google.cloud import bigquery
import functions_framework


@functions_framework.http
def weather_etl(request):
    api_key = os.environ.get("OPENWEATHER_API_KEY")
    base_url = "https://api.openweathermap.org/data/2.5/weather"
    cities = ["Toronto", "Beijing", "London", "New York", "Tokyo"]

    results = []
    for city in cities:
        url = f"{base_url}?q={city}&appid={api_key}&units=metric"
        response = requests.get(url)
        data = response.json()

        # skip cities with failed API response
        if str(data.get("cod")) != "200":
            continue

        results.append({
            "city": city,
            "description": data["weather"][0]["description"],
            "temperature_C": data["main"]["temp"],
            "pressure_hPa": data["main"]["pressure"],
            "humidity_pct": data["main"]["humidity"],
            "wind_speed_mps": data["wind"]["speed"],
            "timestamp": datetime.utcnow().isoformat()
        })

    # load results into BigQuery
    client = bigquery.Client()
    table_id = "crypto-minutia-462600-t8.weather_data_2025.weather_log"
    errors = client.insert_rows_json(table_id, results)

    if not errors:
        return "Data successfully written to BigQuery", 200
    return f"BigQuery insert failed: {errors}", 500