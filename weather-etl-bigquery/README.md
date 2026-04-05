# Weather ETL Pipeline
![Python](https://img.shields.io/badge/Python-3.11-blue)
![GCP](https://img.shields.io/badge/GCP-Cloud_Functions-orange)
![BigQuery](https://img.shields.io/badge/BigQuery-Enabled-green)

A serverless ETL pipeline deployed on GCP Cloud Functions that extracts real-time weather data from OpenWeatherMap API and loads it into BigQuery for analysis.

## Architecture
OpenWeatherMap API → Cloud Functions (HTTP Trigger) → BigQuery

## Tech Stack
- GCP Cloud Functions
- BigQuery
- OpenWeatherMap API
- Python

## How It Works
1. Triggered via HTTP request
2. Fetches current weather data for 5 cities (Toronto, Beijing, London, New York, Tokyo)
3. Loads results into BigQuery table

## Deployment
### Cloud Functions (GCP)
![Cloud Functions](images/cloud-functions.png)

### BigQuery Output
![BigQuery Preview](images/bigquery-preview.png)

## Setup
Set the following environment variable in GCP Cloud Functions:
- `OPENWEATHER_API_KEY`: use your own OpenWeatherMap API key