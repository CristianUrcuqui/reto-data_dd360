import requests
import json
import gzip
import io
import pandas as pd
import logging


class WeatherForecast:
    def __init__(self, url, headers):
        self.url = url
        self.headers = headers

    def fetch_weather_data(self):
        try:
            response = requests.get(self.url, headers=self.headers)
            logging.info(f"HTTP Status Code: {response.status_code}")
            
            if response.status_code != 200:
                logging.error(f"Failed to fetch data: {response.content}")
                return None
            
            gz_stream = io.BytesIO(response.content)
            
            with gzip.GzipFile(fileobj=gz_stream, mode="rb") as f:
                json_data = json.load(f)
            
            if not json_data:
                logging.error("JSON data is empty")
                return None

            df_data = [item for item in json_data]
            data = pd.DataFrame(df_data)
            return data

        except Exception as e:
            logging.error(f"Error fetching weather data: {str(e)}")
            return None

    def get_latest_records(self, data):
        if data is None:
            logging.error("Cannot get latest records as the data is None.")
            return None

        data['hloc'] = pd.to_datetime(data['hloc'])
        last_values = data.sort_values(by='hloc').groupby('nmun', as_index=False).last()
        return last_values

    def save_to_local(self, df, file_path):
        if df is None:
            logging.error("Cannot save data to local as DataFrame is None.")
            return False

        try:
            df.to_csv(file_path, index=False)
            logging.info(f"Successfully saved data to {file_path}")
            return True
        except Exception as e:
            logging.error(f"Failed to save data to local: {str(e)}")
            return False

def run_weather_forecast_pipeline(url, headers, file_name):
    weather_forecast = WeatherForecast(url, headers)
    data = weather_forecast.fetch_weather_data()
    latest_data = weather_forecast.get_latest_records(data)
    weather_forecast.save_to_local(latest_data, file_name)
