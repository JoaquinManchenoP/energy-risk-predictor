import os
import requests
import pandas as pd
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET
from dotenv import load_dotenv
import time
import requests_cache

requests_cache.clear()

load_dotenv()
API_KEY = os.getenv("API_KEY")
BASE_URL = "https://web-api.tp.entsoe.eu/api"

days_back = 365
last_day = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
end_date = datetime.strptime(last_day, "%Y-%m-%d")
start_date = end_date - timedelta(days=days_back - 1)
timezone_offset = 1  # e.g., CET/CEST

utc_start = (start_date - timedelta(hours=timezone_offset)).strftime("%Y%m%d%H%M")
utc_end = ((end_date + timedelta(days=1)) - timedelta(hours=timezone_offset)).strftime("%Y%m%d%H%M")
current_time = datetime.now()

country_codes = {
    "Albania": ["10YAL-KESH-----5"],
    "Austria": ["10YAT-APG------L"],
    "Belgium": ["10YBE----------2"],
    "Bosnia_and_Herzegovina": ["10YBA-JPCC-----D"],
    "Bulgaria": ["10YCA-BULGARIA-R"],
    "Croatia": ["10YHR-HEP------M"],
    "Czech_Republic": ["10YCZ-CEPS-----N"],
    "Denmark": ["10Y1001A1001A65H", "10Y1001A1001A64J", "10YDK-1--------W", "10YDK-2--------M"],
    "Estonia": ["10Y1001A1001A39I"],
    "Finland": ["10YFI-1--------U"],
    "France": ["10YFR-RTE------C"],
    "Georgia": ["10Y1001A1001B012"],
    "Germany": ["10Y1001A1001A83F", "10Y1001A1001A63L", "10YDE-ENBW-----N", "10YDE-EON------1", "10YDE-RWENET---I", "10YDE-VE-------2"],
    "Greece": ["10YGR-HTSO-----Y"],
    "Hungary": ["10YHU-MAVIR----U"],
    "Ireland": ["10Y1001A1001A016", "10YIE-1001A00010", "10Y1001A1001A59C", "10Y1001A1001A63L"],
    "Italy": ["10Y1001A1001A67D", "10Y1001A1001A68B", "10Y1001A1001A70O", "10Y1001A1001A71M", "10Y1001A1001A75E", "10Y1001A1001A74G", "10Y1001A1001A73I", "10Y1001A1001A788", "10Y1001A1001A796"],
    "Kosovo": ["10Y1001C--00100H"],
    "Latvia": ["10YLV-1001A00074"],
    "Lithuania": ["10YLT-1001A0008Q"],
    "Luxembourg": ["10YLU-CEGEDEL-NQ"],
    "Montenegro": ["10YCS-CG-TSO---S"],
    "Netherlands": ["10YNL----------L"],
    "North_Macedonia": ["10YMK-MEPSO----8"],
    "Norway": ["10YNO-0--------C", "10YNO-1--------2", "10YNO-2--------T", "10YNO-3--------J", "10YNO-4--------9", "10Y1001A1001A48H"],
    "Poland": ["10YPL-AREA-----S"],
    "Portugal": ["10YPT-REN------W"],
    "Romania": ["10YRO-TEL------P"],
    "Serbia": ["10YCS-SERBIATSOV"],
    "Spain": ["10YES-REE------0"],
    "Sweden": ["10Y1001A1001A44P", "10Y1001A1001A45N", "10Y1001A1001A46L", "10Y1001A1001A47J"],
    "Switzerland": ["10YCH-SWISSGRIDZ"]
}

def fetch_generation_forecast(start_str, end_str, country_code):

    url = (f"{BASE_URL}?documentType=A71&processType=A01&in_Domain={country_code}"
           f"&periodStart={start_str}&periodEnd={end_str}&securityToken={API_KEY}")
    print("Requesting Generation Forecast URL:")
    print(url)
    retries = 3
    for attempt in range(retries):
        try:
            response = requests.get(url, timeout=10)
            print(f"Attempt {attempt+1}: Status Code: {response.status_code}")
            if response.status_code == 200:
                print("Response snippet (first 500 chars):")
                print(response.text[:500])
                return response.text
            else:
                print(f"Error {response.status_code}: {response.text}")
        except requests.RequestException as e:
            print(f"Attempt {attempt+1}/{retries} failed: {e}")
            time.sleep(2)
    return None

def parse_and_format_generation_forecast(xml_data, country_name, timezone_offset):
    try:
        root = ET.fromstring(xml_data)
        ns = {'ns': 'urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0'}
        time_series_list = root.findall('.//ns:TimeSeries', ns)
        print(f"For {country_name}, found {len(time_series_list)} TimeSeries elements.")
        
        formatted_data = []
        for ts in time_series_list:
            period = ts.find('.//ns:Period', ns)
            if period is None:
                continue
            period_start_el = period.find('.//ns:timeInterval/ns:start', ns)
            if period_start_el is None:
                continue
            period_start = period_start_el.text
            resolution = period.find('.//ns:resolution', ns)
            if resolution is None or resolution.text != "PT60M":
                continue
            points = period.findall('.//ns:Point', ns)
            print(f"For {country_name}, found {len(points)} Point elements (PT60M resolution).")
            for point in points:
                pos_el = point.find('.//ns:position', ns)
                quantity_el = point.find('.//ns:quantity', ns)
                if pos_el is None or quantity_el is None:
                    continue
                position = int(pos_el.text)
                quantity = float(quantity_el.text)
                timestamp_utc = datetime.strptime(period_start, "%Y-%m-%dT%H:%MZ") + timedelta(hours=position - 1)
                timestamp_local = timestamp_utc + timedelta(hours=timezone_offset)
                if timestamp_local > current_time:
                    continue
                formatted_data.append({
                    'timestamp': timestamp_local,
                    'generation_forecast': quantity,
                    'day_of_week': timestamp_local.weekday(),
                    'country': country_name,
                    'data_type': 'generation_forecast'
                })
        df = pd.DataFrame(formatted_data)
        if not df.empty:
            df = df.groupby(['timestamp', 'country', 'data_type'], as_index=False).agg({
                'generation_forecast': 'max',
                'day_of_week': 'first'
            })
        else:
            print("Warning: The DataFrame is empty after parsing.")
        df = df[['timestamp', 'generation_forecast', 'day_of_week', 'country', 'data_type']]
        df.sort_values(by='timestamp', inplace=True)
        return df
    except Exception as e:
        print(f"Error parsing generation forecast XML for {country_name}: {e}")
        return pd.DataFrame()

all_data = []
for country_name, codes in country_codes.items():
    for country_code in codes:
        print(f"\nFetching Generation Forecast for {country_name} ({country_code})...")
        xml_data = fetch_generation_forecast(utc_start, utc_end, country_code)
        if xml_data:
            df = parse_and_format_generation_forecast(xml_data, country_name, timezone_offset)
            if not df.empty:
                print(f"\nFormatted DataFrame for {country_name}:")
                print(df.head())
                print(f"Number of rows in the DataFrame: {len(df)}")
                all_data.append(df)
                break  # Use the first non-empty result for this country
            else:
                print(f"Parsed DataFrame for {country_name} is empty.")
        else:
            print(f"No XML data returned for {country_name} using code {country_code}.")

if all_data:
    final_df = pd.concat(all_data, ignore_index=True)
    final_df.sort_values(by=['country', 'timestamp'], inplace=True)
    
    print("Final Generation Forecast DataFrame preview:")
    print(final_df.head())
    print(f"Total number of records: {len(final_df)}")
    
    # Save the DataFrame as a CSV file compressed with gzip in root/data/generation/
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    output_dir = os.path.join(base_dir, "data", "generation")
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(
        output_dir,
        f"all_countries_generation_forecast_day_ahead_{start_date.strftime('%Y%m%d')}_to_{end_date.strftime('%Y%m%d')}.csv.gz"
    )
    final_df.to_csv(output_path, index=False, compression="gzip")
    print(f"Saved Generation Forecast data to {output_path}")
else:
    print("No Generation Forecast data available.")
