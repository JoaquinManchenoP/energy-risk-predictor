import os
import requests
import pandas as pd
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET
from dotenv import load_dotenv
import time  # For retries
import requests_cache
requests_cache.clear()

# Load environment variables
load_dotenv()

# Constants
API_KEY = os.getenv("API_KEY")
BASE_URL = "https://web-api.tp.entsoe.eu/api"
days_back = 1  # Number of days back to start fetching data
last_day = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

end_date = datetime.strptime(last_day, "%Y-%m-%d")
start_date = end_date - timedelta(days=days_back - 1)
timezone_offset = 1  # Adjust to local timezone (CET/CEST)

utc_start = (start_date - timedelta(hours=timezone_offset)).strftime("%Y%m%d%H%M")
utc_end = ((end_date + timedelta(days=1)) - timedelta(hours=timezone_offset)).strftime("%Y%m%d%H%M")

# Get the current time for filtering
current_time = datetime.now()

# Country codes (adjust as needed)
country_codes = {
    "Albania": ["10YAL-KESH-----5"],
    "Austria": ["10YAT-APG------L"],
    "Belgium": ["10YBE----------2"],
    # "Bosnia_and_Herzegovina": ["10YBA-JPCC-----D"],
    # "Bulgaria": ["10YCA-BULGARIA-R"],
    # "Croatia": ["10YHR-HEP------M"],
    # "Czech_Republic": ["10YCZ-CEPS-----N"],
    # "Denmark": ["10Y1001A1001A65H", "10Y1001A1001A64J", "10YDK-1--------W", "10YDK-2--------M"],
    # "Estonia": ["10Y1001A1001A39I"],
    # "Finland": ["10YFI-1--------U"],
    # "France": ["10YFR-RTE------C"],
    # "Georgia": ["10Y1001A1001B012"],
    # "Germany": ["10Y1001A1001A83F", "10Y1001A1001A63L", "10YDE-ENBW-----N", "10YDE-EON------1", "10YDE-RWENET---I", "10YDE-VE-------2"],
    # "Greece": ["10YGR-HTSO-----Y"],
    # "Hungary": ["10YHU-MAVIR----U"],
    # "Ireland": ["10Y1001A1001A016", "10YIE-1001A00010", "10Y1001A1001A59C", "10Y1001A1001A63L"],
    # "Italy": ["10Y1001A1001A67D", "10Y1001A1001A68B", "10Y1001A1001A70O", "10Y1001A1001A71M", "10Y1001A1001A75E", "10Y1001A1001A74G", "10Y1001A1001A73I", "10Y1001A1001A788", "10Y1001A1001A796"],
    # "Kosovo": ["10Y1001C--00100H"],
    # "Latvia": ["10YLV-1001A00074"],
    # "Lithuania": ["10YLT-1001A0008Q"],
    # "Luxembourg": ["10YLU-CEGEDEL-NQ"],
    # "Montenegro": ["10YCS-CG-TSO---S"],
    # "Netherlands": ["10YNL----------L"],
    # "North_Macedonia": ["10YMK-MEPSO----8"],
    # "Norway": ["10YNO-0--------C", "10YNO-1--------2", "10YNO-2--------T", "10YNO-3--------J", "10YNO-4--------9", "10Y1001A1001A48H"],
    # "Poland": ["10YPL-AREA-----S"],
    # "Portugal": ["10YPT-REN------W"],
    # "Romania": ["10YRO-TEL------P"],
    # "Serbia": ["10YCS-SERBIATSOV"],
    # "Spain": ["10YES-REE------0"],
    # "Sweden": ["10Y1001A1001A44P", "10Y1001A1001A45N", "10Y1001A1001A46L", "10Y1001A1001A47J"],
    # "Switzerland": ["10YCH-SWISSGRIDZ"],
}

# Function to fetch data
def fetch_actual_total_load(start_str, end_str, country_code):
    url = (f"{BASE_URL}?documentType=A65&processType=A16&outBiddingZone_Domain={country_code}"
           f"&periodStart={start_str}&periodEnd={end_str}&securityToken={API_KEY}")
    retries = 3
    for attempt in range(retries):
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                return response.text
            else:
                print(f"Error {response.status_code}: {response.text}")
        except requests.RequestException as e:
            print(f"Attempt {attempt + 1}/{retries} failed: {e}")
            time.sleep(2)
    return None

# Function to parse and format data
def parse_and_format_data(xml_data, country_name, timezone_offset):
    try:
        root = ET.fromstring(xml_data)
        ns = {'ns': 'urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0'}
        time_series = root.findall('.//ns:TimeSeries', ns)
        
        formatted_data = []
        for ts in time_series:
            time_period = ts.find('.//ns:Period', ns)
            period_start = time_period.find('.//ns:timeInterval/ns:start', ns).text
            points = time_period.findall('.//ns:Point', ns)

            for point in points:
                position = point.find('.//ns:position', ns).text
                quantity = point.find('.//ns:quantity', ns).text
                timestamp_utc = datetime.strptime(period_start, "%Y-%m-%dT%H:%MZ") + timedelta(hours=int(position) - 1)
                timestamp_local = timestamp_utc + timedelta(hours=timezone_offset)
                
                # Exclude future data
                if timestamp_local > current_time:
                    continue
                
                formatted_data.append({
                    'timestamp': timestamp_local,
                    'load_value': quantity,
                    'day_of_week': timestamp_local.weekday(),
                    'country': country_name,
                    'data_type': 'actual_load'
                })
        return pd.DataFrame(formatted_data)
    except Exception as e:
        print(f"Error parsing XML for {country_name}: {e}")
        return pd.DataFrame()

# Fetch and process data
all_data = []
for country_name, codes in country_codes.items():
    for country_code in codes:
        print(f"Fetching {country_name} ({country_code})...")
        xml_data = fetch_actual_total_load(utc_start, utc_end, country_code)
        if xml_data:
            df = parse_and_format_data(xml_data, country_name, timezone_offset)
            if not df.empty:
                all_data.append(df)
                break
    else:
        print(f"No data for {country_name}. Adding placeholder.")
        all_data.append(pd.DataFrame([{
            'timestamp': 'N/A',
            'load_value': 'data_unavailable',
            'day_of_week': 'N/A',
            'country': country_name,
            'data_type': 'actual_load'
        }]))

# Combine and save all data in one file
if all_data:
    final_df = pd.concat(all_data, ignore_index=True)
    
    # Remove rows where load_value is 'data_unavailable'
    final_df = final_df[final_df['load_value'] != 'data_unavailable']
    
    # Convert 'timestamp' to datetime, coercing errors to NaT for non-datetime entries
    final_df['timestamp'] = pd.to_datetime(final_df['timestamp'], errors='coerce')
    
    # Sort the DataFrame by 'timestamp' while keeping valid data only
    final_df = final_df.sort_values(by='timestamp').dropna(subset=['timestamp'])
    
    # Save the file to the desired path
    output_path = os.path.join("..", "..", "data", "load", f"all_countries_actual_total_load_{start_date.strftime('%Y%m%d')}_to_{end_date.strftime('%Y%m%d')}.csv.gz")
    final_df.to_csv(output_path, index=False, compression='gzip')
    print(f"Saved to {output_path}")
else:
    print("No data available.")


