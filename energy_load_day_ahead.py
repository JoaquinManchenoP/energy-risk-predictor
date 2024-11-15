import os
import requests
import pandas as pd
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET
from dotenv import load_dotenv

load_dotenv()

# Define your ENTSO-E API key here
API_KEY = os.getenv("API_KEY")

# Base URL for ENTSO-E API
BASE_URL = "https://web-api.tp.entsoe.eu/api"

# Define the date range
days_back = 3  # Number of days back to start fetching data
last_day = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

print(last_day)
# Specify the last day in YYYY-MM-DD format

# Convert last_day to a datetime object
end_date = datetime.strptime(last_day, "%Y-%m-%d")
start_date = end_date - timedelta(days=days_back - 1)

timezone_offset = 1  # CET during winter (UTC+1). Use 2 for summer time (CEST).

# Convert local time to UTC for start and end periods
utc_start = (start_date - timedelta(hours=timezone_offset)).strftime("%Y%m%d%H%M")
utc_end = ((end_date + timedelta(days=1)) - timedelta(hours=timezone_offset)).strftime("%Y%m%d%H%M")

# Country codes dictionary (country -> list of country codes)
country_codes = {
    "Ireland": ["10Y1001A1001A016", "10YIE-1001A00010", "10Y1001A1001A59C", "10Y1001A1001A63L"],
}

# Function to fetch data for a specific period
def fetch_day_ahead_load(start_str, end_str, country_code="10YCZ-CEPS-----N"):
    url = (f"{BASE_URL}?documentType=A65&processType=A01&outBiddingZone_Domain={country_code}"
           f"&periodStart={start_str}&periodEnd={end_str}&securityToken={API_KEY}")
    
    response = requests.get(url)
    
    if response.status_code == 200:
        return response.text
    else:
        print(f"Error fetching data for {country_code}: {response.status_code}")
        return None

# Parse XML and format data as required
def parse_and_format_data(xml_data, country_name, timezone_offset):
    root = ET.fromstring(xml_data)
    ns = {'ns': 'urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0'}
    time_series = root.findall('.//ns:TimeSeries', ns)
    
    formatted_data = []
    
    for ts in time_series:
        time_period = ts.find('.//ns:Period', ns)
        period_start = time_period.find('.//ns:timeInterval/ns:start', ns).text
        period_end = time_period.find('.//ns:timeInterval/ns:end', ns).text
        
        points = time_period.findall('.//ns:Point', ns)
        
        # Convert period start to datetime
        period_start_dt = datetime.strptime(period_start, "%Y-%m-%dT%H:%MZ")
        period_end_dt = datetime.strptime(period_end, "%Y-%m-%dT%H:%MZ")
        
        # Adjust timestamps to the local timezone
        local_start_dt = period_start_dt + timedelta(hours=timezone_offset)
        local_end_dt = period_end_dt + timedelta(hours=timezone_offset)

        for point in points:
            position = point.find('.//ns:position', ns).text
            quantity = point.find('.//ns:quantity', ns).text
            timestamp_utc = period_start_dt + timedelta(hours=int(position) - 1)
            timestamp_local = timestamp_utc + timedelta(hours=timezone_offset)
            
            # Include only timestamps within the requested range
            if start_date.date() <= timestamp_local.date() <= end_date.date():
                formatted_data.append({
                    'timestamp': timestamp_local,
                    'load_value': quantity,
                    'day_of_week': timestamp_local.weekday(),
                    'country': country_name
                })
    
    df = pd.DataFrame(formatted_data)
    return df

# Initialize an empty list to collect all data for the CSV file
all_data = []

# Loop through all countries and fetch their data
for country_name, codes in country_codes.items():
    data_available = False
    
    for country_code in codes:
        print(f"Fetching data for {country_name} ({country_code}) from {utc_start} to {utc_end}")
        data = fetch_day_ahead_load(utc_start, utc_end, country_code)
        
        if data:
            print(f"Data fetched successfully for {country_name} ({country_code})")
            formatted_df = parse_and_format_data(data, country_name, timezone_offset)
            all_data.append(formatted_df)  
            data_available = True
        else:
            print(f"No data available for {country_name} ({country_code})")
    
    if not data_available:
        formatted_data_unavailable = pd.DataFrame([{
            'timestamp': 'N/A',
            'load_value': 'data_unavailable',
            'day_of_week': 'N/A',
            'country': country_name
        }])
        all_data.append(formatted_data_unavailable)  

# If data is collected, save it to a CSV file
if all_data:
    final_df = pd.concat(all_data, ignore_index=True)
    output_filename = f"day_ahead_load_{start_date.strftime('%Y%m%d')}_to_{end_date.strftime('%Y%m%d')}.csv.gz"
    
    # Remove any pre-existing file before saving new data
    if os.path.exists(output_filename):
        os.remove(output_filename)
    
    # Save the data to a CSV file with gzip compression
    final_df.to_csv(output_filename, index=False, compression='gzip', mode='w')
    print(f"Data saved to {output_filename}")
else:
    print("No data available to save.")



# country_codes = {
#     # "Ireland": ["10Y1001A1001A016", "10YIE-1001A00010", "10Y1001A1001A59C", "10Y1001A1001A63L"],
#     # "Estonia": ["10Y1001A1001A39I"],
#     # "United_Kingdom": ["10Y1001A1001A92E", "10YGB----------A"],
#     # "Germany": ["10Y1001A1001A83F", "10Y1001A1001A63L", "10YDE-ENBW-----N", "10YDE-EON------1", "10YDE-RWENET---I", "10YDE-VE-------2"],
#     # "France": ["10YFR-RTE------C"],
#     # "Sweden": ["10Y1001A1001A44P", "10Y1001A1001A45N", "10Y1001A1001A46L", "10Y1001A1001A47J"],
#     # "Norway": ["10YNO-0--------C", "10YNO-1--------2", "10YNO-2--------T", "10YNO-3--------J", "10YNO-4--------9", "10Y1001A1001A48H"],
#     # "Russia": ["10Y1001A1001A49F", "10Y1001A1001A50U"],
#     # "Belarus": ["10Y1001A1001A51S", "10Y1001A1001B004"],
#     # "Italy": ["10Y1001A1001A67D", "10Y1001A1001A68B", "10Y1001A1001A70O", "10Y1001A1001A71M", "10Y1001A1001A75E", "10Y1001A1001A74G", "10Y1001A1001A73I", "10Y1001A1001A788", "10Y1001A1001A796"],
#     # "Austria": ["10YAT-APG------L"],
#     # "Denmark": ["10Y1001A1001A65H", "10Y1001A1001A64J", "10YDK-1--------W", "10YDK-2--------M"],
#     # "Finland": ["10YFI-1--------U"],
#     # "Spain": ["10YES-REE------0"],
#     # "Portugal": ["10YPT-REN------W"],
#     # "Poland": ["10YPL-AREA-----S"],
#     # "Czech_Republic": ["10YCZ-CEPS-----N"],
#     # "Slovakia": ["10YSK-SEPS-----K"],
#     # "Switzerland": ["10YCH-SWISSGRIDZ"],
#     # "Netherlands": ["10YNL----------L"],
#     # "Belgium": ["10YBE----------2"],
#     # "Luxembourg": ["10YLU-CEGEDEL-NQ"],
#     # "Lithuania": ["10YLT-1001A0008Q"],
#     # "Latvia": ["10YLV-1001A00074"],
#     # "North_Macedonia": ["10YMK-MEPSO----8"],
#     # "Greece": ["10YGR-HTSO-----Y"],
#     # "Croatia": ["10YHR-HEP------M"],
#     # "Hungary": ["10YHU-MAVIR----U"],
#     # "Bosnia_and_Herzegovina": ["10YBA-JPCC-----D"],
#     # "Montenegro": ["10YCS-CG-TSO---S"],
#     # "Serbia": ["10YCS-SERBIATSOV"],
#     # "Albania": ["10YAL-KESH-----5"],
#     # "Romania": ["10YRO-TEL------P"],
#     # "Bulgaria": ["10YCA-BULGARIA-R"],
#     # "Turkey": ["10YTR-TEIAS----W"],
#     # "Ukraine": ["10Y1001C--000182", "10Y1001C--00031A", "10Y1001C--00038X"],
#     # "Moldova": ["10Y1001A1001A990"],
#     # "Armenia": ["10Y1001A1001B004"],
#     # "Georgia": ["10Y1001A1001B012"],
#     # "Azerbaijan": ["10Y1001A1001B05V"],
#     # "Malta": ["10Y1001A1001A93C"],
#     # "Iceland": ["IS"],
#     # "Kosovo": ["10Y1001C--00100H"],
#     # "Cyprus": ["10YCY-1001A0003J"]
# }