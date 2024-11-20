import os
import requests
import pandas as pd
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET
from dotenv import load_dotenv
import time  # For adding retries

load_dotenv()

API_KEY = os.getenv("API_KEY")
BASE_URL = "https://web-api.tp.entsoe.eu/api"
days_back = 1  # Number of days back to start fetching data needs to be 1 or more
last_day = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

end_date = datetime.strptime(last_day, "%Y-%m-%d")
start_date = end_date - timedelta(days=days_back - 1)
timezone_offset = 1

utc_start = (start_date - timedelta(hours=timezone_offset)).strftime("%Y%m%d%H%M")
utc_end = ((end_date + timedelta(days=1)) - timedelta(hours=timezone_offset)).strftime("%Y%m%d%H%M")

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
    "Switzerland": ["10YCH-SWISSGRIDZ"],
}


def fetch_day_ahead_load(start_str, end_str, country_code):
    url = (f"{BASE_URL}?documentType=A65&processType=A01&outBiddingZone_Domain={country_code}"
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
            time.sleep(2)  # Retry delay
    return None

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
                data_type = "real" if timestamp_local.date() <= datetime.now().date() else "forecast"  # Label by date
                formatted_data.append({
                    'timestamp': timestamp_local,
                    'load_value': quantity,
                    'day_of_week': timestamp_local.weekday(),
                    'country': country_name,
                    'data_type': data_type
                })
        return pd.DataFrame(formatted_data)
    except Exception as e:
        print(f"Error parsing XML for {country_name}: {e}")
        return pd.DataFrame()

all_data = []

for country_name, codes in country_codes.items():
    for country_code in codes:
        print(f"Fetching {country_name} ({country_code})...")
        xml_data = fetch_day_ahead_load(utc_start, utc_end, country_code)
        if xml_data:
            df = parse_and_format_data(xml_data, country_name, timezone_offset)
            if not df.empty:
                all_data.append(df)
                break

if all_data:
    final_df = pd.concat(all_data, ignore_index=True)
    final_df.sort_values(by=['country', 'timestamp'], inplace=True)  # Sort by country and timestamp

    # Save file to thesis-data/data/load
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))  # thesis-data root folder
    output_dir = os.path.join(base_dir, "data", "load")
    os.makedirs(output_dir, exist_ok=True)  # Ensure directory exists
    filename = os.path.join(output_dir, f"all_countries_load_{start_date.strftime('%Y%m%d')}_to_{end_date.strftime('%Y%m%d')}.csv.gz")
    final_df.to_csv(filename, index=False, compression='gzip')
    print(f"Saved all data to {filename}")
else:
    print("No data available.")




# country_codes = {
#     "Ireland": ["10Y1001A1001A016", "10YIE-1001A00010", "10Y1001A1001A59C", "10Y1001A1001A63L"],
#     "Estonia": ["10Y1001A1001A39I"],
#     "United_Kingdom": ["10Y1001A1001A92E", "10YGB----------A"],
#     "Germany": ["10Y1001A1001A83F", "10Y1001A1001A63L", "10YDE-ENBW-----N", "10YDE-EON------1", "10YDE-RWENET---I", "10YDE-VE-------2"],
#     "France": ["10YFR-RTE------C"],
#     "Sweden": ["10Y1001A1001A44P", "10Y1001A1001A45N", "10Y1001A1001A46L", "10Y1001A1001A47J"],
#     "Norway": ["10YNO-0--------C", "10YNO-1--------2", "10YNO-2--------T", "10YNO-3--------J", "10YNO-4--------9", "10Y1001A1001A48H"],
#     "Russia": ["10Y1001A1001A49F", "10Y1001A1001A50U"],
#     "Belarus": ["10Y1001A1001A51S", "10Y1001A1001B004"],
#     "Italy": ["10Y1001A1001A67D", "10Y1001A1001A68B", "10Y1001A1001A70O", "10Y1001A1001A71M", "10Y1001A1001A75E", "10Y1001A1001A74G", "10Y1001A1001A73I", "10Y1001A1001A788", "10Y1001A1001A796"],
#     "Austria": ["10YAT-APG------L"],
#     "Denmark": ["10Y1001A1001A65H", "10Y1001A1001A64J", "10YDK-1--------W", "10YDK-2--------M"],
#     "Finland": ["10YFI-1--------U"],
#     "Spain": ["10YES-REE------0"],
#     "Portugal": ["10YPT-REN------W"],
#     "Poland": ["10YPL-AREA-----S"],
#     "Czech_Republic": ["10YCZ-CEPS-----N"],
#     "Slovakia": ["10YSK-SEPS-----K"],
#     "Switzerland": ["10YCH-SWISSGRIDZ"],
#     "Netherlands": ["10YNL----------L"],
#     "Belgium": ["10YBE----------2"],
#     "Luxembourg": ["10YLU-CEGEDEL-NQ"],
#     "Lithuania": ["10YLT-1001A0008Q"],
#     "Latvia": ["10YLV-1001A00074"],
#     "North_Macedonia": ["10YMK-MEPSO----8"],
#     "Greece": ["10YGR-HTSO-----Y"],
#     "Croatia": ["10YHR-HEP------M"],
#     "Hungary": ["10YHU-MAVIR----U"],
#     "Bosnia_and_Herzegovina": ["10YBA-JPCC-----D"],
#     "Montenegro": ["10YCS-CG-TSO---S"],
#     "Serbia": ["10YCS-SERBIATSOV"],
#     "Albania": ["10YAL-KESH-----5"],
#     "Romania": ["10YRO-TEL------P"],
#     "Bulgaria": ["10YCA-BULGARIA-R"],
#     "Turkey": ["10YTR-TEIAS----W"],
#     "Ukraine": ["10Y1001C--000182", "10Y1001C--00031A", "10Y1001C--00038X"],
#     "Moldova": ["10Y1001A1001A990"],
#     "Armenia": ["10Y1001A1001B004"],
#     "Georgia": ["10Y1001A1001B012"],
#     "Azerbaijan": ["10Y1001A1001B05V"],
#     "Malta": ["10Y1001A1001A93C"],
#     "Iceland": ["IS"],
#     "Kosovo": ["10Y1001C--00100H"],
#     "Cyprus": ["10YCY-1001A0003J"]
# }