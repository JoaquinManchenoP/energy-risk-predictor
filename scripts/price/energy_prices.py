from datetime import datetime, timedelta
import xml.etree.ElementTree as ET
from dotenv import load_dotenv
import time
import requests_cache
import os
import requests
import pandas as pd

# Clear cache to allow retries
requests_cache.clear()

# Load environment variables from .env (ensure it contains API_KEY)
load_dotenv()
API_KEY = os.getenv("API_KEY")
BASE_URL = "https://web-api.tp.entsoe.eu/api"

# Set up time parameters (using a 1-day period for testing)
days_back = 1
last_day = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
end_date = datetime.strptime(last_day, "%Y-%m-%d")
start_date = end_date - timedelta(days=days_back - 1)
timezone_offset = 1  # e.g., CET/CEST

utc_start = (start_date - timedelta(hours=timezone_offset)).strftime("%Y%m%d%H%M")
utc_end = ((end_date + timedelta(days=1)) - timedelta(hours=timezone_offset)).strftime("%Y%m%d%H%M")
current_time = datetime.now()

# Define country codes (for testing, using one example country)
country_codes = {
    "Austria": ["10YAT-APG------L"],
}

def fetch_energy_prices(start_str, end_str, country_code):
    """
    Fetches Energy Prices data using documentType=A44.
    The request uses both out_Domain and in_Domain set to the given country code.
    """
    url = (f"{BASE_URL}?documentType=A44&out_Domain={country_code}&in_Domain={country_code}"
           f"&periodStart={start_str}&periodEnd={end_str}&securityToken={API_KEY}")
    print("Requesting URL for Energy Prices:")
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

def parse_and_format_energy_prices(xml_data, country_name, timezone_offset):
    """
    Parses the Energy Prices XML data and returns a formatted DataFrame.
    Expected XML structure:
      - Root: <Publication_MarketDocument> (namespace assumed below)
      - Contains one or more <TimeSeries> elements.
      - Each <TimeSeries> has a <Period> element with:
            <timeInterval><start>...</start><end>...</end></timeInterval>
            <resolution>PT60M</resolution> (we process only hourly data)
            multiple <Point> elements, each with:
                <position>...</position>
                <price.amount>...</price.amount>
    The resulting DataFrame contains:
      - timestamp (local time)
      - energy_price (float)
      - day_of_week (0=Monday, …, 6=Sunday)
      - country
      - data_type (set to 'energy_price')
    """
    try:
        root = ET.fromstring(xml_data)
        # Use the namespace from the documentation sample for Energy Prices:
        ns = {'ns': 'urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:3'}
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
            period_start = period_start_el.text  # e.g., "2025-02-03T23:00Z"
            # Process only if the resolution is PT60M (hourly)
            resolution = period.find('.//ns:resolution', ns)
            if resolution is None or resolution.text != "PT60M":
                continue
            points = period.findall('.//ns:Point', ns)
            print(f"For {country_name}, found {len(points)} Point elements (PT60M resolution).")
            for point in points:
                pos_el = point.find('.//ns:position', ns)
                price_el = point.find('.//ns:price.amount', ns)
                if pos_el is None or price_el is None:
                    continue
                position = int(pos_el.text)
                price = float(price_el.text)
                # Calculate the timestamp for this point (assume each point is 60 minutes apart)
                timestamp_utc = datetime.strptime(period_start, "%Y-%m-%dT%H:%MZ") + timedelta(hours=position - 1)
                timestamp_local = timestamp_utc + timedelta(hours=timezone_offset)
                formatted_data.append({
                    'timestamp': timestamp_local,
                    'energy_price': price,
                    'day_of_week': timestamp_local.weekday(),
                    'country': country_name,
                    'data_type': 'energy_price'
                })
        df = pd.DataFrame(formatted_data)
        df.sort_values(by='timestamp', inplace=True)
        return df
    except Exception as e:
        print(f"Error parsing energy prices XML for {country_name}: {e}")
        return pd.DataFrame()

# Main data retrieval loop for Energy Prices
all_data = []
for country_name, codes in country_codes.items():
    for country_code in codes:
        print(f"\nFetching Energy Prices for {country_name} ({country_code})...")
        xml_data = fetch_energy_prices(utc_start, utc_end, country_code)
        if xml_data:
            df = parse_and_format_energy_prices(xml_data, country_name, timezone_offset)
            if not df.empty:
                print(f"\nFormatted DataFrame for {country_name}:")
                print(df.head())
                all_data.append(df)
                break  # Use first non-empty result for the country
            else:
                print(f"Parsed DataFrame for {country_name} is empty.")
        else:
            print(f"No XML data returned for {country_name} using code {country_code}.")

if all_data:
    final_df = pd.concat(all_data, ignore_index=True)
    final_df.sort_values(by=['country', 'timestamp'], inplace=True)
    
    print("Final Energy Prices DataFrame preview:")
    print(final_df.head())
    
    # Build the absolute path so that the output is saved to root/data/price/
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    output_dir = os.path.join(base_dir, "data", "price")
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(
        output_dir,
        f"all_countries_energy_prices_day_ahead_{start_date.strftime('%Y%m%d')}_to_{end_date.strftime('%Y%m%d')}.csv.gz"
    )
    
    # Delete the old file if it exists
    if os.path.exists(output_path):
        os.remove(output_path)
        print(f"Existing file at {output_path} removed.")
    
    final_df.to_csv(output_path, index=False, compression="gzip")
    print(f"Saved Energy Prices data to {output_path}")
else:
    print("No Energy Prices data available.")
