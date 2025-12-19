
import requests
from datetime import datetime

START_DATE = "2025-01-01"
END_DATE = datetime.now().strftime('%Y-%m-%d')


def fetch_ecb_rates(currency, start_date, end_date):
    """Fetch FX rates from ECB API for a single currency"""
    if currency == 'EUR':
        return {}  # EUR/EUR = 1.0, handled separately
    
    url = f"https://data-api.ecb.europa.eu/service/data/EXR/D.{currency}.EUR.SP00.A"
    params = {
        'startPeriod': start_date,
        'endPeriod': end_date,
        'format': 'jsondata'
    }
    
    print(f"Fetching {currency}/EUR from ECB...")
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        rates = {}
        observations = data.get('dataSets', [{}])[0].get('series', {})
        
        if observations:
            series_key = list(observations.keys())[0]
            obs_data = observations[series_key].get('observations', {})
            time_periods = data.get('structure', {}).get('dimensions', {}).get('observation', [{}])[0].get('values', [])
            
            for time_idx, obs_value in obs_data.items():
                date_str = time_periods[int(time_idx)]['id']
                rate = float(obs_value[0])
                rates[date_str] = rate
        
        print(f"Fetched {len(rates)} rates for {currency}")
        return rates
        
    except Exception as e:
        print(f"Error fetching {currency}: {e}")
        return {}


def extract_rates(currencies, start_date, end_date):

    print("Starting extraction from ECB...")

    all_rates = {}

    for currency in currencies:
        if currency == 'EUR':
            continue

        rates = fetch_ecb_rates(currency, start_date, end_date)

        all_rates[currency] = rates

    
    return all_rates