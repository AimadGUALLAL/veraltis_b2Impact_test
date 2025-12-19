import sys 
from pathlib import Path

# Setup Python path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.etl.extract import extract_rates
from src.etl.transform import calculate_cross_pairs
from src.etl.load import load_to_database , determine_window_to_load

from setup_database import setup_database, populate_date_dimension


def main():

    DB_FILE = "data/fake_dwh.db" 

    setup_database()

    CURRENCIES = ['NOK', 'EUR', 'SEK', 'PLN', 'RON', 'DKK', 'CZK']

    start_date, end_date, is_full_load = determine_window_to_load(DB_FILE)

    if start_date is None: 
        return 0
    
    populate_date_dimension(start_date, end_date)
    
    eur_rates = extract_rates(currencies=CURRENCIES, start_date=start_date, end_date=end_date)

    rates = calculate_cross_pairs(eur_rates, CURRENCIES)
    
    counted_rows = load_to_database(rates, DB_FILE)

    return 0


if __name__ == "__main__":

    main()