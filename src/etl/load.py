import sqlite3
from datetime import datetime , timedelta

FULL_LOAD_START_DATE = "2024-01-01"

def determine_window_to_load(db_path):
    """
    Determine date range to load based on existing data
    Returns: (start_date, end_date, is_full_load)
    """

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Check if table exists and has data
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='fact_fx_rates'")
    table_exists = cursor.fetchone() is not None
    
    if not table_exists:
        conn.close()
        print(" No data found: FULL LOAD (entire year)")
        return FULL_LOAD_START_DATE, datetime.now().strftime('%Y-%m-%d'), True
    
    # Check latest date in database
    cursor.execute("SELECT MAX(date) FROM fact_fx_rates")
    result = cursor.fetchone()
    max_date = result[0] if result and result[0] else None
    
    conn.close()
    
    if not max_date:
        print(" Empty database: FULL LOAD (entire year)")
        return FULL_LOAD_START_DATE, datetime.now().strftime('%Y-%m-%d'), True
    
    # Parse latest date
    latest_date = datetime.strptime(max_date, '%Y-%m-%d')
    today = datetime.now()
    
    # Check if we're up to date
    if latest_date.date() >= today.date():
        print(f" Database is current (latest: {max_date}): NO LOAD NEEDED")
        return None, None, False
    
    # Calculate days behind
    days_behind = (today.date() - latest_date.date()).days
    
    if days_behind == 1:
        print(f" Database is 1 day behind: INCREMENTAL LOAD (today only)")
        return today.strftime('%Y-%m-%d'), today.strftime('%Y-%m-%d'), False
    
    else:
        # Load from day after latest to today
        start_date = (latest_date + timedelta(days=1)).strftime('%Y-%m-%d')
        end_date = today.strftime('%Y-%m-%d')
        print(f" Database is {days_behind} days behind: INCREMENTAL LOAD ({start_date} to {end_date})")
        return start_date, end_date, False




def load_to_database(rates, db_path):
    """Load rates into SQLite database"""

    print("Loading rates into database...")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Prepare data for bulk insert
    data = [
        (r['date'], r['base_currency'], r['quote_currency'], r['rate'], r['source'])
        for r in rates
    ]
    
    cursor.executemany("""
        INSERT OR REPLACE INTO fact_fx_rates (date, base_currency, quote_currency, rate, source)
        VALUES (?, ?, ?, ?, ?)
    """, data)
    
    conn.commit()
    loaded = cursor.rowcount
    conn.close()
    
    print(f"Loaded {loaded} rates into database")
    return loaded