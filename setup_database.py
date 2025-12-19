import sqlite3
from datetime import datetime, timedelta
import sys
from pathlib import Path

# Setup Python path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


DB_FILE = "data/fake_dwh.db"

def setup_database():
    """Create database schema"""
    print("Setting up database schema...")
    
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # Date dimension
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dim_date (
            date TEXT PRIMARY KEY,
            year INTEGER NOT NULL,
            month INTEGER NOT NULL,
            day INTEGER NOT NULL
        )
    """)
    
    # FX rates fact table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS fact_fx_rates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            base_currency TEXT NOT NULL,
            quote_currency TEXT NOT NULL,
            rate REAL NOT NULL,
            source TEXT NOT NULL,
            loaded_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (date) REFERENCES dim_date(date),
            UNIQUE(date, base_currency, quote_currency)
        )
    """)
    
    # Create indexes
    #cursor.execute("CREATE INDEX IF NOT EXISTS idx_date ON fact_fx_rates(date)")
    #cursor.execute("CREATE INDEX IF NOT EXISTS idx_currencies ON fact_fx_rates(base_currency, quote_currency)")
    
    conn.commit()
    conn.close()
    print("Database schema created successfully")




def populate_date_dimension(start_date, end_date):
    """Populate date dimension table"""
    
    print(f"Populating date dimension from {start_date} to {end_date}...")
    
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    current = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    
    dates = []
    while current <= end:
        dates.append((
            current.strftime('%Y-%m-%d'),
            current.year,
            current.month,
            current.day
        ))
        current += timedelta(days=1)
    
    cursor.executemany(
        "INSERT OR IGNORE INTO dim_date (date, year, month, day) VALUES (?, ?, ?, ?)",
        dates
    )
    
    conn.commit()
    conn.close()

    print(f"Dim date table opulated with {len(dates)} dates")