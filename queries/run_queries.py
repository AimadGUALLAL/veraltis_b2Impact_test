import sqlite3
import pandas as pd



DB_PATH = 'data/fake_dwh.db'


def run_query(db_path, query):
    """ 
    This function is responsible for testing the queries on our data collected YTD calculations 
    I will use pandas to get the results to simplify
    """

    try:

        conn = sqlite3.connect(db_path)
        df = pd.read_sql_query(query, conn)


        return df
    
    except Exception as e:

        raise f"Error executing the query : {e}"



def main():

    # 1. Simple Rate Lookup
    # to get latest NOK/EUR  rate ex 1euro is around 11.9 NOK
    simple_query = """ 
        SELECT date , base_currency , quote_currency , rate FROM fact_fx_rates 
        WHERE date = (SELECT MAX(date) FROM fact_fx_rates)
        AND base_currency = 'EUR'
        AND quote_currency = 'NOK'
    """

    df_example_lookup = run_query(DB_PATH, simple_query)

    print("Displaying latest NOK/EUR rate exemple : \n", df_example_lookup)


    # 2. some YTD statistics 
    # for some selected currencies for this current year 2025

    query_stats = """ 
        SELECT base_currency , quote_currency , 
            count(*) as days_count , 
            AVG(rate) as ytd_avg_rate,
            MIN(rate) as ytd_min_rate, 
            MAX(rate) as ytd_max_rate

        FROM fact_fx_rates WHERE quote_currency IN ('NOK', 'SEK', 'PLN')
        AND base_currency = 'EUR'
        AND date >= '2025-01-01'
        GROUP BY base_currency, quote_currency
    """

    df_stats = run_query(DB_PATH,query_stats )

    print("Displaying Year-to-date average, min, max for selected currencies : \n", df_stats)

    # 3. YTD change percentage
    # now we will examinate the currency appreciation or depreciation since 1 January of this year

    ytd_query = """ 
        WITH ytd_rates AS (
            SELECT 
                base_currency,
                date,
                rate,
                FIRST_VALUE(rate) OVER (
                    PARTITION BY base_currency 
                    ORDER BY date
                ) AS first_rate
            FROM fact_fx_rates
            WHERE date >= '2025-01-01'
                AND quote_currency = 'EUR'
        )
        SELECT DISTINCT
            base_currency,
            ROUND(first_rate, 6) AS jan_1_rate,
            ROUND(rate, 6) AS latest_rate,
            ROUND(((rate - first_rate) / first_rate * 100), 2) AS ytd_change_pct
        FROM ytd_rates
        WHERE date = (SELECT MAX(date) FROM ytd_rates)
        ORDER BY ABS(((rate - first_rate) / first_rate * 100)) DESC
    """

    df_ytd_change = run_query(DB_PATH, ytd_query)

    print("Displaying ytd percentage changes since January 2025 : \n" , df_ytd_change)

    return 0


if __name__ == "__main__":

    main()




