

def calculate_cross_pairs(eur_rates, currencies):
    """Calculate all cross-currency pairs from EUR-based rates"""

    print("Calculating cross-currency pairs...")
    cross_rates = []
    
    # Get all unique dates
    all_dates = set()
    for currency_rates in eur_rates.values():
        all_dates.update(currency_rates.keys())
    
    for date in sorted(all_dates):
        # Build EUR-based rates for this date
        # eur_base_rates stores: EUR/XXX (how many XXX for 1 EUR)
        eur_base_rates = {}
        
        for currency, rates in eur_rates.items():
            if date in rates:
                # Store as EUR / XXX (e.g., EUR/NOK = 11.5)
                eur_base_rates[currency] = rates[date]
        
        # Now generate all possible pairs
        for base_curr in currencies:
            for quote_curr in currencies:
                if base_curr == quote_curr:
                    continue
                
                rate = None
                
                # Case 1: EUR/XXX (e.g., EUR/NOK)
                if base_curr == 'EUR' and quote_curr in eur_base_rates:
                    rate = eur_base_rates[quote_curr]
                
                # Case 2: XXX/EUR (inverse of EUR/XXX)
                # If EUR/NOK = 11.5, then NOK/EUR = 1/11.5 = 0.087
                elif quote_curr == 'EUR' and base_curr in eur_base_rates:
                    rate = 1.0 / eur_base_rates[base_curr]
                
                # Case 3: Cross-pairs XXX/YYY
                # NOK/SEK = (EUR/SEK) / (EUR/NOK)
                # Example: If EUR/NOK=11.5 and EUR/SEK=12.0
                # Then NOK/SEK = 12.0/11.5 = 1.043 (1 NOK = 1.043 SEK)
                elif base_curr in eur_base_rates and quote_curr in eur_base_rates:
                    rate = eur_base_rates[quote_curr] / eur_base_rates[base_curr]
                
                if rate is not None:
                    cross_rates.append({
                        'date': date,
                        'base_currency': base_curr,
                        'quote_currency': quote_curr,
                        'rate': rate,
                        'source': 'ECB'
                    })
    
    print(f"Calculated {len(cross_rates)} currency pair rates")
    return cross_rates