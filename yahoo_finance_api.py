"""
Note:
    - This is module wich is part of Invest Tracker aplication, if you want use it separately you need make changes! More info below.

This script provides functions to manage financial data for an investment tracker application. It includes functionalities to retrieve the last market price of various assets, download historical data for specified tickers, and update the local SQLite database.

Dependencies:
- yfinance (yf)
- decimal
- datetime
- nbp_api
- interest_government_bond (bond_value)
- sqlite3 (sql)

Functions:
1. get_last_market_price(ticker: str, date_param: str = str(date.today())) -> Tuple[float, str]:
    - Returns the last market price and currency for the given ticker.
    - If the ticker represents a government bond ('EDO', 'COI', 'ROS', 'ROD'), it utilizes the 'bond_value' module.
    - If the ticker is 'CASH', returns a default value of 1.00 PLN.
    - For other tickers, fetches information using yfinance and returns the last market price and currency.

2. download_historical_data(ticker: str, name_of_db: str, period=None, start_date=None) -> bool:
    - Downloads historical data for the specified ticker and updates the SQLite database.
    - Requires the name of the SQLite database to store the data.
    - Optional parameters:
        - period: Specifies the historical data period ('1d', '1mo', '3mo', '1y', etc.).
        - start_date: Specifies the start date for historical data retrieval.
    - Checks for existing data in the database to determine the start date for new data.
    - Prints a success or failure message based on the update status.

3. main():
    - Example usage of the 'download_historical_data' function for a specific ticker ('CDR.WA') and database ('invest_tracker_data_base').

Note:
- Ensure that all dependencies are installed before running the script.
- The script includes an example usage of the 'download_historical_data' function in the 'main' function.
- Adjustments may be needed based on specific use cases or database structures.
"""









import yfinance as yf
from decimal import *
from datetime import date, timedelta, datetime
import nbp_api
import interest_goverment_bond as bond_value
import sqlite3 as sql



#function return's actual regular market price for ticker.
def get_last_market_price(ticker: str, date_param: str= str(date.today())):
     
    
    #if ticker == "":

    if ticker[:3] in ['EDO', 'COI', 'ROS', 'ROD']:
        last_market_price, currency = bond_value.get_current_bond_value(ticker)
    elif ticker == "CASH":
        last_market_price, currency = 1.00, "PLN"
    else:
        ticker = yf.Ticker(ticker)
        stockinfo = ticker.fast_info
        last_market_price = stockinfo["last_price"]
        last_market_price = float(Decimal(last_market_price).quantize(Decimal(10) ** -3))
        currency = stockinfo["currency"]  
    
    return  last_market_price, currency


                
def download_historical_data(ticker: str,
                             name_of_db: str,
                             period=None,
                             start_date=None):
    if ticker not in ['','CASH'] and (ticker[:3] not in ['EDO', 'COI', 'ROS', 'ROD']):
        conn = sql.connect(name_of_db)
        cur = conn.cursor()
        
        if period == None and start_date == None:
            try:
                cur.execute(f"SELECT Date FROM '{ticker}'")
                last_date = cur.fetchall()[-1][0]
                last_date = last_date.split(' ')[0]
                #today = date.today()
                last_date = datetime.strptime(last_date, "%Y-%m-%d").date()
                if last_date != date.today():
                    start_date = last_date + timedelta(days = 1)
                else:
                    return False
            except sql.OperationalError:
                period = "max"
                
        if period != None:
            df = yf.Ticker(ticker).history(period = period,
                                        interval = '1d')
        else:
            df = yf.Ticker(ticker).history(start = start_date,
                                        interval = '1d')

        if not df.empty:
            df = df.reset_index()
            df = df.assign(Date =df.Date.dt.date, Year=df.Date.dt.year, Month=df.Date.dt.month, Day=df.Date.dt.day)
            df = df.set_index("Date")
            df = df.drop(columns = "Stock Splits")
            df = df.round(2)
            df.to_sql(ticker, conn, if_exists="append")

            print(f"[###############100%###############] Successfully updated '{ticker}' data's")
        else:
            print(f"[#                0%               ] Failed updating '{ticker}' data's")



def main():
    download_historical_data(ticker= "CDR.WA",
                          name_of_db="invest_tracker_data_base")
    
    # last_market_price, currency = get_last_market_price("EIMI.L")
    # print(last_market_price)
    # print(currency)


if __name__ == "__main__":
    main()