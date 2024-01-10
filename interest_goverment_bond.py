import sqlite3 as sql
import pandas as pd
from datetime import date, timedelta, datetime

conn = sql.connect("invest_tracker_data_base")

"""
    Note:
    - This is module wich is part of Invest Tracker aplication, if you want use it separately you need make changes! More info below.

    Retrieves the current market value of a government bond based on its ticker symbol.

    Parameters:
    - ticker (str): Ticker symbol representing the government bond. It should start with 'EDO', 'COI', 'ROS', or 'ROD'.
    - date_param (str): Date parameter in the format 'YYYY-MM-DD' representing the evaluation date.
                       Defaults to the current date.

    Returns:
    - tuple: A tuple containing the current market price and the currency.
             - If the bond is not found in the specified tables ('EDO', 'COI', 'ROS', 'ROD'), returns (None, None).
             - If the bond is found, calculates the market value based on the provided date_param and returns (last_market_price, currency).

    Note:
    - This is module wich is part of Invest Tracker aplication, if you want use it separately you need make changes!
    - The function uses an SQLite connection (conn) and assumes the existence of a 'Transactions' table with a 'date_of_purchase' column.
    - Bond interest rates and details are retrieved from the specified table ('EDO', 'COI', 'ROS', 'ROD').
"""

def get_current_bond_value(ticker: str, date_param: str= str(date.today())):
    if ticker[:3] in ['EDO', 'COI', 'ROS', 'ROD']:
        table_name = ticker[:3]
        data_interest = pd.read_sql(f"SELECT * FROM {table_name}", conn)
        data_transactions = pd.read_sql("SELECT * FROM Transactions", conn)
        date_of_buy = list(data_transactions.loc[data_transactions.yahoo_ticker == ticker]['date_of_purchase'])
        date_of_buy = date_of_buy[0]
        date_of_buy = datetime.strptime(date_of_buy, "%Y-%m-%d").date()
        date_param = datetime.strptime(date_param, "%Y-%m-%d").date()
        delta = date_param - date_of_buy
        years = delta.days // 365
        days = delta.days - years*365
        if years >= 0:
            k=100  #Prize of 1 unit of bond
            for i in range(1, years+1):
                interest = float(data_interest.loc[data_interest['seria'] == ticker][f'oprocentowanie_{i}rok'])
                k= k*(1 + interest)
                k = float("{:.2f}".format(k))
            if days != 0:
                interest = float(data_interest.loc[data_interest['seria'] == ticker][f'oprocentowanie_{years+1}rok'])
                k= k*(1+interest*days/365)
                k = float("{:.2f}".format(k))
        else:
            k = None

        currency = 'PLN'
        last_market_price = k

    return  last_market_price, currency

if __name__ == "__main__":
    last_market_price, currency = get_current_bond_value('EDO0132')
    print(f"Obligacje EDO0132. Aktualna wartość: {last_market_price}")