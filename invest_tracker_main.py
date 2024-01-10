"""
Invest Tracker

Note:
Please note that the code below is under development. Ultimately, the application is intended to allow manipulation and control of investment values through a GUI. 
The script, along with additional modules, serves the following purposes:
1) Initializing the database
2) Importing a list of transactions from a CSV file
3) Fetching and processing government bond interest rate tables
4) Retrieving currency exchange rates from the NBP tables
5) Calculating historical parameters of the investment portfolio (Total costs, total value, etc.)
6) Storing the calculated data in the database
7) Presenting basic charts.

Modules:
- `csv`: Module for reading and writing CSV files.
- `sqlite3`: Module for interacting with SQLite database.
- `matplotlib.pyplot`: Library for creating static, animated, and interactive visualizations.
- `plotly.express`: High-level interface for creating interactive plots.
- `pandas`: Data manipulation library.
- `datetime`: Module for working with dates and times.
- `decimal`: Module for decimal floating-point arithmetic.
- `os`: Module for interacting with the operating system.

Own Modules:
- `data_base`: Module for managing the SQLite database.
- `nbp_api`: Module for fetching exchange rates from NBP API.
- `goverment_bond_getting_table_of_interest`: Module for fetching government bond interest rates tables.
- `interest_goverment_bond`: Module for calculating interest on government bonds.
- `yahoo_finance_api`: Module for fetching financial data from Yahoo Finance.

Classes:
- `Account`: Represents an investment account with functionalities for updating, visualizing, and managing transactions.
- `Transaction`: Represents a financial transaction with functionalities for importing from CSV, sorting, and calculating transaction values.

Functions:
- `main()`: Main function that initializes the application, creates accounts, imports transactions, checks exchange rates, updates balances, and visualizes investment performance.

Usage:
1. Import the required modules.
2. Create transactions CSV file (Example file is in repository).
3. Run the invest_tracker_main script to execute the complete workflow.
"""
























import csv
import sqlite3
import matplotlib.pyplot as plt
plt.style.use('seaborn')
import plotly.express as px
import pandas as pd
from datetime import date, datetime, timedelta
import os
import os.path

import data_base
import nbp_api as nbp
import goverment_bond_getting_table_of_interest
import interest_goverment_bond as bond_interest
from decimal import *
getcontext().prec = 15
import yahoo_finance_api as yfin



#Initialiaze data base 
db = data_base.Database("invest_tracker_data_base")

if db.check_table_exists("Accounts"):
    db.drop_table("Accounts")
db.create_table("Accounts", "id INTEGER PRIMARY KEY AUTOINCREMENT", "name", "balance REAL", "currency")

if db.check_table_exists("Transactions"):
    db.drop_table("Transactions")

db.create_table("Transactions", "id INTEGER PRIMARY KEY AUTOINCREMENT",
                                "account_id INTEGER",
                                "date_of_purchase",
                                "operation_ticker",
                                "type_of_transaction_value",
                                "currency",
                                "number_of_units INTEGER",
                                "price_of_one_unit REAL",
                                "commission REAL",
                                "yahoo_ticker",
                                "tax REAL",
                                "total_cost REAL",
                                "total_number_of_units_after_transaction INTEGER",
                                "account_balance_after_operation REAL",
                                "type_of_investment",
                                "FOREIGN KEY(account_id) REFERENCES Accounts(id)") 



class Account:
    
    def __init__(self, name: str = '', currency: str= "PLN", account_id: int=None):

        #Assign to instance
        self.name = name
        self.transactions: dict = {}    #Variable including all transactions for that instance of Account
        self.balance = 0.00 
        self.main_currency = currency

        #Add account to data base
        db.insert_row("Accounts", (account_id, self.name, str(self.balance), self.main_currency))
        
        #get id from data base    
        if account_id == None:
            self.id = int(db.get_last_row_value("Accounts", "id"))
        else:
            self.id = account_id

    def __str__(self):
        return f'Konto o nazwie {self.name} oraz id: {self.id}'

    @property
    def id(self):
        return self.__id

    @id.setter
    def id(self, new_id):
        self.__id = new_id

    @property
    def balance(self):
        return self._balance

    @balance.setter
    def balance(self, balance):
        self._balance = balance

    def actual_balance(self):
        Transaction_df = pd.read_sql("SELECT * FROM Transactions", db.conn)
        Transaction_df.set_index('id', inplace = True)
    
    def figure_plot_for_account(self):
        Transaction_df = db.get_table_df(f"INVESTMENT_VIEW_ACCOUNT_{self.id}", 'type_of_transaction_value', 'current_value')
        
        fig  = px.bar(Transaction_df, x='type_of_transaction_value', y='current_value')
        fig.show()
        fig = px.pie(Transaction_df, values='current_value', names='type_of_transaction_value')
        fig.show()

        Historical_value_df = db.get_table_df(f"ACCOUNT_{self.id}_HISTORICAL_VALUE", "Date", "account_balance", "total_cost")
        Historical_value_df["total_cost"] = Historical_value_df["total_cost"].fillna(method='ffill')
        fig = px.line(Historical_value_df, x="Date", y=Historical_value_df.columns, title='Historical balance')
        fig.show()

    def investment_view_by_type(self):

        Transaction_df = db.get_table_df_with_conditions("Transactions", "type_of_transaction_value", "currency", "yahoo_ticker", "total_number_of_units_after_transaction", "total_cost", "type_of_investment", account_id = f"{self.id}")

        Transaction_total_cost_df = Transaction_df.groupby(["type_of_transaction_value"])["total_cost"].sum()
        Transaction_df = Transaction_df.drop(columns="total_cost")
        Transaction_df = Transaction_df.drop_duplicates(subset=["type_of_transaction_value"], keep="last")

        Transaction_df[["last_market_price", "currency"]] = Transaction_df.apply(lambda x: yfin.get_last_market_price(x["yahoo_ticker"]), axis = 1 , result_type='expand')
        Transaction_df["current_currency_rate"] = Transaction_df["currency"].apply(nbp.get_exchange_rate)
        
        Transaction_df["current_value"] = Transaction_df["total_number_of_units_after_transaction"] * Transaction_df["last_market_price"] * Transaction_df["current_currency_rate"]
        
        Transaction_df = pd.merge(Transaction_df, Transaction_total_cost_df, on= "type_of_transaction_value", how="left")
        
        Transaction_df.to_sql(f"INVESTMENT_VIEW_ACCOUNT_{self.id}", db.conn, if_exists="replace")

    def prepare_table_for_historical_value(self, db):

        df_tickers = db.get_table_df_with_conditions("Transactions", "yahoo_ticker", account_id = "1")
        df_tickers = df_tickers.drop_duplicates()
        tickers_list = list(df_tickers["yahoo_ticker"])
        table_name = f"ACCOUNT_{self.id}_HISTORICAL_VALUE"

        if db.check_table_exists(table_name):
            
            last_row = db.get_last_row_value(table_name)
            last_date = last_row[0]
            last_date = datetime.strptime(last_date, "%Y-%m-%d").date()
            today_date = date.today()
            delta_time = today_date - last_date

            if delta_time.days != 0:
                start_date = last_date
                while start_date != today_date:
                    start_date = start_date + timedelta(days = 1)
                    new_row = tuple([str(start_date)] + [None for _ in range(len(last_row) - 1)])
                    db.insert_row(table_name, new_row)

                Historical_value_df = db.get_table_df(f"ACCOUNT_{self.id}_HISTORICAL_VALUE")
                Historical_value_df[[f"{ticker}_number_of_units" for ticker in tickers_list]] = Historical_value_df[[f"{ticker}_number_of_units" for ticker in tickers_list]].fillna(method='ffill')
                Historical_value_df = Historical_value_df.set_index("Date")
                Historical_value_df.to_sql(f"ACCOUNT_{self.id}_HISTORICAL_VALUE", db.conn, if_exists="replace")
                db.conn.commit()

            else: 
                print(f"Dane w tabeli {table_name} są aktualne.")

        else:
            db.create_table(table_name, "Date", 
                                        "account_balance REAL", 
                                        f"""{','.join([f"'{ticker}_number_of_units' INTEGER, '{ticker}_value' REAL" for ticker in tickers_list])}""")

            df = db.get_table_df(table_name)
            start_date = db.get_first_row_value("Transactions", "date_of_purchase")
            df.Date = pd.date_range(start=start_date, end=date.today(), freq="D")
            df = df.assign(Date =df.Date.dt.date)
            df = df.set_index("Date")
            df.to_sql(f"{table_name}", db.conn, if_exists="replace")
            
            df_transactions = db.get_table_df("Transactions", "date_of_purchase", "yahoo_ticker", "total_number_of_units_after_transaction", sort_col= "date_of_purchase", sort_order="ASC", where_condition= f"account_id == {self.id}")
            df_transactions.apply(self.update_historical_information_of_number_of_units, axis=1)
            Historical_value_df = db.get_table_df(f"ACCOUNT_{self.id}_HISTORICAL_VALUE")
            Historical_value_df = Historical_value_df.fillna(method='ffill')
            Historical_value_df = Historical_value_df.set_index("Date")
            Historical_value_df.to_sql(f"ACCOUNT_{self.id}_HISTORICAL_VALUE", db.conn, if_exists="replace")
            db.conn.commit()
            last_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        return last_date

    def update_historical_information_of_number_of_units(self, item, db= db):
        table_name = f"ACCOUNT_{self.id}_HISTORICAL_VALUE"
        total_number_of_units = item["total_number_of_units_after_transaction"]
        date_of_purchase = item["date_of_purchase"]
        yahoo_ticker = item["yahoo_ticker"]
        db.update_table(table_name = table_name, update_dict= {f"{yahoo_ticker}_number_of_units" : total_number_of_units}, where_dict = { "Date" : date_of_purchase})
        return True

    def get_currency_of(self, ticker_name: str, db):
        currency_df = db.get_table_df_with_conditions(f'INVESTMENT_VIEW_ACCOUNT_{self.id}', "currency", yahoo_ticker = f"{ticker_name}")
        currency = currency_df.loc[0,"currency"]
        return currency

    def get_exchange_rates_df_for(self, ticker: str, start_date: str, db):
        currency = self.get_currency_of(ticker_name = ticker, db=db)
        if currency == "PLN":
            end_date = str(date.today())
            dates = pd.date_range(start_date, end_date, freq='D')
            dates_str = dates.strftime("%Y-%m-%d")
            df_exchange_rates = pd.DataFrame({'Date': dates_str, 'PLN_PLN': 1.0})
            df_exchange_rates = df_exchange_rates.set_index("Date")
        else:    
            df_exchange_rates = db.get_table_df_with_conditions('EXCHANGE_RATE_TABLE', "Date", f"{currency}_PLN", Date = f">=__{start_date}")
            df_exchange_rates = df_exchange_rates.drop_duplicates(subset=['Date'])
            df_exchange_rates = df_exchange_rates.set_index("Date")
        return df_exchange_rates

    def get_number_of_units_df_for(self, ticker: str, start_date: str, db):
        column_name = f""" "{ticker}_number_of_units" """
        df_number_of_units = db.get_table_df_with_conditions("ACCOUNT_1_HISTORICAL_VALUE", "Date", column_name, Date = f'>=__{start_date}')
        df_number_of_units = df_number_of_units.drop_duplicates(subset=['Date'])
        df_number_of_units = df_number_of_units.set_index("Date")
        return df_number_of_units

    def get_historical_prize_df_for(self, ticker: str, start_date: str, db):
        table_name = f" '{ticker}' "
        df_close_prize = db.get_table_df_with_conditions(table_name, "Date", "Close", Date = f'>=__{start_date}')
        df_close_prize = df_close_prize.drop_duplicates(subset=['Date'])
        df_close_prize = df_close_prize.set_index("Date")
        return df_close_prize

    def create_historical_value_df_for(self, ticker: str, currency: str, df_number_of_units,  df_close_prize, df_exchange_rates):
        result_df = pd.concat([df_number_of_units, df_close_prize, df_exchange_rates], axis=1)
        result_df[f"{ticker}_number_of_units"]= pd.to_numeric(result_df[f"{ticker}_number_of_units"], errors='coerce')
        result_df["Close"]= pd.to_numeric(result_df["Close"], errors='coerce')
        result_df[f"{currency}_PLN"]= pd.to_numeric(result_df[f"{currency}_PLN"], errors='coerce')
        column_name = f"{ticker}_value"
        result_df[column_name] = result_df[f"{ticker}_number_of_units"]*result_df["Close"]*result_df[f"{currency}_PLN"]
        result_df = result_df.drop(columns=["Close", f"{currency}_PLN"])
        return result_df 

    def append_historical_value_to(self, df_to_append, db):
        df_historical_datas = db.get_table_df(f"ACCOUNT_{self.id}_HISTORICAL_VALUE")
        df_historical_datas = df_historical_datas.set_index("Date")
        result_df_to_append = df_historical_datas.combine_first(df_to_append)
        result_df_to_append.to_sql(f"ACCOUNT_{self.id}_HISTORICAL_VALUE", db.conn, if_exists="replace")
        return True

    def calculate_historical_value_for_ticker_and_append_to_db(self, ticker: str, db, start_date: str=None):

        if ticker not in ['','CASH'] and (ticker[:3] not in ['EDO', 'COI', 'ROS', 'ROD']):
            currency = self.get_currency_of(ticker_name = ticker, db=db)
            df_exchange_rates = self.get_exchange_rates_df_for(ticker = ticker, start_date = start_date, db=db)
            df_number_of_units = self.get_number_of_units_df_for(ticker = ticker, start_date = start_date, db=db)
            df_close_prize = self.get_historical_prize_df_for(ticker = ticker, start_date = start_date, db=db)
            result_df = self.create_historical_value_df_for(ticker = ticker, currency = currency, df_number_of_units = df_number_of_units,  df_close_prize = df_close_prize, df_exchange_rates = df_exchange_rates)
            self.append_historical_value_to(df_to_append = result_df, db=db)
            return True

        elif ticker[:3] in ['EDO', 'COI', 'ROS', 'ROD']:
            bond_ticker = ticker
            df = db.get_table_df_with_conditions(f'ACCOUNT_{self.id}_HISTORICAL_VALUE', "Date", f"{bond_ticker}_number_of_units, {bond_ticker}_value", Date = f">=__{start_date}")
            df[['prize_of_one_unit','currency']] = df.apply(lambda x:  pd.Series(bond_interest.get_current_bond_value(bond_ticker, x.Date)), axis = 1)
            df = df.drop(columns = ['currency'])
            df[f'{bond_ticker}_number_of_units'] = pd.to_numeric(df[f'{bond_ticker}_number_of_units'], errors='coerce')
            df[f'{bond_ticker}_value'] = df[f'{bond_ticker}_number_of_units'] * df['prize_of_one_unit']
            df = df.drop(columns=["prize_of_one_unit", f'{bond_ticker}_number_of_units'])
            df = df.set_index("Date")
            print(df)
            self.append_historical_value_to(df_to_append = df, db=db)
            return True

        elif ticker == "CASH":
            df_historical_datas = db.get_table_df(f'ACCOUNT_{self.id}_HISTORICAL_VALUE')
            df_historical_datas = df_historical_datas.set_index("Date")
            df_historical_datas["CASH_value"] = df_historical_datas["CASH_number_of_units"]
            df_historical_datas.to_sql(f"ACCOUNT_{self.id}_HISTORICAL_VALUE", db.conn, if_exists="replace")
            return True

        else:
            return False 

    def calculate_historical_balance_and_append_to_db(self, db= db):
        
        df_tickers = db.get_table_df('Transactions', "yahoo_ticker", where_condition= f" account_id == {self.id}")
        
        df_tickers = df_tickers.drop_duplicates()
        tickers_list = list(df_tickers["yahoo_ticker"])
        table_name = f"ACCOUNT_{self.id}_HISTORICAL_VALUE"
        tickers_string = ','.join([f""" "{ticker}_value" """  for ticker in tickers_list])
        df_historical_datas = db.get_table_df(table_name, "Date", tickers_string)
        df_historical_datas[df_historical_datas.columns[1:]] = df_historical_datas[df_historical_datas.columns[1:]].apply(pd.to_numeric, errors='coerce')
        df_historical_datas = df_historical_datas.fillna(method="ffill")
        df_historical_datas = df_historical_datas.fillna(0)
        df_historical_datas["account_balance"] = df_historical_datas.apply(lambda x: sum(x[1:]), axis=1)
        result_df = df_historical_datas[["Date", "account_balance"]]
        result_df = result_df.set_index("Date")
        self.append_historical_value_to(df_to_append = result_df, db=db)
        
    def historical_values_for_investmens_on(self, db= db):
        
        start_date = self.prepare_table_for_historical_value(db= db)
        self.update_historical_data_for_tickers()
        if start_date < date.today():
            print("Aktualizujemy tabele z danymi historycznymi")
            start_date = str(start_date)
            df = db.get_table_df(f"INVESTMENT_VIEW_ACCOUNT_{self.id}")
            df = df["yahoo_ticker"].loc[df["yahoo_ticker"] != '']
            df.apply(lambda x: self.calculate_historical_value_for_ticker_and_append_to_db(ticker= x, db=db, start_date= start_date)) 
        else:
            print("Dane historyczne aktualne, nie obliczamy danych historycznych")
    
    def update_historical_data_for_tickers(self):
        df = db.get_table_df(f"INVESTMENT_VIEW_ACCOUNT_{self.id}", "yahoo_ticker")
        df = df["yahoo_ticker"].loc[df["yahoo_ticker"] != '']
        df.apply(lambda x: yfin.download_historical_data(ticker= x,
                         name_of_db="invest_tracker_data_base"))   

    def get_dates_of_purchases_df_for(self, db= db):
        df_transactions_dates = db.get_table_df_with_conditions('Transactions', "date_of_purchase", account_id = f"{self.id}")
        df_transactions_dates = df_transactions_dates.drop_duplicates()
        df_transactions_dates = df_transactions_dates.rename(columns = {"date_of_purchase": "Date"}) 
        df_transactions_dates = df_transactions_dates.reset_index(drop = True)
        return df_transactions_dates    

    @staticmethod
    def calculate_total_cost_to_date(date_of_purchase: str, df_transactions):
        df_subset = df_transactions.loc[df_transactions["date_of_purchase"] <= date_of_purchase]
        total_cost_for_date = df_subset["total_cost"].sum()
        return total_cost_for_date
    
    def calculate_historical_total_cost_for(self, db= db):
        df_transactions = db.get_table_df_with_conditions('Transactions', "date_of_purchase", "total_cost", account_id = f"{self.id}")
        
        df_transactions_dates = self.get_dates_of_purchases_df_for(db = db)
        df_transactions_dates['total_cost'] = df_transactions_dates.apply(lambda x: self.calculate_total_cost_to_date(x["Date"], df_transactions), axis=1)
        df_transactions_dates = df_transactions_dates.set_index("Date")
        df_historical_total_cost = df_transactions_dates
        self.append_historical_value_to(df_to_append = df_historical_total_cost, db = db)
        return True

class Transaction:
    
    def __init__(self, 
                 account_id: int = 0, 
                 date_of_purchase: str = '-', 
                 operation_ticker: str = '-', 
                 type_of_transaction_value: str = '-', 
                 currency: str = '-', 
                 number_of_units: int = 0, 
                 price_of_one_unit: float = 0.00, 
                 commission: float = 0.00,
                 yahoo_ticker: str = None,
                 type_of_investment: str = None):

        #Assign to instance
        self.account_id = account_id
        self.date_of_purchase = date_of_purchase 
        self.operation_ticker = operation_ticker
        self.type_of_transaction_value = type_of_transaction_value  #Rodzaj waloru inwestycyjnego
        self.currency = currency
        self.number_of_units = number_of_units
        self.price_of_one_unit = price_of_one_unit
        self.commission = commission
        self.yahoo_ticker = yahoo_ticker
        self.type_of_investment = type_of_investment

        #Actions to execute    
        #Add transaction to data base
        db.insert("Transactions", (None, self.account_id, self.date_of_purchase, self.operation_ticker, self.type_of_transaction_value, self.currency, self.number_of_units, str(self.price_of_one_unit), str(self.commission), self.yahoo_ticker, None, None, None, None, self.type_of_investment))
       
        #Get id from database
        self.id = db.get_last_id(table_name = "Transactions")

        self.transaction_value = self.calculate_transaction_value()
        db.update_data(table_name="Transactions", attribute_name= "total_cost", attribute_value= self.transaction_value, item_id= self.id)

        self.total_number_of_units_after_transaction = self.get_total_number_of_units()
        db.update_data(table_name="Transactions", attribute_name= "total_number_of_units_after_transaction", attribute_value= self.total_number_of_units_after_transaction, item_id= self.id)        

    @property
    def id(self):
        return self.__id
    
    @id.setter
    def id(self, new_id):
        self.__id = new_id

    @property
    def price_of_one_unit(self):
        return self._price_of_one_unit
    
    @price_of_one_unit.setter
    def price_of_one_unit(self, price_of_one_unit):
        self._price_of_one_unit = price_of_one_unit

    @property
    def commission(self):
        return self._commission

    @commission.setter
    def commission(self, commission):
        self._commission = commission

    def calculate_transaction_value(self):
        if self.currency != "PLN":
            rate = nbp.get_exchange_rate(self.currency,self.date_of_purchase)
        else:
            rate = 1
        transaction_value = self.number_of_units * self.price_of_one_unit * rate + self.commission
        return transaction_value  #Calculated with commission

    def get_total_number_of_units(self):
        Transaction_df = pd.read_sql("SELECT * FROM Transactions", db.conn)
        total = Transaction_df[ (Transaction_df["account_id"] == self.account_id)  &  (Transaction_df["type_of_transaction_value"] == self.type_of_transaction_value)].number_of_units.sum()
        return total

    def __repr__(self):
        #repr_string = f"Transaction('{self.id}', '{self.account_id}', '{self.date_of_purchase}', '{self.operation_ticker}', '{self.type_of_transaction_value}', '{self.currency}', '{self.number_of_units}', '{self.price_of_one_unit}', '{self.price_of_one_unit}', '{self.commission}', '{self.transaction_value}', '{self.account_balance_after_the_operation}', '{self.number_of_units_after_transaction}')"
                #print(f"{transaction['account_id']} {transaction['date_of_purchase']} {transaction['operation_ticker']} {transaction['type_of_transaction_value']} {transaction['currency']} {transaction['number_of_units']} {transaction['price_of_one_unit']} {transaction['commission']}")    
        #return repr_string
        return f"Transaction id: '{self.id}' "

    @classmethod
    def import_transactions_from_csv(cls):
        with open('transakcje.csv', 'r', encoding="utf-8") as file:
            reader = csv.DictReader(file, delimiter=';')
            
            for transaction in reader:
                Transaction(account_id = int(transaction['account_id']), 
                            date_of_purchase = transaction['date_of_purchase'], 
                            operation_ticker = transaction['operation_ticker'], 
                            type_of_transaction_value = transaction['type_of_transaction_value'], 
                            currency = transaction['currency'], 
                            number_of_units = int(transaction['number_of_units']), 
                            price_of_one_unit = float(transaction['price_of_one_unit']), 
                            commission = float(transaction['commission']),
                            yahoo_ticker = transaction['yahoo_ticker'],
                            type_of_investment = transaction['type_of_investment'])
    
    @classmethod
    def sorting_transactions_csv_by_date(cls, URL):
        df = pd.read_csv(URL,
                         sep=";",
                         encoding="utf-8")
        df = df.sort_values(by=["date_of_purchase"])
        df.to_csv(URL, index=False, sep=";", encoding="utf-8")



def main():
    
    db = data_base.Database("invest_tracker_data_base")

    if db.check_table_exists("Accounts"):
        db.drop_table("Accounts")
    db.create_table("Accounts", "id INTEGER PRIMARY KEY AUTOINCREMENT", "name", "balance REAL", "currency")

    account1 = Account("Porfel Długoterminowy")

    Transaction.import_transactions_from_csv()

    nbp.check_nbp_api_for_new_exchange_rates(["USD", "GBP", "EUR"])

    account1.actual_balance()
    
    account1.investment_view_by_type()
    
    account1.update_historical_data_for_tickers()

    account1.historical_values_for_investmens_on(db=db)

    account1.calculate_historical_balance_and_append_to_db(db =db)

    account1.calculate_historical_total_cost_for(db=db)

    account1.figure_plot_for_account()

if __name__ == '__main__':
    main()