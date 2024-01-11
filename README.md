# Invest-Tracker
Aplication for tracking values of investments. It initializes a database, imports transactions from CSV, fetches government bond interest rates, retrieves currency exchange rates, calculates historical portfolio parameters, stores data in the database, and presents basic charts for a user-friendly investment management experience.

The script, along with additional modules, serves the following purposes:
1) Initializing the database
2) Importing a list of transactions from a CSV file
3) Fetching and processing government bond interest rate tables
4) Retrieving currency exchange rates from the NBP tables
5) Calculating historical parameters of the investment portfolio (Total costs, total value, etc.)
6) Storing the calculated data in the database
7) Presenting basic charts.

## Demo Video

Explore the functionality of Invest-Tracker by watching demo video wich is part of repository [Invest_tracker_gui.mp4]. This brief presentation provides an overview of the application's features.

## Modules:
- `csv`: Module for reading and writing CSV files.
- `sqlite3`: Module for interacting with SQLite database.
- `matplotlib.pyplot`: Library for creating static, animated, and interactive visualizations.
- `plotly.express`: High-level interface for creating interactive plots.
- `pandas`: Data manipulation library.
- `datetime`: Module for working with dates and times.
- `decimal`: Module for decimal floating-point arithmetic.
- `os`: Module for interacting with the operating system.

## Own Modules:
- `data_base`: Module for managing the SQLite database.
- `nbp_api`: Module for fetching exchange rates from NBP API.
- `goverment_bond_getting_table_of_interest`: Module for fetching government bond interest rates tables.
- `interest_goverment_bond`: Module for calculating interest on government bonds.
- `yahoo_finance_api`: Module for fetching financial data from Yahoo Finance.

## Usage:
1. Import the required modules.
2. Create transactions CSV file (Example file is in repository).
3. Run the invest_tracker_main script to execute the complete workflow.

