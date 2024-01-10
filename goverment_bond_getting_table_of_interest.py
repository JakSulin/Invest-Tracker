"""
Note:
    - This module is part of Invest Tracker aplication, if you want use it separately you need make changes! More info below.

Get goverment bond interest table

This Python script serves module for  retrieves data from a specified URL, processes the information using the pandas library, and stores the relevant details in an SQLite database.

- Data Source: The application fetches live data from a government website, either in CSV or Excel format.

- Data Processing: The pandas library is employed to parse and manipulate the data. The script renames columns, removes unnecessary information, and organizes the data based on specific criteria.

- Database Handling: SQLite is used to create and manage tables within the 'invest_tracker_data_base.' Four tables ('EDO', 'COI', 'ROS', 'ROD') store government bond interest rate information.

- Data Retrieval and Update: The application uses the requests library to retrieve data from the specified URL. The script updates the SQLite database, ensuring the information is current.

- Printing Status: A confirmation message is displayed, indicating the successful update of the government bond interest table.

[###############100%###############] Successfully updated the government bond interest table

Note:
- Before run this code you should create SQLite data base with name: "invest_tracker_data_base"
"""



























import sqlite3 as sql
import pandas as pd
import csv
import requests
from datetime import date, timedelta, datetime


con = sql.connect("invest_tracker_data_base")

#csv_url = 'https://api.dane.gov.pl/resources/44360,sprzedaz-obligacji-detalicznych/file'
csv_url = 'https://www.gov.pl/attachment/428159cf-4d04-4bf2-975e-fd79d6b8f621'
#csv_url = 'https://api.dane.gov.pl/media/resources/20230117/Dane_dotyczace_obligacji_detalicznych.xls'


s = requests.get(csv_url).content
xl = pd.ExcelFile(s)


edo = xl.parse('EDO')
dict_name = {'Seria':'seria',
             'Oprocentowanie':'oprocentowanie_1rok',
             'Unnamed: 10':'oprocentowanie_2rok',
             'Unnamed: 11':'oprocentowanie_3rok',
             'Unnamed: 12':'oprocentowanie_4rok',
             'Unnamed: 13':'oprocentowanie_5rok',
             'Unnamed: 14':'oprocentowanie_6rok',
             'Unnamed: 15':'oprocentowanie_7rok',
             'Unnamed: 16':'oprocentowanie_8rok',
             'Unnamed: 17':'oprocentowanie_9rok',
             'Unnamed: 18':'oprocentowanie_10rok',}
edo = edo.rename(columns= dict_name)
edo = edo.drop(['Kod ISIN', 'Data wykupu', 'Początek sprzedaży',
                'Koniec sprzedaży', 'Cena emisyjna', 'Cena zamiany',
                'Sprzedaż łączna \n(mln zł)', 'w tym zamiana (mln zł)', 'Odsetki (zł)'], 
                axis='columns')
edo = edo.drop(0, axis='index')
edo.reset_index()
edo = edo.set_index('seria')
edo.to_sql('EDO', con, schema=None, if_exists='replace', index=True, index_label='seria', chunksize=None, dtype=None, method=None)
con.commit()


coi = xl.parse('COI')
dict_name = {'Seria':'seria',
             'Oprocentowanie':'oprocentowanie_1rok',
             'Unnamed: 10':'oprocentowanie_2rok',
             'Unnamed: 11':'oprocentowanie_3rok',
             'Unnamed: 12':'oprocentowanie_4rok'}
coi = coi.rename(columns= dict_name)
coi = coi.drop(['Kod ISIN', 'Data wykupu', 'Początek sprzedaży',
                'Koniec sprzedaży', 'Cena emisyjna', 'Cena zamiany',
                'Sprzedaż łączna \n(mln zł)', 'w tym zamiana (mln zł)',
                'Odsetki (zł)', 'Unnamed: 14', 'Unnamed: 15', 'Unnamed: 16'], 
                axis='columns')
coi = coi.drop(0, axis='index')
coi.reset_index()
coi = coi.set_index('seria')
coi.to_sql('COI', con, schema=None, if_exists='replace', index=True, index_label='seria', chunksize=None, dtype=None, method=None)
con.commit()


ros = xl.parse('ROS')
dict_name = {'Seria':'seria',
             'Oprocentowanie':'oprocentowanie_1rok',
             'Unnamed: 10':'oprocentowanie_2rok',
             'Unnamed: 11':'oprocentowanie_3rok',
             'Unnamed: 12':'oprocentowanie_4rok',
             'Unnamed: 13':'oprocentowanie_5rok',
             'Unnamed: 14':'oprocentowanie_6rok'}
ros = ros.rename(columns= dict_name)
ros = ros.drop(['Kod ISIN', 'Data wykupu', 'Początek sprzedaży',
                'Koniec sprzedaży', 'Cena emisyjna', 'Cena zamiany',
                'Sprzedaż łączna \n(mln zł)', 'w tym zamiana (mln zł)', 'Odsetki (zł)'], 
                axis='columns')
ros = ros.drop(0, axis='index')
ros.reset_index()
ros = ros.set_index('seria')
ros.to_sql('ROS', con, schema=None, if_exists='replace', index=True, index_label='seria', chunksize=None, dtype=None, method=None)
con.commit()


rod = xl.parse('ROD')
dict_name = {'Seria':'seria',
             'Oprocentowanie':'oprocentowanie_1rok',
             'Unnamed: 10':'oprocentowanie_2rok',
             'Unnamed: 11':'oprocentowanie_3rok',
             'Unnamed: 12':'oprocentowanie_4rok',
             'Unnamed: 13':'oprocentowanie_5rok',
             'Unnamed: 14':'oprocentowanie_6rok',
             'Unnamed: 15':'oprocentowanie_7rok',
             'Unnamed: 16':'oprocentowanie_8rok',
             'Unnamed: 17':'oprocentowanie_9rok',
             'Unnamed: 18':'oprocentowanie_10rok',
             'Unnamed: 19':'oprocentowanie_11rok',
             'Unnamed: 20':'oprocentowanie_12rok'}
rod = rod.rename(columns= dict_name)
rod = rod.drop(['Kod ISIN', 'Data wykupu', 'Początek sprzedaży',
                'Koniec sprzedaży', 'Cena emisyjna', 'Cena zamiany',
                'Sprzedaż łączna \n(mln zł)', 'w tym zamiana (mln zł)', 'Odsetki (zł)'], 
                axis='columns')
rod = rod.drop(0, axis='index')
rod.reset_index()
rod = rod.set_index('seria')
rod.to_sql('ROD', con, schema=None, if_exists='replace', index=True, index_label='seria', chunksize=None, dtype=None, method=None)
con.commit()

print("[###############100%###############] Successfully updated the goverment bond interest table")








