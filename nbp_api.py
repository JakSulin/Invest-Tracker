from requests import get
from datetime import date, timedelta, datetime
import pandas as pd
import sqlite3 as sql
import data_base


def get_exchange_rate(currency_code: str, date_param: str= str(date.today())):
    currency_code = currency_code.strip().lower()
    currencies_available = ("thb", "usd", "aud", "hkd", "cad",
                            "nzd", "sgd", "eur", "huf", "chf",
                            "gbp", "uah", "jpy", "czk", "dkk",
                            "isk", "nok", "sek", "ron", "bgn",
                            "try", "ils", "clp", "php", "mxn",
                            "myr", "idr", "inr", "krw", "cny")
    if currency_code == "pln":
        rate = 1.00
        return rate
    if currency_code not in currencies_available:
        raise AttributeError(f"Can't check '{currency_code}' rate.")
    date_param = date_param.strip()
    if "." in date_param:
        date_param = date_param.replace(".", "-")
    i=0
    while True:
        response = get_response_from_url(currency_code, date_param)
        if response.status_code == 200:
            break
        elif response.status_code == 404:
            date_param = datetime.strptime(date_param, "%Y-%m-%d").date()
            date_param = str(date_param + timedelta(days = -1))  
            response = get_response_from_url(currency_code, date_param)
        elif response.status_code == 400:
            date_param = datetime.strptime(date_param, "%Y-%m-%d").date()
            if date_param > date.today():
                raise AttributeError(f"Invalid date: {date_param} It's out of range.")
        if i >=10:
            raise NameError("Can't get response from server. Too many tries")
        i += 1 
      
    #print(url)
    response = response.json()
    rate = response["rates"][0]["mid"]    
    #print(f"Code of currency: '{currency_code.upper()}', date_param: '{date_param}', rate: '{rate}'")
    return  rate #Return currency rate in float number. 

def get_response_from_url(currency_code:str, date_param: str):
    prefix = "http://api.nbp.pl/api/exchangerates/rates/a"
    suffix = "?format=json"
    url = "/".join((prefix, currency_code, date_param, suffix))
    response = get(url) #Status 2xx- wszystko ok, 3xx- przekierowanie, 404 - coś zepsuł użytkownik, 5xx- cos się stało po stronie serwera    

    return response

def get_exchange_rates_from_date_range(currency_code: str, start_date: str, end_date: str):
    currency_code = currency_code.strip().lower()
    currencies_available = ("thb", "usd", "aud", "hkd", "cad",
                            "nzd", "sgd", "eur", "huf", "chf",
                            "gbp", "uah", "jpy", "czk", "dkk",
                            "isk", "nok", "sek", "ron", "bgn",
                            "try", "ils", "clp", "php", "mxn",
                            "myr", "idr", "inr", "krw", "cny")
    if currency_code not in currencies_available:
        raise AttributeError(f"Can't check '{currency_code}' rate.")

    date_param = "/".join((start_date, end_date))

    if "." in date_param:
        date_param = date_param.replace(".", "-")
    
    response = get_response_from_url(currency_code, date_param)
    if response.status_code == 404:
        raise AttributeError(f"Can't get response with date: {date_param}. No data for the indicated date range")
    elif response.status_code == 400:
        raise AttributeError(f"Invalid date: {date_param} It's out of range.")
 
    response = response.json()   
    df = pd.json_normalize(response["rates"]) 
    df = df.drop(columns="no")
    df = df.rename(columns = {"effectiveDate" : "Date", "mid" : f"{currency_code.upper()}_PLN"})
    df = df.set_index("Date")
    return df

def get_data_frame_exchange_rates_of(list_of_currencies_codes: list=list(), start_date: str="", end_date: str=""):
    conn = sql.connect("invest_tracker_data_base")
    print(f"Uploading data from '{start_date}' to '{end_date}'", end=" ")
    df1 = get_exchange_rates_from_date_range(list_of_currencies_codes[0],str(start_date),str(end_date))
    df2 = get_exchange_rates_from_date_range(list_of_currencies_codes[1],str(start_date),str(end_date))
    result = pd.concat([df1, df2], axis=1)
    for currencies in list_of_currencies_codes[2:]:
        df3 = get_exchange_rates_from_date_range(currencies,str(start_date),str(end_date))
        result = pd.concat([result, df3], axis=1)
    result.to_sql('EXCHANGE_RATE_TABLE', conn, if_exists="append")
    print("DONE")

def check_valid_date_parameter(start_date: str, end_date: str):
    start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
    start_date_limit = datetime.strptime("2002-01-02", "%Y-%m-%d").date()
    end_date_limit = date.today()
    if start_date > end_date:
        raise ValueError(f" Start date '{start_date}' can't be greater than '{end_date}'") 
    if start_date < start_date_limit:
        print(f"Start_date '{start_date}' parameter out of range. Changed to '{start_date_limit}'")
        start_date = start_date_limit
    if end_date > end_date_limit:
        print(f"End_date '{end_date}' parameter out of range. Changed to '{end_date_limit}'")
        end_date = end_date_limit
    return start_date, end_date

def divide_period(start_date: str, end_date: str, period_length: int = 90):
    delta_time = end_date - start_date
    if delta_time.days == 0:
        periods = 1
    elif delta_time.days % period_length != 0:
        periods = (delta_time.days // period_length)
        periods += 1
    else:
        periods = delta_time.days // period_length
    return periods

def redownload_all_exchange_rates_in_data_base_from(list_of_currencies_codes: list=list()):
    conn = sql.connect("invest_tracker_data_base")
    cur = conn.cursor()
    cur.executescript("""DROP TABLE IF EXISTS 'EXCHANGE_RATE_TABLE'""")
    start_date="2002-01-02"
    end_date = str(date.today())
    start_date, end_date = check_valid_date_parameter(start_date, end_date)
    periods = divide_period(start_date, end_date, 90)
    single_query_dates_interval = timedelta(days = 89)
    for _ in range(1, periods +1):
        if start_date + single_query_dates_interval <= end_date:
            if start_date + timedelta(days = 90) == end_date:
                single_query_dates_interval += timedelta(days = 1)
            get_data_frame_exchange_rates_of(list_of_currencies_codes, str(start_date), str(start_date + single_query_dates_interval))    
            start_date = start_date + single_query_dates_interval + timedelta(days = 1)
        else:
            get_data_frame_exchange_rates_of(list_of_currencies_codes, str(start_date), str(end_date))     
            start_date = start_date + single_query_dates_interval
    conn.commit()


def check_nbp_api_for_new_exchange_rates(list_of_currencies_codes: list=list()):
    conn = sql.connect("invest_tracker_data_base")
    cur = conn.cursor()
    


    cur.execute(f"""SELECT name FROM sqlite_master WHERE type == 'table' AND name == 'EXCHANGE_RATE_TABLE'""")
    result = cur.fetchone()
    if result == None:
        redownload_all_exchange_rates_in_data_base_from(["USD", "GBP", "EUR"])
    else:
        print("cos")

    cur.execute(f"SELECT Date FROM 'EXCHANGE_RATE_TABLE'")
    last_date = cur.fetchall()[-1][0]
    last_date = datetime.strptime(last_date, "%Y-%m-%d").date()
    date_today = date.today()
    delta_time = date_today - last_date
    
    
    if last_date != date_today and date_today.isoweekday() <= 5:
        start_date = last_date + timedelta(days = 1)
        time_now = datetime.now()
        #get_data_frame_exchange_rates_of(list_of_currencies_codes, str(start_date), str(date_today))
        #print("Daty się różnią dzisiaj jest dzień pracujący więc aktualizujemy")
        # if time_now.hour >= 12 and time_now.minute >= 16:
        #     get_data_frame_exchange_rates_of(list_of_currencies_codes, str(start_date), str(date_today))
        #     print("Daty się różnią dzisiaj jest dzień pracujący więc aktualizujemy")
        # else:
        #     print("Za wcześnie, zaktualizujemy po 12:15")
        try:
            get_data_frame_exchange_rates_of(list_of_currencies_codes, str(start_date), str(date_today))
            print("Daty się różnią dzisiaj jest dzień pracujący więc aktualizujemy")        
        except AttributeError:
            if time_now.hour < 12:
                print("Za wcześnie, zaktualizujemy po 12:15")
            elif time_now.hour == 12 and time_now.minute <= 16:
                print("Za wcześnie, zaktualizujemy po 12:15")
            else:
                print("Mamy problem nie możemy zaktualizować")


    elif date_today.isoweekday() == 6 and delta_time.days >= 2:
        start_date = last_date + timedelta(days = 1)
        get_data_frame_exchange_rates_of(list_of_currencies_codes, str(start_date), str(date_today))
        print("SObota, ale data jest o dwa dni odległa, więc jedziemy z aktualizacją kursów walut")
    elif date_today.isoweekday() == 7 and delta_time.days >= 3:
        start_date = last_date + timedelta(days = 1)
        get_data_frame_exchange_rates_of(list_of_currencies_codes, str(start_date), str(date_today))
        print("Niedziela,  ale data jest o trzy dni odległa, więc jedziemy z aktualizacją kursów walut")
    else:
        print("Dane kursów walut są aktualne z tabelami NBP, lub jest weekend wiec odpuszczamy")    
     



def main():

    #Examples of usage
    # print(get_exchange_rate("USD","2023.01-19"))
    # print(get_exchange_rate("eur","2022-12-26"))
    # print(get_exchange_rate("gbp", "2023-01-12"))
    # print(get_exchange_rate("gbp", "2023-01-15")) #Return rate of currency from first working day before "2023-01-15" because "2023-01-15" is sunday
    # print(get_exchange_rate("usd")) #Return today rate of currency
    # print(get_exchange_rate("gbp")) #Return today rate of currency
    # print(get_exchange_rate("eur")) #Return today rate of currency
    # get_exchange_rates_from_date_range("USD","2023-02-01","2023-02-24")
    
    #Redownload all exchanges rates from NBP tables
    redownload_all_exchange_rates_in_data_base_from(["USD", "GBP", "EUR"])
    
    #checking and updateing data from nbp api if exists
    #check_nbp_api_for_new_exchange_rates(["USD", "GBP", "EUR"])


if __name__ == "__main__":
    main()