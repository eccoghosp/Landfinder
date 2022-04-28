#!/usr/bin/env python

# libraries
import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlalchemy
from re import sub
from tqdm import tqdm
import numpy as np
import datetime as dt
import time
import random

server = '' # Server name goes here. 
db = 'LandFinder'
states = ['alaska', 'alabama', 'arkansas', 'arizona', 'california', 'colorado', 'connecticut',
          'district of columbia', 'delaware', 'florida', 'georgia', 'hawaii', 'iowa', 'idaho', 'illinois',
          'indiana', 'kansas', 'kentucky', 'louisiana', 'massachusetts', 'maryland', 'maine', 'michigan',
          'minnesota', 'missouri', 'mississippi', 'montana', 'north carolina', 'north dakota', 'nebraska',
          'new hampshire', 'new jersey', 'new mexico', 'nevada', 'new york', 'ohio', 'oklahoma', 'oregon',
          'pennsylvania', 'rhode island', 'south carolina', 'south dakota', 'tennessee', 'texas', 'utah',
          'virginia', 'vermont', 'washington', 'wisconsin', 'west virginia', 'wyoming']


def get_state():
'''
Prompts user for a state, district of columbia, or usa
'''
    choice = input("Which US state's properties do you want to see? ('USA' for all) ").lower()
    while choice not in states and choice != 'usa':
        choice = input('You must spell out the whole state. ').lower()
    return choice


def store_df(df, state='USA'):
'''
Saves the DataFrame into a new table named after the state. If all states are scrapped, then it will put each 
state into its own table as well as a table called 'usa' for the collective dataframe. 
'''
    try:
        engine = sqlalchemy.create_engine(
            f'mssql+pyodbc://@{server}/{db}?trusted_connection=yes&driver=ODBC Driver 17 for SQL Server'
            , fast_executemany=True)
        print(f'\nUpdating SQL Server table: {state}.')
        df.to_sql(f'{state}', con=engine, if_exists='replace', index=False, chunksize=100000)
        print('DataFrame Saved!')
    except NameError:
        print('Problem saving DataFrame')


def get_pagenum(state, headers):
''' 
Get the number of pages on landwatch.com for a particular state. 
'''
    url = f'https://www.landwatch.com/{state}_land_for_sale'
    soup = BeautifulSoup(requests.get(url, headers=headers).content, 'html.parser')
    prop_containers = soup.find_all(class_="_8cfc9")
    max_page = int(prop_containers[-1].get_text())
    return max_page


def get_land(state):
''' 
Scrapes data from each page in a state and puts it into a DataFrame. 
'''
    n_pages = 0
    count = 0
    realtor = []
    n_link = []
    n_price = []
    city = []
    county = []
    acreage = []
    timestamp = []

    headers = ({'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 '
                              'Safari/537.36'})

    pages = get_pagenum(state, headers)
    for page in tqdm(range(pages)):
        time.sleep(1)
        n_pages += 1

        # print(f'Working on page: {n_pages}')
        url = f'https://www.landwatch.com/{state}_land_for_sale/page-{str(n_pages)}'
        soup = BeautifulSoup(requests.get(url, headers=headers).content, 'html.parser')
        prop_containers = soup.find_all(class_="_78864 _87677")
        for i in range(len(prop_containers)):
            prop = prop_containers[i]

            # Get price of property
            if type(prop.find(class_="b04f6")) is not type(None):
                count += 1
                price = prop.find(class_="b04f6").get_text()
                if sub(r'[^\d.]', '', price) != '':
                    n_price.append(float(sub(r'[^\d.]', '', price)))
                else:
                    n_price.append(np.nan)

                # From the title extract acreage, city, and county
                title = prop.find(class_='_6f4cb _9c946').get_text()
                n_title = title.split('-')
                acreage.append(float(sub(r'[^\d.]', '', n_title[0])))
                city.append(n_title[1].split(',')[0].strip())

                if len(n_title[1].split('(')) > 1:
                    county.append(n_title[1].split('(')[1].strip(')').replace(' County', ''))
                else:
                    county.append(n_title[1])

                # Get realtor and url to more detailed page
                if type(prop.find(class_="_2548b")) is not type(None):
                    realtor.append(prop.find(class_="_2548b").get_text())
                else:
                    realtor.append('None')
                link = prop.findChild('a')['href']
                n_link.append('https://www.landwatch.com' + link)

                # Timestamp
                timestamp.append(dt.datetime.now())

    print(f'You scraped {n_pages} pages containing {count} Properties')
    zipped = list(zip(n_price, county, city, acreage, realtor, n_link, timestamp))

    df = pd.DataFrame(zipped, columns=['Price', 'County', 'City', 'Acreage', 'Realtor', 'URL', 'timestamp'])
    df['PricePerAcre'] = df['Price'] / df['Acreage']
    df['State'] = str(state)
    df.replace('', np.nan)
    df.replace({np.inf: np.nan, -np.inf: np.nan}, inplace=True)
    df = df.fillna(0)

    return df


def save_local_copy():
''' 
Option to save a local copy of a state as a .csv
'''
    save = input('Do you want to save local copies? (y/n) ')

    # Determine whether to save local copies of each state
    yes = ['y', 'Y', 'yes', 'Yes']
    no = ['n', 'N', 'No', 'no']
    i = -1
    while i < 0:
        if save in yes:
            i = 1
        elif save in no:
            i = 0
        else:
            save = input("That was not a valid selection. Choose 'y' or 'n'. ")
            i = -1
    return i


def main():
'''
Gets all properties for a state or whole usa. Saves the result to a SQL DB
'''
    # Choose one or all states.csv to update SQL Server
    state_choice = get_state()
    save_state = save_local_copy()

    if state_choice == 'usa':
        data2 = pd.DataFrame()

        for state in states:
            sleep = random.randrange(1, 25)
            print(f'Waiting for {sleep} seconds to avoid detection.')
            time.sleep(sleep)
            print(f'Gathering data for {state}.')
            data = get_land(state)
            data2.append(data)
                    
            if save_state == 1:
                data.to_csv(f'C:{state}.csv', index=False)
            store_df(data, state)
                    
        store_df(data2)
    else:
        print(f'Gathering data for {state_choice}.')
        data = get_land(state_choice)
        store_df(data, state_choice)


if __name__ == '__main__':
    main()
