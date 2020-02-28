"""
First, Scrape one database only (11549) and import it into SQL

I want to make an auto update in SQL afterward
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import ssl
# from selenium import webdriver
# from sqlalchemy import create_engine

# get ssl verification
ssl._create_default_https_context = ssl._create_unverified_context

class ScrapeOneData():
    
    def __init__(self):

        # the place to store the setting of chrome
        self.driver = None
         
        # the dictionary to store the scraped data
        self.DataDict = None

        # the number of the total gov dataset
        self.NumberOfDataset = 522 + 65 + 659 + 250 + 529 + 662 + 16 + 1684 + 835 + 1755 + 997 + 833 + 95 + 2651 + 26 + 256 + 94 + 32391

    def GetTheNumberOfDataset():
        # the way to find the total number of datasets
        pass

    def GetTheDownloadUrl():
    	# should find a way if there is no CSV form
    	pass

    def FindEncoding():
    	# find a way to solve different Encoding problem
    	pass

    def ScrapeTitleAndDownloadURL(self, Number):

        driver = self.driver

        DataDict = {}

        # the url of the webpage of the dataset
        Url = "https://data.gov.tw/dataset/" + str(Number)
        # print("url: ", Url)

        # connect the page
        Page = requests.get(Url)

        # get the content of the page
        Soup = BeautifulSoup(Page.content, 'html.parser')

        # get the title of the website
        try:
            Title = Soup.find_all("h1", class_="node-title")[0].text
            if Title == "404":
                pass
        except:
            print("no title")
            pass

        # get the download url (should try to fix it with GetTheDownloadUrl())
        try:
            DownloadUrl = Soup.find_all("a", string="CSV")[0]['href']
            print("csv_url: ", DownloadUrl)
        except:
            print("no download url for csv")
            pass

        return [Title, DownloadUrl]

    def DownloadCSVFile(self, DownloadUrl, location):

        data = pd.read_csv(DownloadUrl)

        data.to_csv(location)

        # print(type(data))

        return data


# class MySQLWithPython(): # the function to do SQL command through python

def MysqlCommand(host, user, password, database, sql, message=None):
    # host: the web host
    # user: the user name
    # password: the password of loacl MySQL DataBase
    # sql: the command want to do in sql
    # return: completion of the sql command
    try:
        # connect to database
        conn = pymysql.connect(host=host,
                               user=user,
                               password=password
                               )
        print('Connected to DB: {}'.format(host))
        # Creates cursor for multiple seperate working environments through the
        # same connection to the database.
        cursor = conn.cursor()
        # execute the sql command:
        cursor.execute('USE ' + database)
        cursor.execute(sql)
        # commit the change of the SQL code:
        conn.commit()
        print('Succuessfully execute MySQL Command')
        conn.close()
        # print out the message about what we want to do
        if message is not None:
            print(message)

    except Exception as e:
        # if error arouses during the try, print out the error
        print('Error: {}'.format(str(e)))
        # explanation of sys.exit(1): https://medium.com/p/4a895670e5e4/edit
        # sys.exit(1)

'''
def ImportCSVintoSQL(self, title, password):
    
    print("Start to Load Data: " title)

    database = title
    host = '127.0.0.1'
    user = 'root'

    try:
    print("==========================")
    print("loading data:", TARGET_NAMES[i], file_names[i])

    # command of sql for importing data
    sql = load_table(paths[i], TARGET_NAMES[i])

    # execute the MySQL command
    python_mysql_command(host, user, password, database, sql,
                        "Import Successfully")
    except:
        pass
'''
