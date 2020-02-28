"""
First, Scrape one database only (11549) and import it into SQL

I want to make an auto update in SQL afterward
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import ssl
from selenium import webdriver
# from sqlalchemy import create_engine

# get ssl verification
ssl._create_default_https_context = ssl._create_unverified_context

class ScrapeOneData():
    
    def __init__(self):
         
        # the dictionary to store the scraped data
        self.DataDict = None

        # the number of the total gov dataset
        self.NumberOfDataset = 522 + 65 + 659 + 250 + 529 + 662 + 16 + 1684 + 835 + 1755 + 997 + 833 + 95 + 2651 + 26 + 256 + 94 + 32391

        # the place to store the setting of chrome
        self.driver = None

    def SetUpChromeDriver(self, location):
        options = webdriver.ChromeOptions()
        # options.add_argument(location)
        driver = webdriver.Chrome()
        self.driver = driver

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
