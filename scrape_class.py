"""
First, Scrape one database only (11549) and import it into SQL

I want to make an auto update in SQL afterward
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import ssl
from selenium import webdriver

# get ssl verification
ssl._create_default_https_context = ssl._create_unverified_context

class ScrapeClass():
    
    def __init__(self):
         
        # the dictionary to store the scraped data
        self.DataDict = None

        # the number of the total gov dataset
        self.NumberOfDataset = 522 + 65 + 659 + 250 + 529 + 662 + 16 + 1684 + 835 + 1755 + 997 + 833 + 95 + 2651 + 26 + 256 + 94 + 32391

        # the place to store the setting of chrome
        self.driver = None

    def SetUpChromeDriver(self, location):
        options = webdriver.ChromeOptions()
        options.add_experimental_option("prefs", 
            {
            "download.default_directory": location,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True
            })
        driver = webdriver.Chrome(chrome_options=options)
        self.driver = driver

    def GetTheNumberOfDataset():
        pass

    def GetTheDownloadUrl():
    	# should find a way if there is no CSV form
    	pass

    def FindEncoding():
    	# find a way to solve different Encoding problem
    	pass

    def ImportCSVintoSQL(self):
        pass

    def ScrapeData(self):

        driver = self.driver

        DataDict = {}

        DatasetNum = self.NumberOfDataset

        for Number in range(11549, 11550):

            print("============")

	        # the url of the webpage of the dataset
            Url = "https://data.gov.tw/dataset/" + str(Number)
            print("url: ", Url)

	        # connect the page
            Page = requests.get(Url)

	        # get the content of the page
            Soup = BeautifulSoup(Page.content, 'html.parser')

	        # get the title of the website
            try:
                Title = Soup.find_all("h1", class_="node-title")[0].text
                if Title == "404":
                    continue
            except:
                print("no title")
                continue

            # get the download url (should try to fix it with GetTheDownloadUrl())
            try:
                DownloadUrl = Soup.find_all("a", string="CSV")[0]['href']
                print("csv_url: ", DownloadUrl)
            except:
                print("no download url for csv")
                continue

            try:
		        # start to get the data
                '''
                Data = pd.read_csv(DownloadUrl)
                print(Data)
                print("row_num: ", len(Data.index))
                '''
                driver.get(DownloadUrl)

            except:
            	print("no Data")
    	        continue

            # DataDict[Title] = [len(Data.index), Data]

            # print(data_dict)

        return DataDict

