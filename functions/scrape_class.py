"""
First, Scrape one database only (11549) and import it into SQL

I want to make an auto update in SQL afterward
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import ssl
import mysql.connector
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

    def DownloadCSVFile(self, Title, DownloadUrl, location):

        data = pd.read_csv(DownloadUrl)

        data.to_csv(location + Title + ".csv")

        # print(type(data))

        return data


class MySQLWithPython(): # the function to do SQL command through python

    def __init__(self, host, user, password, database):
        # host: the web host
        self.host = host
        # user: the user name
        self.user = user
        # password: the password of loacl MySQL DataBase
        self.password = password
        self.database = database

    def MysqlCommand(self, host, user, password, database, sql):
        # sql: the command want to do in sql
        # return: completion of the sql command
        try:
            # connect to database
            connection = mysql.connector.connect(host=host,
                                                 user=user,
                                                 password=password,
                                                 database=database
                                                 )

            # check whether connected
            if connection.is_connected():
                db_Info = connection.get_server_info()
                print("Connected to MySQL Server version ", db_Info)
                cursor = connection.cursor()
                cursor.execute("select database();")
                record = cursor.fetchone()
                print("You're connected to database: ", record)

            # excute sql code
            cursor = connection.cursor()

            # execute the sql command:
            cursor.execute(sql)
            print('Succuessfully execute MySQL Command')

        except mysql.connector.Error as error:
            # if error arouses during the try, print out the error
            print("Failed to create table in MySQL: {}".format(error))
        
        finally:
            if (connection.is_connected()):
                cursor.close()
                connection.close()
                print("MySQL connection is closed")

    def DataTypeDFtoSQL(file):

        colnames_datatype = []

        data = pd.read_csv(file)

        colnames = data.columns.values
        for colname in colnames:
            col_data = data[colname]

            if colname == "Unnamed: 0":
                colname = "id"

            datatypes = col_data.apply(type)
            datatype = datatypes.describe().top.__name__

            if datatype == "int":
                SQL_datatype = "INT"
            elif datatype == "str":
                SQL_datatype = "TEXT"
            else:
                print("there is another datatype!!!!!!")

            colnames_datatype.append([colname, SQL_datatype])

        return colnames_datatype

    def create_table(file, table_name, primary=True):
        """
        function for creating elements of table (ex: id INT AUTO)
        """
        colnames_datatype = DataTypeDFtoSQL(file)

        elements = []
        for i in range(len(colnames_datatype)):
            colname_datatype = colnames_datatype[i]
            elements.append(" ".join(colname_datatype))

        if primary:
            elements.append("PRIMARY KEY (id)")
        else:
            pass

        table_elements = "(" + ",".join(elements) + ")"

        command = " ".join(["CREATE TABLE", table_elements])
        command = command + ";"

        return(command)

    
    def LoadTable(self, path, table_name):

        host = self.host
        user = self.user
        password = self.password
        database = self.database

        sql = ("LOAD DATA INFILE \'{path}\'" +
               " INTO TABLE {table_name}" +
               " FIELDS TERMINATED BY ','" +
               " ENCLOSED BY '\"'" +
               " LINES TERMINATED BY '\\n'" +
               " IGNORE 1 ROWS;").format(path=path, table_name=table_name)

        self.MysqlCommand(host, user, password, database, sql)
