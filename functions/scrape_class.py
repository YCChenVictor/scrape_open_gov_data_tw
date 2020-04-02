import requests
from bs4 import BeautifulSoup
import pandas as pd
import ssl
import mysql.connector
import shutil
import time
from docs.identity import config, location
import traceback
import io

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
                print("no website")
            else:
                print("Title: ", title)
        except:
            pass

        # get the download url (should try to fix it with GetTheDownloadUrl())
        try:
            DownloadUrl = Soup.find_all("a", string="CSV")[0]['href']
            print("found csv_url: ", DownloadUrl)
        except:
            DownloadUrl = None
            print("no download url for csv")
            pass

        return [Title, DownloadUrl]

    def DownloadCSVFile(self, Title, DownloadUrl, location):

        data = pd.read_csv(DownloadUrl)

        data.to_csv(location + Title + ".csv")

        # print(type(data))

        return data

    def GetContentFromCSVUrl(self, url):

        try:
            url_content = requests.get(url).content
            csv = pd.read_csv(io.StringIO(url_content.decode('utf-8')))
            # check the number data is above 10000 or not
            if len(csv.index) < 10000:
                print("the number of data is too few, length: ", len(csv.index))
                return
            else:
                self.DownloadCSVFile(title, url, location)
        except Exception as e:
            print(e)
            print("Let me sleep for 5 seconds")
            print("ZZzzzz...")
            time.sleep(5)
            print("Was a nice sleep, now let me continue...")
            pass

        return len(csv.index)


class MySQLWithPython(): # the function to do SQL command through python

    def __init__(self, config, location):

        self.config = config
        self.location = location

    def ExecuteMysqlCommand(self, sqlcommands):

        config = self.config
        location = self.location

        try:
            # connect to database
            connection = mysql.connector.connect(**config)

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
            for sqlcommand in sqlcommands:
                cursor.execute(sqlcommand)
                connection.commit()
                print('Succuessfully execute MySQL Command: ', sqlcommand)

        except mysql.connector.Error as error:
            # if error arouses during the try, print out the error
            print("Failed in MySQL: {}".format(error))
        
        finally:
            if (connection.is_connected()):
                cursor.close()
                connection.close()
                print("MySQL connection is closed")

    def DataTypeDFtoSQL(self, file):

        colnames_datatype = []

        data = pd.read_csv(file)

        colnames = data.columns.values

        for colname in colnames:

            col_data = data[colname]

            if colname == "Unnamed: 0":
                colname = "id"

            colname = self.NameForSQL(colname)

            datatypes = col_data.apply(type)
            datatype = datatypes.describe().top.__name__

            if datatype == "int":
                SQL_datatype = "INT"
            elif datatype == "str":
                SQL_datatype = "TEXT"
            elif datatype == "float":
                SQL_datatype = "DECIMAL"
            else:
                print("there is another datatype!!!!!!")

            colnames_datatype.append([colname, SQL_datatype])

        return colnames_datatype

    def NameForSQL(self, name):
  
        # replace all marks except for space
        marks = [">", "<"]      
        for mark in marks:
            name = name.replace(mark, "")

        # replace space into _
        name = name.replace(" ", "_")

        return name

    def CommandCreateTable(self, file, table_name, primary=True):
        """
        function for creating elements of table (ex: id INT AUTO)
        """
        colnames_datatype = self.DataTypeDFtoSQL(file)

        elements = []
        for i in range(len(colnames_datatype)):
            colname_datatype = colnames_datatype[i]
            elements.append(" ".join(colname_datatype))

        if primary:
            elements.append("PRIMARY KEY (id)")
        else:
            pass

        table_elements = "(" + ",".join(elements) + ")"

        command = " ".join(["CREATE TABLE", table_name ,table_elements])
        command = command + ";"

        return([command])

    def CommandLoadTable(self, file, table_name):

        # sql command for loading data
        command = ("LOAD DATA INFILE \'{path}\'" +
                   " INTO TABLE {table_name}" +
                   " FIELDS TERMINATED BY ','" +
                   " ENCLOSED BY '\"'" +
                   " LINES TERMINATED BY '\\n'" +
                   " IGNORE 1 ROWS;").format(path=file, table_name=table_name)

        return ([command])

    def CreateLoadTable(self, title):

        try:
            # MySQL command
            print("====================")
            print("loading data: ", title, " into", " gov")  

            # execute the MySQL command    

            # get the file path
            file_path = location + title + ".csv"  

            # create table accroding to csv titles
            print("====================")
            print("creating table~~~~~")
            title_for_sql = self.NameForSQL(title) # No special character in table name
            sqlcommands = self.CommandCreateTable(file_path, title_for_sql)
            self.ExecuteMysqlCommand(sqlcommands)    

            # load the csv table into DataBase
            print("==================")
            print("loading table~~~~~")
            sqlcommands = self.CommandLoadTable(file_path, title_for_sql)
            self.ExecuteMysqlCommand(sqlcommands)

        except Exception as e:
            traceback.print_exc()
            pass
