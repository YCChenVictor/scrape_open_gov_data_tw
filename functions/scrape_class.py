import requests
from bs4 import BeautifulSoup
import pandas as pd
import ssl
import mysql.connector # pip install mysql-connector
import shutil
import time
from docs.identity import config, location
import traceback
import io
import backoff

'''
無法解決：
1. (5950)ConnectionError(ProtocolError('Connection aborted.', OSError("(54, 'ECONNRESET')")))
2. (5951)ConnectionError(MaxRetryError('None: Max retries exceeded with url: /opencms/files/openData/04.zip (Caused by None)
'''

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

        # the place to save the length of csv file
        self.length = None

        # check whether to get the csv content
        self.GetTheContent = False

        # check whether to get the title of website
        self.title = None

        # check whether to get the csv_url of website
        self.csv_url = None

        # the minimum number of data 
        self.minimum = 1000

    def GetTheNumberOfDataset():
        # the way to find the total number of datasets
        pass

    def GetTheDownloadUrl():
    	# should find a way if there is no CSV form
    	pass

    def FindEncoding():
    	# find a way to solve different Encoding problem
    	pass

    @backoff.on_exception(backoff.expo,
                      requests.exceptions.RequestException,
                      max_time=60)
    def get_url(self, url):
        return requests.get(url, verify=False)

    def ScrapeTitleAndDownloadURL(self, Number):

        driver = self.driver

        DataDict = {}

        # the url of the webpage of the dataset
        Url = "https://data.gov.tw/dataset/" + str(Number)
        # print("url: ", Url)

        # connect the page
        Page = self.get_url(Url)

        # get the content of the page
        Soup = BeautifulSoup(Page.content, 'html.parser')

        # get the title of the website
        try:
            Title = Soup.find_all("h1", class_="node-title")[0].text
            if Title == "404":
                print("no website")
                return
            else:
                print("Title: ", Title)
                self.title = Title
        except Exception as e:
            print("error during getting title: ", e)
            pass

        # get the download url (should try to fix it with GetTheDownloadUrl())
        try:
            csv_url = Soup.find_all("a", string="CSV")[0]['href']
            print("found csv_url: ", csv_url)
            self.csv_url = csv_url
        except Exception as e:
            print("error during getting csv_url: ", e)
            pass

    def GetContentFromCSVUrl(self, url):

        # csv file
        url_content = self.get_url(url).content
        try:
            excel = pd.read_excel(io.StringIO(url_content.decode('utf-8', 'ignore')), error_bad_lines=False)
        except:
            excel = pd.read_csv(io.StringIO(url_content.decode('utf-8', 'ignore')), error_bad_lines=False)
        length = len(excel.index)
        self.length = length

        # check the number data is above 10000 or not
        if len(excel.index) < self.minimum:
            print("the number of data is too few, length: ", len(excel.index))
            return
        else:
            excel.to_csv(location + self.title + ".csv")
            self.GetTheContent = True

class MySQLWithPython(): # the function to do SQL command through python

    def __init__(self, config, location):

        self.config = config

        self.location = location

        # whether to choose datatype of SQL
        self.whether_choose_datatype = False

        # whether to SKIP All Error
        self.whether_SKIP_all_error = False

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

        # determine the datatype of SQL (net of empty)
        for colname in colnames:

            col_data = data[colname]
            col_data = col_data.dropna()

            if colname == "Unnamed: 0":
                colname = "id"

            colname = self.NameForSQL(colname)

            datatypes = col_data.apply(type)
            datatype = datatypes.describe().top.__name__

            if colname == "id":
                SQL_datatype = "INT"
            elif not self.whether_choose_datatype:
                SQL_datatype = "TEXT"
                print("Specify SQL datatype to TEXT anyway")
            elif datatype == "int":
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
        marks = [">", "<", "/", "(", ")"]      
        for mark in marks:
            name = name.replace(mark, "")

        # replace space into _
        name = name.replace(" ", "_")

        return name

    def CommandDealWithEmptyToNULL(self, file):
        """
        function to create command to deal with Empty value and all to create table
        """
        data = pd.read_csv(file)
        colnames = data.columns.values

        # create null command
        nullif_statements = []
        i = 0
        for colname in colnames:
            
            i += 1

            if colname == "Unnamed: 0":
                colname = "id"

            colname = self.NameForSQL(colname)

            nullif_statement = colname + " = NULLIF(@col" + str(i) + ",'')"
            nullif_statements.append(nullif_statement)

        nullif_command = ",".join(nullif_statements)
        nullif_command = " ".join(["SET", nullif_command])

        # create @ statement
        at_col_statements = []
        for i in range(len(colnames)):
            
            i += 1

            at_col_statement = "@col" + str(i)
            at_col_statements.append(at_col_statement)

        at_col_command = ",".join(at_col_statements)
        at_col_command = "(" + at_col_command + ")"

        # full statement
        full_command = " ".join([at_col_command, nullif_command])

        return full_command

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

        # sql command to deal with Empty to NULL
        empty_to_null = self.CommandDealWithEmptyToNULL(file)

        if self.whether_SKIP_all_error:
            command = ("LOAD DATA INFILE '{path}\'" +
                       "SKIP ALL ERRORS" +
                       " INTO TABLE {table_name}" +
                       " FIELDS TERMINATED BY ','" +
                       " ENCLOSED BY '\"'" +
                       " LINES TERMINATED BY '\\n'" +
                       " IGNORE 1 ROWS" +
                       empty_to_null +
                       ";").format(path=file, table_name=table_name)
        else:
            # sql command for loading data
            command = ("LOAD DATA INFILE '{path}\'" +
                       " INTO TABLE {table_name}" +
                       " FIELDS TERMINATED BY ','" +
                       " ENCLOSED BY '\"'" +
                       " LINES TERMINATED BY '\\n'" +
                       " IGNORE 1 ROWS" +
                       empty_to_null +
                       ";").format(path=file, table_name=table_name)
        print(command)

        return ([command])

    def CreateLoadTable(self, title):

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
        print(sqlcommands)
        self.ExecuteMysqlCommand(sqlcommands)
