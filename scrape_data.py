from functions.scrape_class import ScrapeOneData, MySQLWithPython
import os
from docs.identity import config, location
import pandas as pd
import time

"""
5943有資料，可是沒有下載下來

522 + 65 + 659 + 250 + 529 + 662 + 16 + 1684 + 835 + 1755 + 997 + 833 + 95 + 2651 + 26 + 256 + 94 + 32391
"""

for i in range(5943, 5944):
    
    try:

        print("================================")
        print("downloading dataset: ", i)    

        # scrape the title and the file url with data over 10000
        test = ScrapeOneData()
        try:
            title, url = test.ScrapeTitleAndDownloadURL(i)
        except KeyboardInterrupt:
            stored_exception=sys.exc_info()    

        # check the number data is above 10000 or not
        if url is None:
        	continue
        if len(pd.read_csv(url).index) < 10000:
            print("the number of data is too few")
            continue    

        test.DownloadCSVFile(title, url, location)    

        # the file path
        file_path = location + title + ".csv"    

        # MySQL command
        print("loading data: ", title, " into", " gov")
        MySQL_EXE = MySQLWithPython(config, location)    

        # execute the MySQL command    

        # create table accroding to csv titles
        print("====================")
        print("creating table~~~~~")
        title_for_sql = MySQL_EXE.TableName(title) # No special character in table name
        sqlcommands = MySQL_EXE.CommandCreateTable(file_path, title_for_sql)
        MySQL_EXE.ExecuteMysqlCommand(sqlcommands)    

        # load the csv table into DataBase
        print("==================")
        print("loading table~~~~~")
        sqlcommands = MySQL_EXE.CommandLoadTable(file_path, title_for_sql)
        MySQL_EXE.ExecuteMysqlCommand(sqlcommands)

    except:

        print("Connection refused by the server..")
        print("Let me sleep for 5 seconds")
        print("ZZzzzz...")
        time.sleep(5)
        print("Was a nice sleep, now let me continue...")
        continue
