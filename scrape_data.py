from functions.scrape_class import ScrapeOneData, MySQLWithPython
# from functions.f_about_path import parent_path
import os
from docs.identity import password, location, database, host, user

# scrape the title and the file url
test = ScrapeOneData()
title, url = test.ScrapeTitleAndDownloadURL(11549)
test.DownloadCSVFile(title, url, location)

# MySQL command
print("loading data: ", title, " into", " gov")
MySQL_EXE = MySQLWithPython(host, user, password, database)

# execute the MySQL command

# create table accroding to csv titles
file_path = location + title + ".csv"
marks = [" ", ">"]
title = MySQL_EXE.TableName(title) # No special character in table name

sqlcommand = MySQL_EXE.CreateTable(file_path, title)
MySQL_EXE.MysqlCommand(sqlcommand)

# load the csv table into DataBase
# sqlcommand.LoadTable(path, table_name)
