from docs.identity import location, config
from functions.mysql_class import MySQLWithPython
import mysql.connector
from docs.counties_list import counties

# connect to mysql gov database
MySQL = MySQLWithPython(config)

'''
# create county table (command must be in list type, so that multiple elements of code can be execute in one connection)
command = ["CREATE TABLE counties_test (id INT NOT NULL AUTO_INCREMENT, county_name TEXT, PRIMARY KEY (id));"]
MySQL.ExecuteMysqlCommand(command, return_result=False)

# insert county data into table
command = ["INSERT INTO counties_test (county_name) VALUES ('臺北市'), ('新北市'), ('基隆市'), ('桃園市'), ('新竹市'), ('新竹縣'), ('苗栗縣'), ('臺中市'), ('彰化縣'), ('南投縣'), ('雲林縣'), ('嘉義市'), ('嘉義縣'), ('臺南市'), ('高雄市'), ('屏東縣'), ('宜蘭縣'), ('花蓮縣'), ('臺東縣');"]
MySQL.ExecuteMysqlCommand(command, return_result=False)
'''

tables = MySQL.ExecuteMysqlCommand(["SHOW TABLES"])

'''
# iteratively delete wrong column
for table in tables:
    table = ''.join(table)
    command = MySQL.CommandDeleteColumn(table, "county")
    MySQL.ExecuteMysqlCommand(command, return_result = False)

# Iteratively create column for counties in tables
# the command to add column of counties
for table in tables:
    table = ''.join(table)
    command = MySQL.CommandAddColumn(table, "county", "TEXT")
    MySQL.ExecuteMysqlCommand(command, return_result = False)

# find the data including counties then add the county into county column
for table in tables:
    table = ''.join(table)
    county_list = MySQL.CommandFindCounty(table, counties)
    # update data into county column
    MySQL.CommandUpdateCounty(table, county_list)
'''

'''
# change the county index datatype into INTEGER
for table in tables:
    table = ''.join(table)
    command = MySQL.CommandChangeDataType(table, "county", "INTEGER")
    MySQL.ExecuteMysqlCommand(command, return_result = False)
'''

# build relationship according to counties
for table in tables:
    table = ''.join(table)
    command = MySQL.CommandBuildRelationship(table)
    MySQL.ExecuteMysqlCommand(command, return_result = False)
