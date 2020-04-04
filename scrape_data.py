from functions.scrape_class import ScrapeOneData, MySQLWithPython
import os
from docs.identity import config, location
import pandas as pd
import time
import requests
import io
import traceback
import pickle

"""
從 5945 開始有資料
5950 (中華郵政全國營業據點)因為有特殊符號，沒有存進SQL
5951, 5952 因為.zip 目前無法下載，根本就進不去網頁
5996 的 SQL error 是因為太多 Empty 在同一個 column，導致給錯 type，這是可以解決的，在一開始決定 type 的時候

'error_during_csv_content': unsupported operand type(s) for <<: 'str' and 'int'(沒有解決)
因為實在太多 datatype error，唯一的辦法可能就是先全部都設為 TEXT，不過這個方法可以等到第二回scraping的時候
想辦法放到external hard disk裡
"""

'''
dict_data = {}
with open('./docs/record.pickle', 'wb') as handle:
    pickle.dump(dict_data, handle, protocol=pickle.HIGHEST_PROTOCOL)
'''

all_num = 522 + 65 + 659 + 250 + 529 + 662 + 16 + 1684 + 835 + 1755 + 997 + 833 + 95 + 2651 + 26 + 256 + 94 + 32391

for i in range(5996, all_num):

    print("================================")
    print("downloading dataset: ", i)    

    data = {}

    # scrape the title and the file url with data over 10000
    Scrape = ScrapeOneData()
    try:
        Scrape.ScrapeTitleAndDownloadURL(i)
        data["title"] = Scrape.title
        data["url"] = Scrape.csv_url
        print("trying to scrape, ",Scrape.title, "url: ", Scrape.csv_url)
    except KeyboardInterrupt:
        stored_exception=sys.exc_info()    

    url = Scrape.csv_url
    if url is None:
    	continue
    else:
        try: # get the content from csv url
            Scrape.GetContentFromCSVUrl(url)
            csv_length = Scrape.length
            data["num_of_data"] = csv_length
            data["error_during_csv_content"] = None
        except Exception as e_during_csv_content:
            print("error_during_csv_content: " ,e_during_csv_content)
            data["error_during_csv_content"] = e_during_csv_content
            print("Let me sleep for 5 seconds")
            print("ZZzzzz...")
            time.sleep(5)
            print("Was a nice sleep, now let me continue...")
            pass

        # create and load table
        data["error_during_SQL_command"] = None
        if Scrape.GetTheContent:
            try:
                SQL = MySQLWithPython(config, location)
                SQL.whether_choose_datatype = False
                SQL.CreateLoadTable(Scrape.title)
            except Exception as e_during_SQL_command:
                data["error_during_SQL_command"] = e_during_SQL_command
                traceback.print_exc()
                pass

    # open the record file
    print(data)
    with open('./docs/record.pickle', 'rb') as handle:
        dict_data = pickle.load(handle)

    dict_data[i] = data

    with open('./docs/record.pickle', 'wb') as handle:
        pickle.dump(dict_data, handle, protocol=pickle.HIGHEST_PROTOCOL)
