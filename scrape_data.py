from functions.scrape_class import ScrapeOneData, MySQLWithPython
import os
from docs.identity import config, location
import pandas as pd
import time
import requests
import io
import traceback
import json

"""
從5945開始有資料
5950有資料，可是沒有下載下來 (中華郵政全國營業據點)
5951, 5952 .zip 目前無法下載，根本就進不去

522 + 65 + 659 + 250 + 529 + 662 + 16 + 1684 + 835 + 1755 + 997 + 833 + 95 + 2651 + 26 + 256 + 94 + 32391

想辦法放到external hard disk裡
"""

for i in range(5945, 522 + 65 + 659 + 250 + 529 + 662 + 16 + 1684 + 835 + 1755 + 997 + 833 + 95 + 2651 + 26 + 256 + 94 + 32391):

    print("================================")
    print("downloading dataset: ", i)    

    # scrape the title and the file url with data over 10000
    Scrape = ScrapeOneData()
    try:
        title, url = Scrape.ScrapeTitleAndDownloadURL(i)
        print("trying to scrape, ",title, "url: ", url)
    except KeyboardInterrupt:
        stored_exception=sys.exc_info()    

    if url is None:
    	continue
    else:
        # get the content from csv url
        csv_length = Scrape.GetContentFromCSVUrl(url)

        # create and load table
        SQL = MySQLWithPython(config, location)
        SQL.CreateLoadTable(title)

    # record
    with open('./docs/records.json', 'r') as read_file:
        dict_data = json.load(read_file)

    data = {'title': title, 'csv_url': url, 'num_of_data': csv_length}
    dict_data[i] = data

    with open('./docs/records.json', 'w') as f:
        json.dump(dict_data, f)

