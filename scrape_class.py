import requests
from bs4 import BeautifulSoup
import pandas as pd

class scrape_class():
    
    def __init__(self):
         
        # the dictionary to store the scraped data
        self.data_dict = None
        self.the_number_of_dataset = 522 + 65 + 659 + 250 + 529 + 662 + 16 + 1684 + 835 + 1755 + 997 + 833 + 95 + 2651 + 26 + 256 + 94 + 32391

    def get_the_number_of_dataset():
        pass

    def scrape_data(self):

        data_dict = {}

        dataset_num = self.the_number_of_dataset

        for number in range(9001, dataset_num + 1):

            print("============")

	        # the url of the webpage of the dataset
            url = "https://data.gov.tw/dataset/" + str(number)
            print(url)

	        # connect the page
            page = requests.get(url)

	        # get the content of the page
            soup = BeautifulSoup(page.content, 'html.parser')

	        # get the title of the website
            title = soup.find_all("h1", class_="node-title")[0].text
            if title == "404":
        	    continue

	        # get the download url
            try:

		        # start to get the data
    	        download_url = soup.find_all("a", string="CSV")[0]['href']
    	        print(download_url)
    	        data = pd.read_csv(download_url)
    	        print(len(data.index))

            except Exception as e:
    	        print(e)

            data_dict[title] = [len(data.index), data]

        return data_dict

