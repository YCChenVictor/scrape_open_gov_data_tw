from scrape_class import ScrapeClass
from functions.f_about_path import parent_path
import os

# this file path
file_path = os.path.dirname(os.path.abspath(__file__))
file_path_parent = parent_path(file_path, 1)

# find path for csv file downloading
csv_file_path = file_path_parent + "/csv_file"

# scrape the title and the file url
test = ScrapeClass()
test.SetUpChromeDriver(csv_file_path)
test.ScrapeData()
