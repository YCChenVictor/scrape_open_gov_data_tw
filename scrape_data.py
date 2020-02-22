"""
記得要加入
.gitignore 跟 requirement.txt
"""
from scrape_class import scrape_class
# import time
# import re

# scrape the title and the file url
test = scrape_class()
data = test.scrape_data()
print(data)
