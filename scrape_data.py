"""
記得要加入
.gitignore 跟 requirement.txt
"""

import requests
from bs4 import BeautifulSoup

# scrape the title and the file url
for number in range(9000, 9339):

    # the url of the webpage of the dataset
    url = "https://data.gov.tw/dataset/" + str(number)
    
    # connect the page
    page = requests.get(url)

    # get the content of the page
    soup = BeautifulSoup(page.content, 'html.parser')

    # get the title of the website
    title = soup.find_all("h1", class_="node-title")
    print(title)

    # get the url of the download url
    data = soup.find_all("a", string="CSV")
    print(data)

'''
page = requests.get("https://forecast.weather.gov/MapClick.php?lat=37.7772&lon=-122.4168#.XkvssJMzZQI")
soup = BeautifulSoup(page.content, 'html.parser')
seven_day = soup.find(id="seven-day-forecast")
# print(seven_day)
period_tags = seven_day.select(".tombstone-container .period-name")
periods = [pt.get_text() for pt in period_tags]
print(periods)
'''
# <a href="https://drive.google.com/uc?export=download&amp;confirm=jeak&amp;id=1ji6gKs_iMZIYmEyepXyaYXzweyP8YfaT" class="dgresource ff-icon ff-icon-csv externallink" data-nid="9338">CSV</a>
# <a href="https://ws.moe.edu.tw/001/Upload/4/relfile/0/4764/a27e7193-392e-4780-9133-f27d3a65c791.csv" class="dgresource ff-icon ff-icon-csv externallink" data-nid="28387">CSV</a>