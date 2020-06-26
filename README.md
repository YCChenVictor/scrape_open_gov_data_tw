# Scrape Taiwan Open Gov Data

This project scrapes all the csv files from Taiwan Open Gov Data

## Installation

```bash
pip install -r requirements.txt
```

## Usage

create docs (identity.py and record.pickle) and csv_files (downloaded csv files) folder

```bash
python scrape_data.py # for data scraping and load csv file into MySQL database
python show_status.py # to show to problems during scrape and load
python build_map_relationship.py # to build tables relationship with county id
```

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License
[MIT](https://choosealicense.com/licenses/mit/)
