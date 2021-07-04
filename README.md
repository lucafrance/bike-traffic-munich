# Bike Traffic in Munich

This code collects and consolidates for [Kaggle](https://www.kaggle.com) information about the bike traffic in the city of Munich.

### Requisites
* [Requests](https://docs.python-requests.org)
* [pandas](https://pandas.pydata.org/)
* [Kaggle API](https://github.com/Kaggle/kaggle-api)

### Usage
* To download the source csv: `python bike_data.py "d"`
* To build the dataset: `python bike_data.py "b"`
* To bot download the source csv and build the dataset: `python bike_data.py "db"`
* To upload a new version of the dataset on Kaggle: `kaggle datasets version -m "Updated dataset" -p "./dataset"`

### Links
* [Resulting dataset on Kaggle](https://www.kaggle.com/lucafrance/bike-traffic-in-munich)
* [Open Data Portal - Raddauerzählstellen](https://www.opengov-muenchen.de/pages/raddauerzaehlstellen)
* [Datensätze - Raddauerzählstellen](https://www.opengov-muenchen.de/dataset?tags=Raddauerz%C3%A4hlstellen)