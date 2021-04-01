import os
import shutil
import logging

import requests
import pandas as pd


logging.basicConfig(
    filename="bike-traffic-munich.log", 
    format="%(asctime)s %(levelname)s %(message)s", 
    encoding="utf-8", 
    level=logging.DEBUG)


def reset_directories():
    for dir_name in ["csv", "dataset"]:
        shutil.rmtree(dir_name, ignore_errors=True)
        os.mkdir(dir_name)
    return
    
    
def download_csv():
    
    # Find all urls of the csv files to download. 
    months = ["Januar", "Februar", "März", "April", "Mai", "Juni", "Juli", "August", "September", "October", "November", "Dezember"]
    queries = ["Raddauerzählstellen München {} {}".format(month, year) for year in range(2017, 2022) for month in months]
    csv_urls = []
    for query in queries:
        r = requests.get("https://www.opengov-muenchen.de/api/3/action/package_search", params = {"q": query})
        query_result = r.json()["result"]
        logging.debug("The query for {} returned {} results.".format(query, query_result["count"]))
        for result in query_result["results"]:
            for resource in result["resources"]:
                res_name = resource["name"]
                if query in res_name:
                    logging.info("Found resource \"{}\" in the results of query \"{}\".".format(res_name, query))
                    csv_urls.append(resource["url"])
                else:
                    logging.debug("Ignored resource \"{}\" in the results of query \"{}\".".format(res_name, query))
    
    # Link to the csv of the resource "Raddauerzählstellen in München"
    # https://www.opengov-muenchen.de/dataset/raddauerzaehlstellen-muenchen/resource/211e882d-fadd-468a-bf8a-0014ae65a393
    csv_urls.append("https://www.opengov-muenchen.de/dataset/aca4bcb6-d0ff-4634-b5b9-8b5d133ab08e/resource/211e882d-fadd-468a-bf8a-0014ae65a393/download/radzaehlstellen.csv")
                
    # Download the csv files to the "csv" folder
    for csv_url in csv_urls:
        csv_name = csv_url.split("/")[-1]
        csv_path = os.path.join("csv", csv_name)
        r_csv = requests.get(csv_url)
        with open(csv_path, "wb") as f:
            f.write(r_csv.content)
        logging.info("Saved \"{}\"".format(csv_path))
    return


def build_dataset():
    
    stations_csv = "radzaehlstellen.csv"
    os.rename(os.path.join("csv", stations_csv), os.path.join("dataset", stations_csv))
    csv_data_day = []
    csv_data_15min = []
    filenames = list(os.walk("csv"))[0][2]
    for filename in filenames:
        file_path = os.path.join("csv", filename)
        logging.info("Read data from \"{}\"".format(file_path))
        df = pd.read_csv(file_path)
        if "tage" in filename:
            csv_data_day.append(df)
        elif "15min" in filename: 
            csv_data_15min.append(df)
        else:
            logging.error("Unknown file \"{}\"".filename(file_path))
        
    csv_day_concat = pd.concat(csv_data_day, ignore_index=True)
    csv_15min_concat = pd.concat(csv_data_15min,ignore_index=True)
    logging.info("Dataframes concatenated.")
    
    #TODO Datum format is incosistent in July 2019: DD.MM.YYYY instead of YYYY.MM.TT 
    
    sort_clms = ["datum", "uhrzeit_start", "zaehlstelle"]
    csv_day_concat.sort_values(sort_clms)
    csv_15min_concat.sort_values(sort_clms)
    logging.info("Dataframes sorted.")
    
    csv_15min_concat.to_csv("dataset/rad_15min.csv", index=False)
    csv_day_concat.to_csv("dataset/rad_tage.csv", index=False)
    logging.info("Dataframes saved to csv.")
    
    return
    
    
if __name__ == "__main__":
    try:
        reset_directories()
        download_csv()
        build_dataset()
    except Exception as e:
        logging.error("Dataset update failed. {}".format(e))
    else:
        logging.info("Dataset updated successfully.")
        
    
    