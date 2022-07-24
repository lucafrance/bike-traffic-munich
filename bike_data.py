import os
import sys
import shutil
import logging

import requests
import pandas as pd


logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s", 
    encoding="utf-8",
    handlers=[
        logging.FileHandler("bike-traffic-munich.log"),
        logging.StreamHandler()
    ],
    level=logging.INFO)


def reset_directory(dir_name):
    shutil.rmtree(dir_name, ignore_errors=True)
    os.mkdir(dir_name)
    return
    
    
def download_csv():
    
    # Find all urls of the csv files to download. 
    months = ["Januar", "Februar", "März", "April", "Mai", "Juni", "Juli", "August", "September", "Oktober", "November", "Dezember"]
    queries = ["Raddauerzählstellen München {} {}".format(month, year) for year in range(2017, 2023) for month in months]
    csv_urls = []
    
    for query in queries:
        r = requests.get("https://www.opengov-muenchen.de/api/3/action/package_search", params = {"q": query})
        query_result = r.json()["result"]
        logging.debug("The query for \"{}\" returned {} results.".format(query, query_result["count"]))
        
        query_tageswerte_und_werte_found = False
        query_15_minuten_werte_found = False
        
        for result in query_result["results"]:
            for resource in result["resources"]:
                res_name = resource["name"]
                keywords = query.split(" ")
                include = True
                for keyword in keywords:
                    if keyword not in res_name:
                        include = False
                if include:
                    logging.info("Found resource \"{}\" in the results of query \"{}\".".format(res_name, query))
                    csv_urls.append(resource["url"])
                    if "Tageswerte und Wetter" in res_name:
                        query_tageswerte_und_werte_found = True
                    if "15 Minuten-Werte" in res_name:
                        query_15_minuten_werte_found = True
                else:
                    logging.debug("Ignored resource \"{}\" in the results of query \"{}\".".format(res_name, query))
        
        if not query_tageswerte_und_werte_found:
            logging.warning("No valid daily values results for query \"{}\".".format(query))
        if not query_15_minuten_werte_found:
            logging.warning("No valid 15-minutes values results for query \"{}\".".format(query))
        if not (query_tageswerte_und_werte_found and query_15_minuten_werte_found):
            logging.warning("You might want to check the response to \"{}\".".format(r.url))

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
    shutil.copyfile(os.path.join("csv", stations_csv), os.path.join("dataset", stations_csv))
    csv_data_day = []
    csv_data_15min = []
    filenames = list(os.walk("csv"))[0][2]
    for filename in filenames:
        if filename == stations_csv:
            continue
        file_path = os.path.join("csv", filename)
        logging.info("Read data from \"{}\"".format(file_path))
        df = pd.read_csv(file_path, dtype={"datum": str, "uhrzeit_start": str, "uhrzeit_ende": str})
        
        # Convert dates and times to datetime objects and then back to a consistent string format
        df["datum"] = pd.to_datetime(df["datum"], dayfirst=True)
        df["datum"] = df["datum"].dt.strftime("%Y.%m.%d")
        for time_clm in ["uhrzeit_start", "uhrzeit_ende"]:
            # uhrzeit_ende is represented as 23.59 instead of 23:59 in daily values (radYYYYMMDDtage.csv)
            df[time_clm] = df[time_clm].str.replace(".", ":", regex=False)
            df[time_clm] = pd.to_datetime(df[time_clm])
            df[time_clm] = df[time_clm].dt.strftime("%H:%M")
        
        if "tage" in filename:
            csv_data_day.append(df)
        elif "15min" in filename: 
            csv_data_15min.append(df)
        else:
            logging.error("Unknown file \"{}\"".format(file_path))
    
    args = {"ignore_index": True}
    csv_day_concat = pd.concat(csv_data_day, **args)
    csv_15min_concat = pd.concat(csv_data_15min, **args)
    logging.info("Dataframes concatenated.")
    
    sort_clms = ["datum", "uhrzeit_start", "zaehlstelle"]
    csv_day_concat.sort_values(sort_clms, inplace=True)
    csv_15min_concat.sort_values(sort_clms, inplace=True)
    logging.info("Dataframes sorted.")
    
    csv_15min_concat.to_csv("dataset/rad_15min.csv", index=False)
    csv_day_concat.to_csv("dataset/rad_tage.csv", index=False)
    logging.info("Dataframes saved to csv.")
    
    return
    
    
if __name__ == "__main__":
    
    help_msg = "Run the script with the argument \"d\" to download the csv files, \"b\" to build the dataset and \"db\" to perform both operations."
    if len(sys.argv) == 1:
        print(help_msg)
    else:
        args = sys.argv[1]
        try:
            if "d" in args:
                reset_directory("csv")
                download_csv()
            if "b" in args:
                build_dataset()
            if "d" not in args and "b" not in args:
                print(help_msg)
        except Exception as e:
            logging.error("Dataset update failed. {}".format(e))
            raise
        else:
            logging.info("Dataset updated successfully.")
        
    