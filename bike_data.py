import os
import shutil
import logging

import requests
import pandas as pd


logging.basicConfig(
    filename="bike-traffic-munich.log", 
    format="%(asctime)s %(levelname)s %(message)s", 
    encoding="utf-8", 
    level=logging.INFO)


def reset_directories():
    for dir_name in ["csv", "dataset"]:
        shutil.rmtree(dir_name, ignore_errors=True)
        os.mkdir(dir_name)
    return
    
    
def download_csv():
    r = requests.get("https://www.opengov-muenchen.de/api/3/action/package_search?q=Raddauerz%C3%A4hlstellen")
    for result in r.json()["result"]["results"]:
        for resource in result["resources"]:
            csv_url = resource["url"]
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
        
    
    