import os
import sys
import logging
import zipfile
import urllib
import ssl
from datetime import date, timedelta, datetime
from bs4 import BeautifulSoup
from urllib.request import urlopen
from urllib.error import URLError, HTTPError


logging.basicConfig(level=logging.INFO)
local_dir = os.getenv('FILES')


def download_url(url, save_path):
    """
        Function to download files from a specific URL.
    """
    logging.info("Downloading file to: " + save_path)
    try:
        ssl._create_default_https_context = ssl._create_unverified_context
        r = urllib.request.urlretrieve(url=url, filename=save_path)
        logging.info("File was found, and downloading the file")

    except HTTPError as e:
        logging.info(e)

    except URLError as e:
        logging.info(e)


def unzip_files(folder_path, file_path):
    with zipfile.ZipFile(file_path, "r") as zip_ref:
        zip_ref.extractall(folder_path)


def validate_existing_file(local_dir, file_name):
    for file in os.listdir(local_dir):
        if file_name in os.listdir(local_dir):
            return False

    return True

    return True


# url_list = [
#     "https://www.portaldatransparencia.gov.br/download-de-dados/auxilio-brasil/" + dt_refer,
#     "https://www.portaldatransparencia.gov.br/download-de-dados/auxilio-emergencial/" + dt_refer,
#     "https://www.portaldatransparencia.gov.br/download-de-dados/bolsa-familia-pagamentos/" + dt_refer,
#     "https://www.portaldatransparencia.gov.br/download-de-dados/bolsa-familia-saques/" + dt_refer,
#     "https://www.portaldatransparencia.gov.br/download-de-dados/bpc/" + dt_refer,
#     "https://www.portaldatransparencia.gov.br/download-de-dados/garantia-safra/" + dt_refer,
#     "https://www.portaldatransparencia.gov.br/download-de-dados/peti/" + dt_refer,
#     "https://www.portaldatransparencia.gov.br/download-de-dados/seguro-defeso/" + dt_refer
# ]
# save_path_list = [
#     local_dir + "/auxilio_brasil" + dt_refer + ".zip",
#     local_dir + "/auxilio-emergencial" + dt_refer + ".zip",
#     local_dir + "/bolsa-familia-pagamentos" + dt_refer + ".zip",
#     local_dir + "/bolsa-familia-saques" + dt_refer + ".zip",
#     local_dir + "/bpc" + dt_refer + ".zip",
#     local_dir + "/garantia-safra" + dt_refer + ".zip",
#     local_dir + "/peti" + dt_refer + ".zip",
#     local_dir + "/seguro-defeso" + dt_refer + ".zip"
# ]
# file_name_list = [
#     dt_refer + "_AuxilioBrasil.csv",
#     dt_refer + "_AuxilioEmergencial.csv",
#     dt_refer + "_BolsaFamilia_Pagamentos.csv",
#     dt_refer + "_BolsaFamilia_Saques.csv",
#     dt_refer + "_BPC.csv",
#     dt_refer + "_GarantiaSafra.csv",
#     dt_refer + "_PETI.csv",
#     dt_refer + "_SeguroDefeso.csv"
# ]


def main():
    date_list = [1, 36, 66, 96, 126, 156]

    for i in date_list:
        last_day = date.today().replace(day=1) - timedelta(days=i)
        dt_refer = last_day.strftime('%Y%m')

        url = "https://www.portaldatransparencia.gov.br/download-de-dados/auxilio-brasil/" + dt_refer
        save_path = local_dir + "/auxilio-brasil" + dt_refer + ".zip"
        file_name = dt_refer + "_" + "AuxilioBrasil.csv"
        response = download_url(url, save_path)

    try:
        logging.info("Unzipping files")
        for file in os.listdir(local_dir):
            if file.endswith(".zip") and zipfile.is_zipfile(local_dir + "/" + file):
                unzip_files(local_dir, local_dir + "/" + file)
    except zipfile.BadZipFile as e:
        logging.info(f"Something went wrong unziping file: {e}")

    logging.info(f"Cronjob successfully finished")

if __name__ == '__main__':
    main()

