import os
from urllib.request import urlopen

import pandas as pd
from bs4 import BeautifulSoup
from yaml import load
from yaml.loader import Loader

base_url = "http://books.toscrape.com/catalogue/"
url_page = base_url + "page-{}.html"


def _url_to_beautiful_soup(url):
    print((f"Getting {url}"))
    return BeautifulSoup(urlopen(url).read().decode("utf-8"), features="html.parser")


def get_book_info(n_page, logical_date):
    books = []
    for book_info in (
            _url_to_beautiful_soup(url=url_page.format(n_page))
                    .find("ol", {"class": "row"})
                    .findAll("li")[1:]
    ):

        book_soup = _url_to_beautiful_soup(
            url=base_url + book_info.find("a").get("href")
        )
        book = {
            "Title": book_soup.h1.get_text(),
            "Price": book_soup.find("p", {"class": "price_color"}).get_text(),
            "Availability": book_soup.find("p", {"class": "instock"}).text.strip(),
            "Rating": book_soup.find("p", {"class": "star-rating"}).get("class")[1],
        }

        for item_cat in book_soup.find("ul", {"class": "breadcrumb"}).findAll("a"):

            if item_cat.get_text() not in ("Home", "Books"):
                book["Category"] = item_cat.get_text()

        books.append(book)

    _save_to_gcs_bucket(dataframe=pd.DataFrame(books), page=n_page, logical_date=logical_date)


def get_range_pages():
    soup_principal = _url_to_beautiful_soup(url="http://books.toscrape.com")

    return range(1, int(
        soup_principal.find("li", {"class": "current"})
        .get_text()
        .replace("\n", "")
        .replace(" ", "")
        .split("of")[-1]
    ) + 1)


def _setup_credentials():
    def _read_yaml(path):
        with open(path, 'r') as f:
            dict_vars = load(f, Loader)

        return dict_vars

    yml_path = './datapipeline/airflow/credentials/google.yaml'

    try:
        dict_vars = _read_yaml(yml_path)
    except FileNotFoundError:
        dict_vars = _read_yaml(f"/home/virtual{yml_path[1:]}")

    for var in dict_vars:
        os.environ[var] = dict_vars[var]

    print('Credentials OK')


def _save_to_gcs_bucket(dataframe, page, logical_date):
    _setup_credentials()
    path_gcp = f'gs://book_club/raw/{logical_date}/page_{str(page).zfill(2)}.csv'
    dataframe.to_csv(path_gcp, index=False)
    print(f"Salvo em {path_gcp}")
