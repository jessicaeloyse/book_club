import os
from datetime import datetime, timedelta, date
from urllib.request import urlopen

import pandas as pd
from airflow.decorators import dag, task
from bs4 import BeautifulSoup
from yaml import load
from yaml.loader import Loader

base_url = "http://books.toscrape.com/catalogue/"
url_page = base_url + "page-{}.html"


def url_to_beautiful_soup(url):
    print((f"Getting {url}"))
    return BeautifulSoup(urlopen(url).read().decode("utf-8"), features="html.parser")


def _get_book_info(n_page):
    books = []
    for book_info in (
            url_to_beautiful_soup(url=url_page.format(n_page))
                    .find("ol", {"class": "row"})
                    .findAll("li")[1:]
    ):

        book_soup = url_to_beautiful_soup(
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
    return books


def _setup_credentials():
    with open('./google.yaml', 'r') as f:
        dict_vars = load(f, Loader)

    for var in dict_vars:
        os.environ[var] = dict_vars[var]

    print('Credentials OK')


default_args = {
    'start_date': datetime(2022, 5, 24),
    'retries': 5,
    'retry_delay': timedelta(seconds=15),
}


@dag('book_club_dag', schedule_interval='0 17 * * *', default_args=default_args, catchup=False)
def book_club_dag():
    @task
    def extract_pages_info(limit_pages=None):
        soup_principal = url_to_beautiful_soup(url="http://books.toscrape.com")

        books = []
        final_page = limit_pages if limit_pages else int(
            soup_principal.find("li", {"class": "current"})
                .get_text()
                .replace("\n", "")
                .replace(" ", "")
                .split("of")[-1]
        ) + 1
        for n_page in range(1, final_page):
            book_list = _get_book_info(n_page)
            books.extend(book_list)

        pd_books = pd.DataFrame(books)

        return pd_books

    @task
    def save_to_gcs_bucket(dataframe):
        _setup_credentials()

        dataframe.to_csv(f'gs://book_club/raw/book_club_raw_{date.today().strftime("%Y%m%d")}.csv', index=False)
        print('Salvo com sucesso!')

    save_to_gcs_bucket(extract_pages_info())


dag = book_club_dag()
