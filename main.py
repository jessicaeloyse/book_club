from urllib.request import urlopen
import pandas as pd
from bs4 import BeautifulSoup
from retry import retry
import logging

logger = logging.getLogger("Book_Club")
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - (%(levelname)s) - %(message)s")
ch.setFormatter(formatter)
logger.addHandler(ch)

base_url = "http://books.toscrape.com/catalogue/"
url_page = base_url + "page-{}.html"


@retry(tries=5, delay=2, backoff=2, logger=logger)
def url_to_beautiful_soup(url):
    logger.debug(f"Getting {url}")
    return BeautifulSoup(urlopen(url).read().decode("utf-8"), features="html.parser")


def get_book_info(n_page):
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


def save_pages_info():
    soup_principal = url_to_beautiful_soup(url="http://books.toscrape.com")

    books = []

    for n_page in range(
        1,
        int(
            soup_principal.find("li", {"class": "current"})
            .get_text()
            .replace("\n", "")
            .replace(" ", "")
            .split("of")[-1]
        )
        + 1,
    ):
        book_list = get_book_info(n_page)
        books.extend(book_list)
    return pd.DataFrame(books)


if __name__ == "__main__":
    df = save_pages_info()
    df.to_csv("books_2.csv", index=False)
