import sys
import glob
import random
import requests
import time
import random
import datetime as dt
from typing import Union, List, Dict
from pathlib import Path
from collections import defaultdict
import pandas as pd


from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from clean.clean_web_extractions import (
    clean_range_of_posts_on_page,
    clean_date_of_post,
    extract_gig_postingbody_from_html,
    clean_gig_postingbody,
    extract_dollar_amount,
    extract_dollar_amount_per_time,
    extract_time_phrase,
)

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:105.0) Gecko/20100101 Firefox/105.0'
}

def save_gig_links(cl_gigs_page_dir: str, city: str="boston") -> None:
    """
    Visits Boston CraigsList, navigates to gigs section, itterates
    through gig pages and saves each gig page in cl_gigs_page_dir.

    Args:
        cl_gigs_page_dir:
            directory to save each page from gigs section

    Returns:
        None

    Raises:
        NoSuchElementException
    """
    link = f"https://{city}.craigslist.org"
    driver = webdriver.Firefox()
    driver.get(link)

    gigs_link = driver.find_element(By.CLASS_NAME, "ggg")
    gigs_link.click()

    today = dt.datetime.today().strftime("%Y_%m_%d")
    on_last_page = False
    page_count = 0
    # cl_gigs_page_dir = Path("working_dir") / f"gig_pages_{today}"
    # cl_gigs_page_dir.mkdir(exist_ok=True, parents=True)
    try:
        while not on_last_page:
            # TODO: Handle timeout
            try:
                gig_post_title = WebDriverWait(driver, 20).until(
                    EC.element_to_be_clickable(
                        (
                            By.CSS_SELECTOR,
                            "li.cl-search-result:nth-child(2) > a:nth-child(3)",
                        )
                    )
                )
            except NoSuchElementException:
                print(f"{gig_post_title} post-titles not found!")

            page_content = driver.page_source
            soup = BeautifulSoup(page_content, "html.parser")
            gig_posts = soup.find_all(
                "li", {"class": "cl-search-result cl-search-view-mode-list"}
            )

            if gig_posts:  # double check page is what we want
                with (Path(cl_gigs_page_dir) / f"gig_post_page_{page_count}.html").open(
                    "w"
                ) as fh:

                    fh.write(page_content)

            else:
                print(f"{page_count} didn't have expected class for gig post!")

            try:
                next_button = WebDriverWait(driver, 20).until(
                    EC.element_to_be_clickable(
                        (
                            By.CSS_SELECTOR,
                            "div.cl-search-paginator:nth-child(2) > button:nth-child(4)",
                        )
                    )
                )
            except NoSuchElementException:
                print("No page element!")

            page_number = soup.find("span", {"class": "cl-page-number"})
            if page_number is not None:
                # example 2,161 - 2,171 of 2,171
                page_num_range = page_number.text
                begin, end, end_range = clean_range_of_posts_on_page(page_num_range)
            if next_button is not None and end != end_range:
                next_button.click()
                page_count += 1
                continue
            else:
                on_last_page = True
    except Exception as e1:
        print(e1)

    finally:
        print(f"killing current browsing session.")
        driver.quit()

def extract_links_from_gigs_page(path_to_gig_page_html: str) -> List[Dict[str, str]]:
    """
    Appends to csv file the links from a gigs page. If csv does
    not exist, will create it.

    Args:
        path_to_gig_page_html:
            The path to an html file page from the gigs section
            containing links to the individual posts

    Returns:
        all_gig_posts_for_gig_page:
            A list of dictionaries representing the extracted links
            and metadata of all gig posts for a given gig page.

    Raises:
        None
    """
    with open(path_to_gig_page_html, "r") as fh:
        html_content = fh.read()
    soup = BeautifulSoup(html_content, "html.parser")
    gig_posts = soup.find_all(
        "li", {"class": "cl-search-result cl-search-view-mode-list"}
    )
    all_gig_posts_for_gig_page = []
    for gig_post in gig_posts:
        row = defaultdict()
        a_el = gig_post.find("a")

        if a_el is not None:
            link = a_el["href"]
            title = a_el.find("span")
        else:
            title = None

        if title is not None:
            title = title.text

        time_el = gig_post.find("time")
        if time_el is not None:
            datetime = time_el["datetime"]
            datetime = clean_date_of_post(datetime)

        row["title"] = title
        row["link"] = link
        row["filename"] = link.split("/")[-1]  # convienent for later
        row["date"] = datetime
        row["archive_source_path"] = path_to_gig_page_html

        all_gig_posts_for_gig_page.append(row)

    return all_gig_posts_for_gig_page


def extract_gig_post(filename: str, link: str, gig_posts_folder: Path) -> None:
    """
    This downloads an individual gig post from a link into the
    gig_posts_folder

    Args:
        filename:
            the name of the individual HTML file, derived from link.
        link:
            the link to the Craiglist Gig Posting.
        gig_posts_folder:
            the folder to save the individual file.

    Returns:
        None

    Raises:
        requests.Timeout, all other requests exceptions.
    """
    sleep_time = random.randrange(4, 10)
    time.sleep(sleep_time)

    try:
        response = requests.get(link, headers=HEADERS)
        if response.status_code == 200:
            with (gig_posts_folder / filename).open("wb") as fh:
                fh.write(response.content)
        else:
            print(f"Response Status Code: {response.status_code}")
            return

    except requests.Timeout as e:
        print(str(e))
    except:
        raise  # catch-all


if __name__ == "__main__":
    pass
