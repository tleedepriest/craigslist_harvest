import sys
import csv
import glob
import random
import requests
import datetime as dt
from typing import Union
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
    clean_gig_posting,)


def save_gig_links(cl_gigs_page_dir: str) -> None:
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
    link = "https://boston.craigslist.org"
    driver = webdriver.Firefox()
    driver.get(link)

    gigs_link = driver.find_element(By.CLASS_NAME, "ggg")
    gigs_link.click()

    today = dt.datetime.today().strftime("%Y_%m_%d")
    on_last_page = False
    page_count = 0
    # cl_gigs_page_dir = Path("working_dir") / f"gig_pages_{today}"
    # cl_gigs_page_dir.mkdir(exist_ok=True, parents=True)
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
            with (Path(cl_gigs_page_dir)
                  / f"gig_post_page_{page_count}.html").open("w") as fh:

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


def extract_links_from_gigs_page(path_to_gig_page_html: str,
                                 header: bool,
                                 csv_output: str)-> None:
    """
    Appends to csv file the links from a gigs page. If csv does
    not exist, will create it.

    Args:
        path_to_gig_page_html:
            The path to an html file page from the gigs section
            containing links to the individual posts

        header:
            determines whether or not to write the header of the file

        csv_output:
            The path to the csv file.
    Returns:
        None

    Raises:
    """
    with open(path_to_gig_page_html, "r") as fh:
        html_content = fh.read()
    soup = BeautifulSoup(html_content, "html.parser")
    gig_posts = soup.find_all(
        "li", {"class": "cl-search-result cl-search-view-mode-list"}
    )
    with Path(csv_output).open('a+', newline='') as fh:
        fieldnames = ['title', 'link', 'filename',
                      'date', 'archive_source_path']
        csv_writer = csv.DictWriter(fh, fieldnames=fieldnames)
        if header:
            csv_writer.writeheader()
        for gig_post in gig_posts:
            row = defaultdict()
            a_el = gig_post.find("a")
            title = a_el.find('span').text
            link = a_el["href"]
            time_el = gig_post.find("time")
            datetime = time_el["datetime"]
            row['title'] = title
            row['link'] = link
            row['filename'] = link.split('/')[-1]
            row['date'] = clean_date_of_post(datetime)
            row['archive_source_path'] = path_to_gig_page_html
            csv_writer.writerow(row)


def extract_gig_post(filename: str,
                     link: str,
                     gig_posts_folder: Path)-> None:
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
    try:
        response = requests.get(link)
        if response.status_code == 200:
            with (gig_posts_folder / filename).open('wb') as fh:
                fh.write(response.content)
        else:
            print(f"Response Status Code: {response.status_code}")
            return

    except requests.Timeout as e:
        print(str(e))
    except:
        raise # catch-all


def analyze_gig_posts():
    """
    """
    gig_postings = pd.read_csv('working_dir/links_to_gig_postings.csv')
    gig_postings_drop_dupes = gig_postings.copy().drop_duplicates(subset=['title', 'date'])
    gig_postings_drop_dupes['gig_posting_text'] = gig_postings_drop_dupes['filename'].apply(lambda x: clean_gig_posting(x))
    print(gig_postings_drop_dupes)
    gig_postings_drop_dupes.to_csv('final.csv')

if __name__ == "__main__":
    analyze_gig_posts()
