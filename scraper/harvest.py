import sys
import datetime as dt
import glob
from pathlib import Path
from collections import defaultdict

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from clean.clean_web_extractions import clean_range_of_posts_on_page

def save_gig_links()-> None:
    """
    Visits the boston craigslist, navigates to gig section, itterates
    through pages of gigs and saves the pages for later analysis.

    Args:

    Returns:
        None

    Raises:
        NoSuchElementException
    """
    link = "https://boston.craigslist.org"
    driver = webdriver.Firefox()
    driver.get(link)

    gigs_link = driver.find_element(By.CLASS_NAME, 'ggg')
    gigs_link.click()

    today = dt.datetime.today().strftime('%Y_%m_%d')
    on_last_page = False
    page_count = 0
    work_dir = Path('working_dir') / f'gig_pages_{today}'
    work_dir.mkdir(exist_ok=True, parents=True)
    while not on_last_page:
        try:
            gig_post_title = WebDriverWait(driver, 20).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, 'li.cl-search-result:nth-child(2) > a:nth-child(3)'))
            )
        except NoSuchElementException:
            print(f"{gig_post_title} post-titles not found!")

        page_content = driver.page_source
        soup = BeautifulSoup(page_content, 'html.parser')
        gig_posts = soup.find_all(
            "li", {"class": "cl-search-result cl-search-view-mode-list"})

        if gig_posts: # double check page is what we want
            with (
                work_dir /\
                f'gig_post_page_{page_count}.html').open('w') as fh:

                fh.write(page_content)

        else:
            print(f"{page_count} didn't have expected class for gig post!")

        try:
            next_button = WebDriverWait(driver, 20).until(
                EC.element_to_be_clickable((
                    By.CSS_SELECTOR,
                    'div.cl-search-paginator:nth-child(2) > button:nth-child(4)'))
            )
        except NoSuchElementException:
            print("No page element!")

        page_number = soup.find('span', {'class':"cl-page-number"})
        if page_number is not None:
            # example 2,161 - 2,171 of 2,171
            page_num_range = page_number.text
            begin, end, end_range = clean_range_of_posts_on_page(
                page_num_range)
        if next_button is not None and end!=end_range:
            next_button.click()
            page_count+=1
            continue
        else:
            on_last_page=True

def extract_links_from_gig_pages(path_to_gig_page_html):
    """
    """
    top_dir = Path(path_to_gig_page_html).parent.parent
    csv_output = top_dir / 'links_to_gig_postings.csv'
    with open(path_to_gig_page_html, 'r') as fh:
        html_content = fh.read()
    soup = BeautifulSoup(html_content, 'html.parser')
    gig_posts = soup.find_all(
        "li", {"class": "cl-search-result cl-search-view-mode-list"})
    #with csv_output.open('w') as fh:
    #    fh.write()

    for gig_post in gig_posts:
        #row = {"title":"", "link"}
        title = gig_post.find('span')

        a_el = gig_post.find('a')
        link = a_el['href']
        time_el = gig_post.find('time')
        datetime = time_el['datetime']
        print(title)
        print(a_el)
        print(link)
        print(time_el)
        print(datetime)

if __name__ == "__main__":
    today = dt.datetime.today().strftime('%Y_%m_%d')
    work_dir = Path('working_dir') / f'gig_pages_{today}'
    for file in work_dir.glob('**/*.html'):
        extract_links_from_gig_pages(file)
    #main(sys.argv[1])
