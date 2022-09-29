import pytest
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from scraper.clean.clean_web_extractions import clean_range_of_posts_on_page


def test_page_has_correct_title():
    driver = webdriver.Firefox()
    driver.get("https://boston.craigslist.org/")
    assert (
        driver.title
        == "craigslist: boston jobs, apartments, for sale, services, community, and events"
    )


def test_page_has_no_element():
    driver = webdriver.Firefox()
    driver.get("https://boston.craigslist.org/")
    try:
        element = driver.find_element(By.CLASS_NAME, "ggf")
    except NoSuchElementException:
        print("NoSuchElementException handled for CLASS_NAME ggf")
        element = None
        driver.close()
    assert element is None


@pytest.mark.parametrize(
    "test_input, expected",
    [("1,161 - 2,171 of 2,171", (1161, 2171, 2171)), ("1 - 120 of 439", (1, 120, 439))],
)
def test_clean_range_of_posts_on_page(test_input, expected):
    assert clean_range_of_posts_on_page(test_input) == expected
