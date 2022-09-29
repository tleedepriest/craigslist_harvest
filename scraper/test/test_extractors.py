import pytest
from scraper.clean.clean_web_extractions import (
    split_on_punc,
    has_numbers,
    remove_phone_number,
    extract_dollar_amount,
    extract_dollar_amount_per_time,
)


@pytest.mark.parametrize(
    "test_input, expected",
    [("hello there my number is 757-839-9828", "hello there my number is ")],
)
def test_remove_phone_numbers(test_input, expected):
    assert remove_phone_number(test_input) == expected


@pytest.mark.parametrize(
    "test_input, expected",
    [("Hello? My name is Travis!", ["Hello", " My name is Travis", ""])],
)
def test_split_on_punc(test_input, expected):
    assert split_on_punc(test_input) == expected


@pytest.mark.parametrize(
    "test_input, expected",
    [("Hello? My name is Travis!4", True), ("Hells fjkdla", False)],
)
def test_split_on_punc(test_input, expected):
    assert has_numbers(test_input) == expected


@pytest.mark.parametrize(
    "test_input, expected",
    [("I will 23 give you 1,500 dollars", 1500), ("I will 23 lend you $1500", 1500)],
)
def test_extract_dollar_amount(test_input, expected):
    assert extract_dollar_amount(test_input) == expected


@pytest.mark.parametrize(
    "test_input, expected",
    [("58 I will give you $23/hr", 23), ("I will 43 lend you $23/hour", 23)],
)
def test_extract_dollar_amount_per_time(test_input, expected):
    assert extract_dollar_amount_per_time(test_input) == expected
