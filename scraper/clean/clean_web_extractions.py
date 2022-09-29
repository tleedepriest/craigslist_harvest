"""
module for cleaning text extracted from HTML (clean_name_of_function)
or extracting text from HTML (extract_name_of_function)
"""
import re
from bs4 import BeautifulSoup
from pathlib import Path
from typing import Tuple, List, Union
from datetime import datetime
from datetime import timezone


def clean_range_of_posts_on_page(range_of_posts_on_page: str) -> Tuple[int, int, int]:
    """
    Args:
        range_of_posts_on_page:
            string of format '1,161 - 2,171 of 2,171'
    Returns:
        A tuple representing the three numbers in original string.

    Raises:
        ValueError, TypeError
    """
    if not isinstance(range_of_posts_on_page, str):
        raise TypeError(f"{range_of_posts_on_page} needs to be type str!")

    should_contain = ["-", "of"]

    if not all(x in range_of_posts_on_page for x in should_contain):
        raise ValueError(f"{range_of_posts_on_page} needs to contain a '-' and 'of'!")

    range_of_posts_on_page = range_of_posts_on_page.replace(" ", "").replace(",", "")
    range_split = range_of_posts_on_page.split("-")
    begin_num_post_on_page = int(range_split[0])

    range_split_again = range_split[1].split("of")
    end_num_post_on_page = int(range_split_again[0])
    num_post = int(range_split_again[1])
    return begin_num_post_on_page, end_num_post_on_page, num_post


def clean_date_of_post(utc_datestring: str) -> str:
    """
    Args:
        utc_datestring:
            timestamp from craigslist post
            e.g 2022-09-27T15:11:18.000Z

    Returns:
        same timestamp but only for day.
        e.g. 2022-09-027

    Raises:
        TypeError, ValueError
    """
    if not isinstance(utc_datestring, str):
        raise TypeError(f"{utc_datestring} needs to be type str!")

    should_contain = ["-", "T", "Z", ".", ":"]

    if not all(x in utc_datestring for x in should_contain):
        raise ValueError(f"{utc_datestring} needs to contain a '-' and 'of'!")
    to_datetime = datetime.fromisoformat(utc_datestring[:-1]).astimezone(timezone.utc)
    return to_datetime.strftime("%Y-%m-%d")


def extract_gig_postingbody_from_html(filename: str) -> str:
    """
    extracts text section from HTML of gig post

    Args:
        filename:
            name of file in gig_posts
    Returns:
        the postingbody text of the craigslist post
    """
    full_path = Path("working_dir") / "gig_posts" / f"{filename}"
    with Path(full_path).open("rb") as fh:
        html_contents = fh.read()

    soup = BeautifulSoup(html_contents, "html.parser")
    section = soup.find("section", {"id": "postingbody"})
    if section is not None:
        return section.text
    return ""


def split_on_punc(string: str) -> List[str]:
    """
    split a sentence on three different types of punctuation.
    Args:
        string:
            input string to split
    Returns:
        split_on_punc:
    """
    split_on_punc = re.split(r"[\.!?]", string)
    return split_on_punc


def has_numbers(string: str) -> bool:
    """
    determines if any characters in string are digits

    Args:
        string:
            input string
    Returns:
        bool
            boolean indicator of digits
    """
    return any(char.isdigit() for char in string)


def remove_phone_number(string: str) -> str:
    """
    remove phone numbers

    Args:
        string:
            input string

    Returns:
        string without phone numbers
    """
    return re.sub(r"(\+0?1\s)?\(?\d{3}\)?[\s.-]\d{3}[\s.-]\d{4}", "", string)


def clean_gig_postingbody(gig_postingbody_txt_raw: str) -> str:
    """
    cleans text from gig_posting_body of newlines, extra spacing,
    and phone numbers. Splits on punctuation and filters sentences
    for the ones that contain digits. Joins list back on '.'

    Args:
        gig_postingbody_txt_raw:
            the postingbody text previously extracted from soup
    Returns:
        gig_postingbody_txt_clean:
            string with newlines and spacing removed.
    """
    gig_postingbody_txt_clean = re.sub(r"\s+", " ", gig_postingbody_txt_raw).replace(
        "QR Code Link to This Post", ""
    )
    sop = split_on_punc(gig_postingbody_txt_clean)
    sop = list(map(remove_phone_number, sop))
    sop = list(filter(has_numbers, sop))
    return ".".join(sop)


def extract_dollar_amount(string: str) -> Union[str, None]:
    """
    Extracts strictly only dollar amounts if a dollar rate
    is not present.

    Args:
        extracts dollar amounts
    Returns:
        dollar amounts or None if no match
    """

    def format(match: re.match) -> int:
        """
        removes everything but numbers from match.
        """
        match_string = match.group(0)
        match_string = [char for char in match_string if char.isdigit()]
        match_string = "".join(match_string)
        return int(float(match_string))

    # only want dollar amounts strictly, not per hour, so dont double cnt
    match = re.search(
        r"(?:[\$]{1}[,\d]+) ?\+?(\/|per) ?(hr|hour|day|dy|week)",
        string,
        flags=re.IGNORECASE,
    )

    if match is not None:
        return 0

    match = re.search(r"(?:[\$]{1}[,\d\.]+)", string)
    if match is not None:
        return format(match)

    match = re.search(r"([,\d\.]+?) dollars", string, flags=re.IGNORECASE)
    if match is not None:
        return format(match)

    return None


def extract_dollar_amount_per_time(string: str) -> Union[float, None]:
    """
    extracts dollar amounts per time. Converts the rate back
    to an hourly rate.
    """

    def format(match: re.match) -> str:
        """
        cleans/formats the match dollar amount per unit time
        """
        remove_chars = ["/", "per", "$", ",", "+", " "]
        match_string = match.group(0)
        for char in remove_chars:
            match_string = match_string.replace(char, "")
        return match_string.lower()

    match = re.search(
        r"(?:[\$]{1}[,\d]+) ?\+?(\/|per) ?(hr|hour|day|dy|week)",
        string,
        flags=re.IGNORECASE,
    )

    if match is not None:
        match_string = format(match)
        match_digits = re.search(r"\d+", match_string)
        formatted_match = float(int(match_digits.group(0)))
        if "h" in match_string:
            return formatted_match
        if "d" in match_string:
            return formatted_match / 8
        if "week" in match_string:
            return formatted_match / 40
        else:
            raise ValueError(f"{match_string} should contain 'h', 'd', or 'week'")
    return None


def extract_time_phrase(string: str) -> Union[float, None]:
    """
    Searches for substring indicating a period of time.
    Args:
        string:
            input string
    Returns:
        match_string
    """
    match = re.search(
        r"\d{1,2} ?-?(day?s|minute?s|min?s|hr?s|hour?s)", string, flags=re.IGNORECASE
    )
    if match is not None:
        match_string = match.group(0)
        match_digits = re.search(r"\d+", match_string)
        match_digits = float(int(match_digits.group(0)))
        if "d" in match_string.lower():
            return match_digits * 8
        if "m" in match_string.lower():
            return match_digits * (1 / 60)
        if "h" in match_string.lower():
            return match_digits
        else:
            raise ValueError(f"The string {match_digits} " f"should have 'd', 'm', or")
    return None
