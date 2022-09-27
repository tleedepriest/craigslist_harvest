"""
module for cleaning text extracted from HTML
"""
from typing import Tuple

def clean_range_of_posts_on_page(
    range_of_posts_on_page: str) -> Tuple[int, int, int]:
    """
    Args:
        range_of_posts_on_page:
            string of format '1,161 - 2,171 of 2,171'
    Returns:
        A tuple representing the three numbers in original string.

    Raises:
        ValueError
    """
    if not isinstance(range_of_posts_on_page, str):
        raise TypeError(
            f"{range_of_posts_on_page} needs to be type str!"
        )

    if "-" not in range_of_posts_on_page or 'of' not in range_of_posts_on_page:
        raise ValueError(
            f"{range_of_posts_on_page} needs to contain a '-' and 'of'!"
        )

    range_of_posts_on_page = range_of_posts_on_page.replace(
        ' ', '').replace(',', '')
    range_split = range_of_posts_on_page.split('-')
    begin_num_post_on_page = int(range_split[0])

    range_split_again = range_split[1].split('of')
    end_num_post_on_page = int(range_split_again[0])
    num_post = int(range_split_again[1])
    return begin_num_post_on_page, end_num_post_on_page, num_post

