"""
functions to analyze text of individual gig posts.
"""
from typing import Dict, List
import pandas as pd

from clean.clean_web_extractions import (
    extract_gig_postingbody_from_html,
    clean_gig_postingbody,
    extract_dollar_amount,
    extract_dollar_amount_per_time,
    extract_time_phrase)

def transform_gig_posts(gig_links_df: pd.DataFrame)-> pd.DataFrame:
    """
    Args:
        gig_links_df: pd.DataFrame()
            dataframe containing gig links and html filenames

    Returns:
        gp_dedupe: pd.DataFrame()
            dataframe containing extractions from gig posts

    Raises:
    """
    # don't want to double count on a day.
    gp_dedupe = gig_links_df.copy().drop_duplicates(
        subset=['title', 'date'])

    gp_dedupe['gig_posting_text'] = gp_dedupe['filename'].apply(
        extract_gig_postingbody_from_html)
    gp_dedupe['gig_posting_text_clean'] = gp_dedupe['gig_posting_text'].apply(clean_gig_postingbody)

    gp_dedupe['dollar_amount'] = gp_dedupe['gig_posting_text_clean'].apply(extract_dollar_amount)
    gp_dedupe['dollars_per_hour'] = gp_dedupe['gig_posting_text_clean'].apply(extract_dollar_amount_per_time)
    gp_dedupe['hours'] = gp_dedupe['gig_posting_text_clean'].apply(extract_time_phrase)
    return gp_dedupe

def answer_question(transformed_gigs_posts:pd.DataFrame)-> Dict[str, List[int, int, int]]:
    """
    Args:
        transformed_gigs_posts: pd.DataFrame()
            dataframe containing gig links and html filenames

    Returns:
        answer:
            answer dict where dates are keys and List is ints.
            ints are estimate one, estime two, number of gigs

    Raises:
    """
    all_dollars_one = 0
    all_dollars_two = 0
    cols_to_analyze = ["date", "dollar_amount", "dollars_per_hour", "hours"]
    dates = list(set(transformed_gigs_posts["date"].tolist()))

    transformed_gigs_posts = transformed_gigs_posts[transformed_gigs_posts["dollar_amount"] < 1000]
    answer = {}
    for date in dates:
        data_for_date = transformed_gigs_posts.copy()[transformed_gigs_posts["date"] == date]
        #data_for_date = data_for_date[cols_to_analyze].dropna(how="all")
        # drop some outliers skewing the data.
        data_for_date["dollar_amount"] = data_for_date["dollar_amount"].fillna(0)
        data_for_date["dollars_per_hour"] = data_for_date["dollars_per_hour"].fillna(0)
        data_for_date["hours"] = data_for_date["hours"].fillna(8)
        data_for_date["dollars_per_day_one"] = data_for_date["dollar_amount"]/data_for_date["hours"]*8
        data_for_date["dollars_per_day_two"] = data_for_date["dollars_per_hour"]*data_for_date["hours"]
        sum_of_dollars_one = data_for_date["dollars_per_day_one"].sum()
        sum_of_dollars_two = data_for_date["dollars_per_day_two"].sum()
        final_sum = sum_of_dollars_one + sum_of_dollars_two
        # if dollar amount equals dollars your double count....
        # so need to drop one or the other for that case
        answer[date] = [sum_of_dollars_one, sum_of_dollars_two, len(data_for_date)]
    return answer
