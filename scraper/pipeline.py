"""
This script defines the pipeline
and DAG for the functions in the harvest.py.
"""
import csv
from pathlib import Path
import datetime as dt
import pandas as pd

import luigi
from luigi.util import requires

from harvest import (
    save_gig_links,
    extract_links_from_gigs_page,
    extract_gig_post,
)

from analyze import transform_gig_posts, answer_question


class ScrapeCraigsListGigs(luigi.Task):
    """
    See save_gig_links doc string.

    Args:
        date:
            Allows creation of new folder daily. Allows
            task to rerun on daily basis.
    """

    date = luigi.DateParameter(default=dt.datetime.today())

    def requires(self):
        return None

    def output(self):
        return luigi.LocalTarget(f"working_dir/gig_pages_{self.date}")

    def run(self):
        Path(self.output().path).mkdir(exist_ok=True, parents=True)
        save_gig_links(self.output().path)


@requires(ScrapeCraigsListGigs)
class ExtractLinksFromGigPages(luigi.Task):
    """
    see extract_links_from_gigs_page docstring. Extracts links
    from all local copies of gig pages from input of ScrapeCraigListGigs
    and appends to csv.

    Args:
        date:
            inherits from ScrapeCraigsListGigs. appended to output
            filename and allows task to rerun on daily basis.
    """

    def output(self):
        return luigi.LocalTarget(f"working_dir/links_to_gig_postings_{self.date}.csv")

    def run(self):
        output = Path(self.output().path)
        output.parent.mkdir(exist_ok=True)
        cl_gigs_page_dir = Path(self.input().path)

        with output.open('w', newline='') as fh:
            fieldnames = ['title', 'link', 'filename',
                          'date', 'archive_source_path']
            csv_writer = csv.DictWriter(fh, fieldnames=fieldnames)
            csv_writer.writeheader()
            for gig_page in cl_gigs_page_dir.glob("**/*.html"):
                gig_posts = extract_links_from_gigs_page(gig_page)

                for gig_post in gig_posts:
                    csv_writer.writerow(gig_post)

class ScrapeGigPost(luigi.Task):
    """ """

    dynamic_html_name = luigi.Parameter()
    dynamic_link = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(f"working_dir/gig_posts/{self.dynamic_html_name}")

    def run(self):
        gig_post_dir = Path(self.output().path).parent
        gig_post_dir.mkdir(exist_ok=True, parents=True)
        extract_gig_post(self.dynamic_html_name, self.dynamic_link, gig_post_dir)


@requires(ExtractLinksFromGigPages)
class ScrapeGigPosts(luigi.Task):
    def output(self):
        return luigi.LocalTarget(f"working_dir/gig_posts_{self.date}.marker")

    def run(self):
        gig_posting_links = pd.read_csv(self.input().path)
        gig_posting_links["date"] = pd.to_datetime(gig_posting_links["date"])
        links = gig_posting_links["link"].tolist()
        filenames = gig_posting_links["filename"].tolist()
        for filename, link in list(zip(filenames, links)):
            yield ScrapeGigPost(filename, link)

        with self.output().open("w") as fh:
            pass

@requires(ExtractLinksFromGigPages, ScrapeGigPosts)
class TransformGigPosts(luigi.Task):
    """
    see transform_gig_posts. Prepares gig posts for analysis
    """
    def output(self):
        return luigi.LocalTarget(f"working_dir/transformed_gig_posts_{self.date}.csv")
    def run(self):
        gig_posting_links = pd.read_csv(self.input()[0].path)
        gig_posting_transformed = transform_gig_posts(gig_posting_links)
        gig_posting_transformed.to_csv(self.output().path)

@requires(TransformGigPosts)
class AnswerQuestion(luigi.Task):
    """
    Answers the final question of how much money you could make in
    a day from doing all the Boston Gigs
    """
    def output(self):
        return luigi.LocalTarget("final_answer.txt")

    def run(self):
        transformed_gig_posts = pd.read_csv(self.input().path)
        answer = answer_question(transformed_gig_posts)
        with open('final.txt', 'w') as fh:
            sum_one_total = 0
            sum_two_total = 0
            for date, sum_list in answer.items():
                sum_one_total+=sum_list[0]
                sum_two_total+=sum_list[1]
                fh.write(
                    f"Date: {date}, est_one: {int(sum_list[0])}, est_two: {int(sum_list[1])}, number_of_gigs: {sum_list[2]}\n")

            fh.write(f"final answer {int(sum_one_total/len(answer))}\n")
            fh.write(f"final answer {int(sum_two_total/len(answer))}\n")



class RunPipeLine(luigi.WrapperTask):
    """
    Runs the entire pipeline
    """
    def requires(self):
        yield ScrapeGigPosts()
        yield TransformGigPosts()
        yield AnswerQuestion()

if __name__ == "__main__":
    luigi.run()
