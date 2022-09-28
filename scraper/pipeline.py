"""
This script defines the pipeline
and DAG for the functions in the harvest.py script.
"""
from pathlib import Path
import datetime as dt
import pandas as pd

import luigi
from luigi.util import requires

from harvest import (
    save_gig_links,
    extract_links_from_gigs_page,
    #extract_links_wrapper,
    extract_gig_post,)

class ScrapeCraigsListGigs(luigi.Task):
    """
    """

    date = luigi.DateParameter(default=dt.datetime.today())

    def requires(self):
        return None

    def output(self):

    # cl_gigs_page_dir.mkdir(exist_ok=True, parents=True)
        return luigi.LocalTarget(f'working_dir/gig_posts_{self.date}')

    def run(self):
        Path(self.output().path).mkdir(exist_ok=True, parents=True)
        save_gig_links(self.output().path)


@requires(ScrapeCraigsListGigs)
class ExtractLinksFromGigPages(luigi.Task):
    """
    """
    def output(self):
        return luigi.LocalTarget(f'working_dir/links_to_gig_postings.csv')

    def run(self):
        Path(self.output().path).parent.mkdir(exist_ok=True)
        cl_gigs_page_dir = self.input().path

        for num, file in enumerate(Path(cl_gigs_page_dir).glob("**/*.html")):
            if num==0:
                extract_links_from_gigs_page(
                    file,
                    True,
                    self.output().path)
            else:
                extract_links_from_gigs_page(
                    file,
                    False,
                    self.output().path)


class ScrapeGigPost(luigi.Task):
    """
    """
    dynamic_html_name = luigi.Parameter()
    dynamic_link = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(f'working_dir/gig_posts/{self.dynamic_html_name}')
    def run(self):
        gig_post_dir = Path(self.output().path).parent
        gig_post_dir.mkdir(exist_ok=True, parents=True)
        extract_gig_post(
            self.dynamic_html_name,
            self.dynamic_link,
            gig_post_dir)


@requires(ExtractLinksFromGigPages)
class ScrapeGigPosts(luigi.Task):
    def output(self):
        return luigi.LocalTarget('working_dir/gig_posts.marker')

    def run(self):
        gig_posting_links = pd.read_csv(self.input().path)
        gig_posting_links['date'] = pd.to_datetime(
            gig_posting_links['date'])
        links = gig_posting_links['link'].tolist()
        filenames = gig_posting_links['filename'].tolist()
        for filename, link in list(zip(filenames, links)):
            yield ScrapeGigPost(filename, link)

        with self.output().open('w') as fh:
            pass

class RunPipeLine(luigi.WrapperTask):
    def requires(self):
        yield ScrapeGigPosts()

if __name__ == "__main__":
    luigi.run()
