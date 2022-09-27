# craigslist_harvest
Selenium and BeautifulSoup to scrape Boston's Craiglist gig section and subsequent analysis about possible daily earnings in the gig economy.

# Setup Instructions

Code was written on XUbuntu 20.04 with python 3.9.5. Firefox Version 96.0.

1. Create virtual environment and install pip list into environment.
2. Download Firefox Webdriver. For the most recent on at the time of this writing
`wget https://github.com/mozilla/geckodriver/releases/download/v0.31.0/geckodriver-v0.31.0-linux64.tar.gz`
3. Unzip the contents of the file.
`tar -xvzf geckodriver-v0.31.0-linux64.tar.gz`
4. Change permissions on executable
`chmod +x geckodriver`
5. Add geckodriver/executable to your PATH
`export PATH=$PATH:/path/to/directory/of/executable/downloaded/in/previous/step`

