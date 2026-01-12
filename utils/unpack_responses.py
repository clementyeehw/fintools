from bs4 import BeautifulSoup
import datetime as dt
import pandas as pd
from playwright.sync_api import sync_playwright


def init_pw(url: str, timeout: int = 1000) -> BeautifulSoup:
    """Initialize Playwright and return page content."""
    p = sync_playwright().start()
    browser = p.chromium.launch(headless=False)
    context = browser.new_context()
    page = context.new_page()
    page.goto(url)
    page.wait_for_timeout(timeout)
    return BeautifulSoup(page.content(), "html.parser")

def preprocess(prices: pd.DataFrame) -> pd.DataFrame:
    """Format date timeseries to YYYYMMDD and reorder columns."""
    if "calcDate" not in prices.columns:
        prices["calcDate"] = dt.datetime.now().strftime("%Y%m%d")
    prices["calcDate"] = pd.to_datetime(prices["calcDate"]).dt.strftime("%Y%m%d")
    prices["date"] = pd.to_datetime(prices["date"]).dt.strftime("%Y%m%d")
    prices = prices.groupby([prices["date"].str[:4].astype(int), 
                             prices["date"].str[4:6].astype(int)]).tail(1)
    prices.loc[:, ["close"]] = prices.loc[:, "close"].astype(float)
    prices = prices.loc[prices["close"].notnull(), 
                        ["calcDate", "currency", "date", "close"]]
    return prices
