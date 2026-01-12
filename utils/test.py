from bs4 import BeautifulSoup
import datetime as dt
import io
import numpy as np
import pandas as pd
from playwright.sync_api import sync_playwright
import re
import requests
import yfinance as yf



url = ""
p = sync_playwright().start()
browser = p.chromium.launch(headless=False)
context = browser.new_context()
page = context.new_page()
page.goto(url)
page.wait_for_timeout(1000)
soup = BeautifulSoup(page.content(), "html.parser")
url = soup.find("a", 
            {
                "class": "widget-download-list-item-link text-body website-link",
                "href": True
            }
        )["href"]
response = requests.get("https://links.sgx.com/1.0.0/isin/1/23%20Dec%202025")
rows = list(map(lambda x: re.split(r"\s{3,}", x), response.text.split("\r\n")))
rows = [row for row in rows if len(row) > 1]
rows = [row if len(row) == 5 else [row[0], "", row[1], row[2], row[3]] for row in rows]
df = pd.DataFrame(rows[1:], columns=rows[0])


tickers = [row["data-row-id"] + ".SI" for row in rows]

metadata = []
for ticker in tickers:
    meta = yf.Ticker(ticker).info
    if meta["quoteType"] == "EQUITY":
        metadata.append(
            [
                meta["symbol"],
                meta["longName"] if "longName" in meta.keys() else meta["prevName"],
                meta["sector"] if "sector" in meta.keys() else np.nan,
                meta["fullExchangeName"],
                meta["quoteType"],
                meta["currency"]
            ]
        )
tickers = pd.DataFrame(metadata,
                    columns=["ticker", "name", "sector",
                            "exchange", "type", "currency"]
                )