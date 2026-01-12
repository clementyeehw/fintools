from utils.unpack_responses import *
import numpy as np
import os
import pandas as pd
import re
import requests
import yfinance as yf


class Global:
    def __init__(self):
        self.config_path = os.path.dirname(os.path.dirname(__file__))
        self.consolidate()
    
    def consolidate(self):
        """Write dataframes to Excel spreadsheets."""
        indices = pd.read_excel(self.config_path + r"\data_sources.xlsx", sheet_name="index")
        dfs = {"index": indices, "stock": self.sgx()}
        with pd.ExcelWriter(self.config_path + r"\data_sources.xlsx", engine="openpyxl") as writer:
            for name, df in dfs.items():
                df.to_excel(writer, sheet_name=name, index=False) 
    
    def sgx(self) -> pd.DataFrame:
        """Return SGX stock tickers and metadata."""
        response = requests.get("https://api.sgx.com/securities/v1.1")
        data = response.json()["data"]["prices"]
        df = pd.DataFrame(data)
        df = df.loc[(df["type"].isin(["stocks", "businesstrusts", "reits", "adrs"])) &
                    (df["i"] == ""), 
                    ["nc", "issuer-name", "m"]]
        df = df.rename(columns={
            "nc": "ticker",
            "issuer-name": "issuer_name",
            "m": "exchange",
        })
        lst = df.values.tolist()
        metadata = []
        for ticker, name, exchange in lst:
            meta = yf.Ticker(ticker + ".SI").info
            if "quoteType" in meta.keys() and meta["quoteType"] == "EQUITY":
                metadata.append([
                    meta["symbol"],
                    meta["longName"] if "longName" in meta.keys() else name,
                    meta["sector"] if "sector" in meta.keys() else np.nan,
                    meta["fullExchangeName"] + " " + exchange.title(),
                    meta["quoteType"],
                    meta["currency"]]
                )
        tickers = pd.DataFrame(
                        metadata,
                        columns=["ticker", "name", "sector",
                                "exchange", "type", "currency"]
                    )
        return tickers

    def set_tickers(self) -> BeautifulSoup:
        """Return stock tickers and metadata."""
        return print(init_pw("https://www.set.or.th/api/set/stock/INET/profile?lang=en"))
