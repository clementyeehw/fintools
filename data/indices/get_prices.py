import datetime as dt
import io
from utils.unpack_responses import *
import os
import pandas as pd
import re
import requests
import yfinance as yf


class Index:
    def __init__(self, name: str, variant: str):
        self.config_path = os.path.dirname(os.path.dirname(__file__))
        self.meta = pd.read_excel(self.config_path + r"\data_sources.xlsx", sheet_name="index")
        self.df = self.hist_eom(name, variant)
        self.df.to_csv(self.config_path + "\\indices\\datasets\\" + \
                       f"{re.sub('-', '', self.df['calcDate'].iloc[0])}_{name}.csv", index=False)    
    
    def hist_eom(self, name: str, variant: str) -> pd.DataFrame:
        """Get historical end-of-month prices for a particular index and its variant.
        Args:
            name (str): Security short name, e.g., ACWI.
            variant (str): Security variant (e.g., Price Return, Total Return).
        Returns:
            DataFrame containing historical prices with 'date' and 'price' columns.
        """
        provider, url = self.meta.loc[
            (self.meta["name"] == name)
            & (self.meta["variant_name"] == variant.title()),
            ["provider", "api"],
        ].values.tolist()[0]
        if provider == "CBOE":
            return self.cboe(url)
        elif provider == "MSCI":
            return self.msci(url)
        elif provider == "S&P":
            return self.sp(url)
        elif provider == "Yahoo":
            return self.yahoo(url)
        else:
            raise NotImplementedError("Index price retrieval is not implemented yet.")

    def cboe(self, url: str) -> pd.DataFrame:
        """Unpack CBOE request into a DataFrame."""
        response = requests.get(url)
        prices = pd.read_csv(io.StringIO(response.text), parse_dates=["DATE"])
        prices = prices[["DATE", "CLOSE"]].rename(columns={"DATE": "date", "CLOSE": "close"})
        prices["calcDate"] = prices["date"].iloc[-1]
        prices["currency"] = "USD"
        return preprocess(prices)
    
    def msci(self, url: str) -> pd.DataFrame:
        """Unpack the MSCI response into a DataFrame."""
        meta_data = {}
        response = requests.get(url)
        data = response.json()["data"]
        for key, value in data.items():
            if key != "indexes":
                meta_data[key] = value
            else:
                prices = pd.DataFrame(data["indexes"][0]["performanceHistory"])
                columns = list(prices.columns)
        for key, value in meta_data.items():
            prices[key] = value
        prices = prices[list(meta_data.keys()) + columns]
        prices.rename(columns={"value": "close"}, inplace=True)
        return preprocess(prices)

    def sp(self, url: str) -> pd.DataFrame:
        """Unpack the S&P response into a DataFrame."""
        data = init_pw(url)
        currency = data["indexDetailHolder"]["indexDetail"]["currencyCode"]
        prices = pd.DataFrame(data["indexLevelsHolder"]["indexLevels"])
        prices["calcDate"] = pd.to_datetime(prices["fetchedDate"], unit="ms")
        prices["currency"] = currency
        prices.rename(columns={"formattedEffectiveDate": "date", 
                               "indexValue": "close"}, inplace=True)
        return preprocess(prices)

    def yahoo(self, url: str) -> pd.DataFrame:
        """Unpack the Yahoo response into a DataFrame."""
        currency = yf.Ticker(url).info["currency"]
        prices = yf.download(url, start="2000-01-01", interval="1mo")
        prices.reset_index(inplace=True)
        prices = prices.droplevel(level=1, axis=1)
        prices["currency"] = currency
        prices.rename(columns={"Date": "date", 
                    "Close": "close"}, inplace=True)
        prices.set_index("date", inplace=True)
        if dt.datetime.now().month != (dt.datetime.now() + dt.timedelta(days=1)).month:
            prices = prices.resample("ME").last()
        else:
            prices = prices.iloc[:-1].resample("ME").last()
        prices.reset_index(inplace=True)
        return preprocess(prices)
    