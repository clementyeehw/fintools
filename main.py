from data.indices.get_prices import Index
from data.stocks.get_tickers import Global

if __name__ == "__main__":
    prices = Index(name="Nikkei 225", variant="Price Return")
    #tickers = Global().set_tickers()