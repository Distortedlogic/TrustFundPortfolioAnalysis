import ftplib
import io
import json
from datetime import datetime, timedelta

import ccxt
import pandas as pd
import requests
from yahoo_fin import stock_info

exchange = ccxt.binance()


class TickerUtils:
    ETFS = ["IWM", "SPY", "QQQ", "DIA", "GLD", "PDBC", "TLT"]

    @classmethod
    def get_sp500_tickers(cls) -> list[str]:
        resp = requests.get('http://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
        sp500 = pd.read_html(resp._content)[0]
        sp500["Symbol"] = sp500["Symbol"].str.replace(".", "-", regex=True)
        return sp500.Symbol.tolist()

    @classmethod
    def get_dow_tickers(cls) -> list[str]:
        resp = requests.get('https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average')
        table = pd.read_html(resp._content, attrs={"id": "constituents"})[0]
        return sorted(table['Symbol'].tolist())

    @classmethod
    def get_nasdaq_tickers(cls) -> list[str]:
        ftp = ftplib.FTP("ftp.nasdaqtrader.com")
        ftp.login()
        ftp.cwd("SymbolDirectory")

        r = io.BytesIO()
        ftp.retrbinary('RETR nasdaqlisted.txt', r.write)

        info = r.getvalue().decode()
        splits = info.split("|")

        tickers = [x for x in splits if "\r\n" in x]
        tickers = [x.split("\r\n")[1] for x in tickers if "NASDAQ" not in x != "\r\n"]
        nasdaq_tickers = [ticker for ticker in tickers if "File" not in ticker]

        ftp.close()
        return nasdaq_tickers

    @classmethod
    def extract_ticker(cls, col_name: str, upper: bool = True):
        ticker = col_name.split("_")[0]
        return ticker.upper() if upper else ticker

    @classmethod
    def rename_suffix(cls, col_name: str, suffix: str):
        return col_name.split("_")[0] + "_" + suffix

    @classmethod
    def add_suffix(cls, ticker: str, suffix: str):
        return ticker.lower()+"_"+suffix

    @classmethod
    def get_all_stocks(cls):
        sp_tickers = cls.get_sp500_tickers()
        dow_tickers = cls.get_dow_tickers()
        nasdaq_tickers = cls.get_nasdaq_tickers()
        return sp_tickers + dow_tickers + nasdaq_tickers

    @classmethod
    def get_comparison_stocks(cls):
        return [cls.extract_ticker(col) for col in pd.read_csv("alt_purchases.csv").columns]

    @classmethod
    def get_crypto_spot_price(cls, ticker: str, base="USDT") -> float:
        # 'BTC/USDT'
        return exchange.fetch_ohlcv(f"{ticker}/{base}", '1m',  limit=1)[0][1]

    @classmethod
    def get_crypto_ohlcv(cls, pair: str, start_date: datetime):
        ohlvc = exchange.fetch_ohlcv(pair, '1d', since=int(start_date.timestamp())*1000, limit=1500)
        ohlcv_df = pd.DataFrame.from_records(ohlvc, columns=["date", "open", "high", "low", "close", "volume"])
        ohlcv_df["index"] = (ohlcv_df["date"]
                             .divide(1000)
                             .apply(datetime.fromtimestamp)
                             .apply(lambda date: date.strftime('%Y-%m-%d'))
                             .apply(lambda date: datetime.strptime(date, '%Y-%m-%d')))
        return ohlcv_df

    @classmethod
    def get_stock_spot_price(cls, ticker: str, end_date=datetime.utcnow() - timedelta(hours=1)) -> float:
        try:
            return stock_info.get_data(ticker, end_date=end_date).close[-1]
        except Exception as e:
            print("ticker", ticker)
            print("enddate", end_date)
            raise e

    @classmethod
    def get_stock_ohlcv(cls, ticker: str, start_date: datetime, end_date: datetime):
        return stock_info.get_data(ticker, start_date=start_date, end_date=end_date).reset_index()

    @classmethod
    def get_current_prices(cls, tickers: list[str], is_crypto: bool, end_date=datetime.utcnow() - timedelta(hours=1)):
        get_price = cls.get_crypto_spot_price if is_crypto else cls.get_stock_spot_price
        try:
            with open("current_prices.json", "r") as f:
                current_prices = json.load(f)
        except Exception:
            current_prices: dict[str, float] = {}
        tickers_to_do = [ticker for ticker in tickers if ticker not in current_prices]
        runs = 0
        while tickers_to_do:
            if runs > 10:
                raise Exception("too many current prices runs")
            for ticker in tickers_to_do:
                try:
                    current_prices[ticker] = get_price(ticker)
                    with open("current_prices.json", "w") as f:
                        json.dump(current_prices, f)
                except:
                    pass
            tickers_to_do = [ticker for ticker in tickers if ticker not in current_prices]
            runs += 1
        return current_prices
