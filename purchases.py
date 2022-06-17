import ftplib
import io
import json
from datetime import datetime, timedelta

import ccxt
import pandas as pd
import requests
from IPython.display import clear_output
from tqdm import tqdm
from yahoo_fin import stock_info

exchange = ccxt.binance()


def get_stock_close_price(ticker: str) -> float:
    try:
        end_date = datetime.utcnow()
        end_date = (end_date - timedelta(days=1)).replace(hour=0, minute=0)
        # if 9 < end_date.hour or end_date.hour < 17:
        #     end_date = end_date - timedelta(days=1)
        return stock_info.get_data(ticker, end_date=end_date).close[-1]
    except Exception as e:
        print("ticker", ticker)
        print("enddate", end_date)
        raise e


def get_sp500_tickers():
    resp = requests.get('http://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    sp500 = pd.read_html(resp._content)[0]
    sp500["Symbol"] = sp500["Symbol"].str.replace(".", "-", regex=True)
    return sp500.Symbol.tolist()


def get_dow_tickers():
    resp = requests.get('https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average')
    table = pd.read_html(resp._content, attrs={"id": "constituents"})[0]
    return sorted(table['Symbol'].tolist())


def get_nasdaq_tickers():
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


etfs = ["IWM", "SPY", "QQQ", "DIA", "GLD", "PDBC", "TLT"]


def extract_ticker(col_name: str, upper: bool = True):
    ticker = col_name.split("_")[0]
    return ticker.upper() if upper else ticker


def rename_suffix(col_name: str, suffix: str):
    return col_name.split("_")[0] + "_" + suffix


def get_all_stocks():
    sp_tickers = get_sp500_tickers()
    dow_tickers = get_dow_tickers()
    nasdaq_tickers = get_nasdaq_tickers()
    all_stocks: list[str] = sp_tickers + dow_tickers + nasdaq_tickers
    return all_stocks


def get_purchases_df():
    try:
        return pd.read_csv("purchases_df.csv")
    except:
        xls = pd.ExcelFile('summary.xls')
        sheets = pd.read_excel(xls, None)
        purchases_df = pd.DataFrame()
        for _, df in sheets.items():
            purchases_df = pd.concat([purchases_df,
                                     (df.dropna(subset=['Security ID', "Date Acquired"])
                                      .drop(["Primary Account Holder", "Security Description", "Recent MV", "Gain / Loss", "Currency Code"], axis=1))])
        purchases_df = purchases_df.rename(columns={"Security ID": "ticker_sym",
                                                    "Recent Qty": "qty",
                                                    "Date Acquired": "purchased_at",
                                                    "Cost per Share": "cost_per_share",
                                                    "Cost": "cost"})
        purchases_df.to_csv("purchases_df.csv", index=False)
        return purchases_df


def get_btc_spot_price() -> float:
    return exchange.fetch_ohlcv('BTC/USDT', '1m',  limit=1)[0][1]


def get_btc_prices(start_date: datetime):
    ohlvc = exchange.fetch_ohlcv('BTC/USDT', '1d', since=int(start_date.timestamp())*1000, limit=1500)
    ohlcv_df = pd.DataFrame.from_records(ohlvc, columns=["date", "open", "high", "low", "close", "volume"])
    ohlcv_df["index"] = ohlcv_df["date"].divide(1000).apply(datetime.fromtimestamp).apply(
        lambda date: date.strftime('%Y-%m-%d')).apply(lambda date: datetime.strptime(date, '%Y-%m-%d'))
    return ohlcv_df


def construct_alt_purchases(tickers: list[str], save_to: str):
    try:
        alt_purchases = pd.read_csv(save_to)
    except:
        alt_purchases = pd.DataFrame()

    stocks_to_do = [ticker for ticker in tickers if f"{ticker.lower()}_qty" not in alt_purchases.columns]

    def print_stats():
        clear_output(wait=True)
        print(f"fails: {fails}")
        print(f"{idx}/{len(stocks_to_do)}")
        uptime = datetime.now()-start_time
        print(f"uptime: {uptime}")
        print(f"avg time: {uptime/idx}")
        print(f"eta: {uptime/idx*len(stocks_to_do) - uptime}")
        print(f"timestamp: {datetime.now()}")
        print(f"exceptions:")
        print(exceptions)

    start_time = datetime.now()
    fails = 0

    purchases_df = get_purchases_df()
    purchase_dates = purchases_df["purchased_at"]
    start_date = datetime.strptime(purchase_dates.min(), '%Y-%m-%d')
    end_date = datetime.strptime(purchase_dates.max(), '%Y-%m-%d') + timedelta(days=1)
    exceptions: set[str] = set()

    def get_qtys(ticker: str):
        qtys = []
        if ticker == "BTC":
            stock_data_df = get_btc_prices(start_date)
        else:
            stock_data_df = stock_info.get_data(ticker, start_date=start_date, end_date=end_date).reset_index()
        for _, row in purchases_df.iterrows():
            close_price = stock_data_df[stock_data_df["index"] == datetime.strptime(row["purchased_at"], '%Y-%m-%d')]["close"].values[0]
            qtys.append(row["cost"]/close_price)
        return qtys

    for idx, ticker in enumerate(stocks_to_do, 1):
        try:
            qtys = get_qtys(ticker)
        except Exception as e:
            fails += 1
            exceptions.add(str(e))
            print_stats()
            print(e)
            continue
        print_stats()
        alt_purchases[f"{ticker.lower()}_qty"] = qtys
        alt_purchases.to_csv(save_to, index=False)
