from datetime import datetime, timedelta

import pandas as pd
from IPython.display import clear_output

from ticker_utils import TickerUtils


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


def construct_comparison_purchases(tickers: list[str], save_to: str, is_crypto: bool):
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

    def get_qtys(ticker: str, is_crypto: bool):
        qtys = []
        if is_crypto:
            stock_data_df = TickerUtils.get_crypto_ohlcv('BTC/USDT', start_date)
        else:
            stock_data_df = TickerUtils.get_stock_ohlcv(ticker, start_date=start_date, end_date=end_date)
        for _, row in purchases_df.iterrows():
            close_price = stock_data_df[stock_data_df["index"] == datetime.strptime(row["purchased_at"], '%Y-%m-%d')]["close"].values[0]
            qtys.append(row["cost"]/close_price)
        return qtys

    for idx, ticker in enumerate(stocks_to_do, 1):
        try:
            qtys = get_qtys(ticker, is_crypto)
        except Exception as e:
            fails += 1
            exceptions.add(str(e))
            print_stats()
            print(e)
            continue
        print_stats()
        alt_purchases[f"{ticker.lower()}_qty"] = qtys
        alt_purchases.to_csv(save_to, index=False)
