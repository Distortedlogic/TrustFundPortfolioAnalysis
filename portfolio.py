from datetime import datetime, timedelta

import ccxt
import pandas as pd
from IPython.display import display
from yahoo_fin import stock_info

from purchases import *

exchange = ccxt.binance()


def get_cost_basis_per_share():
    purchases_df = pd.read_csv("purchases_df.csv")
    grouped_by_ticker = purchases_df.groupby(['ticker_sym'])
    cost_basis_per_share = grouped_by_ticker[['qty', 'cost']].sum().reset_index()
    cost_basis_per_share["first_purchase"] = grouped_by_ticker['purchased_at'].min().reset_index()["purchased_at"]
    cost_basis_per_share["last_purchase"] = grouped_by_ticker['purchased_at'].max().reset_index()["purchased_at"]
    cost_basis_per_share["cost_per_share"] = cost_basis_per_share["cost"].divide(cost_basis_per_share["qty"])
    cost_basis_per_share["current_price"] = cost_basis_per_share["ticker_sym"].apply(lambda ticker: stock_info.get_data(
        ticker, end_date=datetime.utcnow().replace(hour=0, minute=0)).close[-1])
    cost_basis_per_share["pl_per_share"] = cost_basis_per_share["current_price"].subtract(cost_basis_per_share["cost_per_share"])
    cost_basis_per_share["current_value"] = cost_basis_per_share["current_price"].multiply(cost_basis_per_share["qty"])
    cost_basis_per_share["current_pl"] = cost_basis_per_share["current_value"].subtract(cost_basis_per_share["cost"])
    cost_basis_per_share["roi"] = cost_basis_per_share["current_value"].divide(cost_basis_per_share["cost"]).apply(lambda x: x-1)
    cost_basis_per_share = cost_basis_per_share[["ticker_sym",
                                                 "qty",
                                                 "cost",
                                                 "current_value",
                                                 "current_pl",
                                                 "roi",
                                                 "cost_per_share",
                                                 "current_price",
                                                 "pl_per_share",
                                                 "first_purchase",
                                                 "last_purchase"]]
    return cost_basis_per_share


def get_current_prices(tickers: list[str]):
    current_prices: dict[str, float] = {}
    eod_datetime = datetime.utcnow().replace(hour=0, minute=0)  # -timedelta(days=1)
    for ticker in tickers:
        if ticker == "BTC":
            price = get_btc_spot_price()
        else:
            price = stock_info.get_data(ticker, end_date=eod_datetime).close[-1]
        current_prices[ticker.lower()+"_qty"] = price
    return current_prices


def get_overview_df():
    cost_basis_per_share = get_cost_basis_per_share()

    total_cost = cost_basis_per_share["cost"].sum()
    current_value = cost_basis_per_share["current_price"].multiply(cost_basis_per_share["qty"]).sum()
    current_pl = current_value-total_cost
    roi = (current_value/total_cost)-1

    return pd.DataFrame.from_records([dict(total_cost=total_cost,
                                           current_value=current_value,
                                           current_pl=current_pl,
                                           roi=roi)])


def get_comparison_rois_df(comparison_tickers: list[str]):
    cost_basis_per_share = get_cost_basis_per_share()
    alt_purchases = pd.read_csv("alt_purchases.csv")
    etf_purchases = pd.read_csv("etf_purchases.csv")
    btc_purchases = pd.read_csv("btc_purchases.csv")
    all_purchases = [alt_purchases, etf_purchases, btc_purchases]

    total_cost = cost_basis_per_share["cost"].sum()
    current_prices = get_current_prices(comparison_tickers)
    # print(json.dumps(current_prices, indent=4))

    rois = {}
    for col, price in current_prices.items():
        value = None
        for df in all_purchases:
            if col in df.columns:
                value = price * df[col].sum()
                break
        if value is None:
            raise Exception(f"unknown ticker - {col}")
        rois[rename_suffix(col, "roi")] = (value/total_cost)-1
    return pd.DataFrame.from_records([rois])


def display_overview(comparison_tickers: list[str]):
    overview_df = get_overview_df()
    rois = get_comparison_rois_df(comparison_tickers)
    overall = pd.concat([overview_df, rois], axis=1)
    display(overall.style.format({
        "total_cost": "${:,.0f}",
        "current_value": "${:,.0f}"
    }).hide(axis='index').applymap(
        lambda value: 'color: '+('red;' if value < 0 else 'green;'), subset=["current_pl", "roi",  *rois.columns]
    ).format(
        lambda num: '{0:.2%}'.format(num) if num >= 0 else '({0:.2%})'.format(abs(num)), subset=["roi", *rois.columns]
    ).format(
        lambda num: '${0:,.0f}'.format(num) if num >= 0 else '(${0:,.0f})'.format(abs(num)), subset=["current_pl"]
    ))


def display_portfolio_breakdown():
    cost_basis_per_share = get_cost_basis_per_share()
    display(cost_basis_per_share.style.format({
        "qty": "{:,.0f}",
        "cost": "${:,.0f}",
        "current_value": "${:,.0f}",
        "cost_per_share": "${:.2f}",
        "current_price": "${:.2f}"
    }).hide(axis='index').applymap(
        lambda value: 'color: '+('red;' if value < 0 else 'green;'), subset=["current_pl", "roi", "pl_per_share"]
    ).format(
        lambda num: '${0:.2f}'.format(num) if num >= 0 else '(${0:,.2f})'.format(abs(num)), subset=["pl_per_share"]
    ).format(
        lambda num: '${0:.0f}'.format(num) if num >= 0 else '(${0:,.0f})'.format(abs(num)), subset=["current_pl"]
    ).format(
        lambda num: '{0:.2%}'.format(num) if num >= 0 else '({0:.2%})'.format(abs(num)), subset=["roi"]
    ))
