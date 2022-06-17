import random
from functools import partial
from multiprocessing import Pool, cpu_count
from typing import Optional

import pandas as pd
from numpy import isin

from purchases import *
from utils import display_df

purchases_df = pd.read_csv("purchases_df.csv")
total_cost: float = purchases_df["cost"].sum()
alt_purchases = pd.read_csv("alt_purchases.csv")
# alt_purchases = alt_purchases[[col for col in alt_purchases.columns if col != "Unnamed: 0"]]
# alt_purchases.to_csv("alt_purchases.csv", index=False)
# alt_purchases = pd.read_csv("alt_purchases.csv")


advisors_stocks: list[str] = list(set(purchases_df["ticker_sym"]))
assert "Unnamed: 0" not in advisors_stocks
comparison_stocks: list[str] = list(alt_purchases.columns)
assert "Unnamed: 0" not in comparison_stocks
# alt_purchases = alt_purchases.loc[:, list(set(alt_purchases.columns))]
# alt_purchases.to_csv("alt_purchases.csv")


def get_advisors_stocks_to_random_stocks():
    random_stocks = random.sample(comparison_stocks, k=len(advisors_stocks))
    assert "Unnamed: 0" not in random_stocks
    assert len(random_stocks) == len(advisors_stocks)
    return dict(zip(advisors_stocks, random_stocks))


def get_current_roi() -> float:
    grouped_by_ticker = purchases_df.groupby(['ticker_sym'])
    cost_basis_per_share = grouped_by_ticker[['cost', 'qty']].sum().reset_index()
    cost_basis_per_share["current_price"] = cost_basis_per_share["ticker_sym"].apply(get_stock_close_price)
    cost_basis_per_share["current_value"] = cost_basis_per_share["current_price"].multiply(cost_basis_per_share["qty"])
    current_roi = cost_basis_per_share["current_value"].sum()/cost_basis_per_share["cost"].sum() - 1
    return current_roi


def run_simulation(current_prices: dict[str, float] = {}) -> float:
    def get_current_price(ticker: str):
        if ticker in current_prices:
            return current_prices[ticker]
        else:
            price = get_stock_close_price(ticker)
            current_prices[ticker] = price
            return price
    advisors_stocks_to_random_stocks = get_advisors_stocks_to_random_stocks()
    monte_carlo_df = pd.concat([purchases_df, alt_purchases[advisors_stocks_to_random_stocks.values()]], axis=1)
    grouped_by_ticker = monte_carlo_df.groupby(['ticker_sym'])
    cost_basis_per_share = grouped_by_ticker[['cost', *advisors_stocks_to_random_stocks.values()]].sum().reset_index()

    def get_comparison_qty(ticker: str):
        mask = cost_basis_per_share[cost_basis_per_share["ticker_sym"] == ticker]
        assert len(mask) == 1
        comparison_stock = advisors_stocks_to_random_stocks[ticker]
        assert isinstance(comparison_stock, str)
        values = mask[comparison_stock].values
        assert len(values) == 1
        value = values[0]
        try:
            assert isinstance(value, float)
            return value
        except Exception as e:
            print("advisors_stocks_to_random_stocks", advisors_stocks_to_random_stocks)
            print("mask")
            display_df(mask)
            print("comparison_stock", comparison_stock)
            print("mask[comparison_stock]")
            display_df(mask[comparison_stock])
            raise e
    cost_basis_per_share["comparison_qty"] = cost_basis_per_share["ticker_sym"].apply(get_comparison_qty)
    cost_basis_per_share["comparison_ticker"] = cost_basis_per_share["ticker_sym"].apply(
        lambda ticker: extract_ticker(advisors_stocks_to_random_stocks[ticker]))
    cost_basis_per_share["comparison_current_price"] = cost_basis_per_share["comparison_ticker"].apply(get_current_price)
    cost_basis_per_share["comparison_current_value"] = cost_basis_per_share["comparison_current_price"].multiply(cost_basis_per_share["comparison_qty"])
    comparison_roi = cost_basis_per_share["comparison_current_value"].sum()/total_cost - 1
    try:
        assert isinstance(comparison_roi, float)
        return comparison_roi
    except Exception as e:
        print("type(comparison_roi)", type(comparison_roi))
        print("comparison_roi", comparison_roi)
        display_df(cost_basis_per_share[['ticker_sym', 'cost', "comparison_qty", "comparison_ticker", "comparison_current_price", "comparison_current_value"]])
        raise e


def run_simulations(num: int):
    try:
        sims = pd.read_csv("monte_carlo_sims.csv").squeeze("columns")
        print("loaded sims")
    except:
        sims = pd.Series(dtype=float)
        print("new sims")
    try:
        with open("current_prices.json", "r") as f:
            current_prices = json.load(f)
        print("loaded prices")
    except Exception as e:
        current_prices = {}
        with open("current_prices.json", "w") as f:
            json.dump(current_prices, f)
        print("new prices")
    original_length = len(current_prices)
    while len(sims) < num:
        sims = [run_simulation(current_prices) for _ in range(num)]
        assert isinstance(sims[0], float)
        sims = pd.concat([sims, pd.Series(sims)], ignore_index=True)
        if original_length != len(current_prices):
            with open("current_prices.json", "w") as f:
                json.dump(current_prices, f)
        sims.to_csv("monte_carlo_sims.csv", index=False)
        clear_output(wait=True)
        print(f"{len(sims)}/{num}")
    return sims


def get_sim(_: int, current_prices: dict[str, float]):
    return run_simulation(current_prices)


def run_simulations_multi(num: int):
    try:
        sims = pd.read_csv("monte_carlo_sims.csv").squeeze("columns")
        print("loaded sims")
    except:
        sims = pd.Series(dtype=float)
        print("new sims")
    try:
        with open("current_prices.json", "r") as f:
            current_prices = json.load(f)
        print("loaded prices")
    except Exception:
        current_prices = {ticker: get_stock_close_price(ticker) for ticker in comparison_stocks}
        with open("current_prices.json", "w") as f:
            json.dump(current_prices, f)
        print("new prices")
    original_length = len(current_prices)
    with Pool(cpu_count()//2) as workers:
        sims = pd.Series(tqdm(workers.imap_unordered(partial(get_sim, current_prices=current_prices), range(num)), total=num))
    if original_length != len(current_prices):
        with open("current_prices.json", "w") as f:
            json.dump(current_prices, f)
    sims.to_csv("monte_carlo_sims.csv", index=False)
    return sims
