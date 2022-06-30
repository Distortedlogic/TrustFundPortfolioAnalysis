import random
from functools import partial
from multiprocessing import Pool, cpu_count

import pandas as pd
from tqdm import tqdm

from purchases import *

purchases_df = pd.read_csv("purchases_df.csv")
total_cost: float = purchases_df["cost"].sum()
alt_purchases = pd.read_csv("alt_purchases.csv")
advisors_stocks: list[str] = list(set(purchases_df["ticker_sym"]))
comparison_stocks: list[str] = list(alt_purchases.columns)


def get_advisors_stocks_to_random_stocks():
    random_stocks = random.sample(comparison_stocks, k=len(advisors_stocks))
    assert "Unnamed: 0" not in random_stocks
    assert len(random_stocks) == len(advisors_stocks)
    return dict(zip(advisors_stocks, random_stocks))


def get_current_roi() -> float:
    grouped_by_ticker = purchases_df.groupby(['ticker_sym'])
    cost_basis_per_share = grouped_by_ticker[['cost', 'qty']].sum().reset_index()
    cost_basis_per_share["current_price"] = cost_basis_per_share["ticker_sym"].apply(TickerUtils.get_stock_spot_price)
    cost_basis_per_share["current_value"] = cost_basis_per_share["current_price"].multiply(cost_basis_per_share["qty"])
    current_roi = cost_basis_per_share["current_value"].sum()/cost_basis_per_share["cost"].sum() - 1
    return current_roi


def run_simulation(current_prices: dict[str, float] = {}) -> float:
    advisors_stocks_to_random_stocks = get_advisors_stocks_to_random_stocks()
    monte_carlo_df = pd.concat([purchases_df, alt_purchases[advisors_stocks_to_random_stocks.values()]], axis=1)
    grouped_by_ticker = monte_carlo_df.groupby(['ticker_sym'])
    cost_basis_per_share = grouped_by_ticker[['cost', *advisors_stocks_to_random_stocks.values()]].sum().reset_index()

    def get_comparison_qty(ticker: str):
        mask = cost_basis_per_share[cost_basis_per_share["ticker_sym"] == ticker]
        comparison_stock = advisors_stocks_to_random_stocks[ticker]
        values = mask[comparison_stock].values
        value = values[0]
        assert isinstance(value, float)
        return value
    cost_basis_per_share["comparison_qty"] = cost_basis_per_share["ticker_sym"].apply(get_comparison_qty)
    cost_basis_per_share["comparison_ticker"] = cost_basis_per_share["ticker_sym"].apply(
        lambda ticker: TickerUtils.extract_ticker(advisors_stocks_to_random_stocks[ticker]))
    cost_basis_per_share["comparison_current_price"] = cost_basis_per_share["comparison_ticker"].apply(lambda ticker: current_prices[ticker])
    cost_basis_per_share["comparison_current_value"] = cost_basis_per_share["comparison_current_price"].multiply(cost_basis_per_share["comparison_qty"])
    comparison_roi = cost_basis_per_share["comparison_current_value"].sum()/total_cost - 1
    assert isinstance(comparison_roi, float)
    return comparison_roi


def run_simulations(num: int):
    try:
        sims = pd.read_csv("monte_carlo_sims.csv").squeeze("columns")
        print("loaded sims")
    except:
        sims = pd.Series(dtype=float)
        print("new sims")
    current_prices = TickerUtils.get_current_prices(TickerUtils.get_comparison_stocks(), is_crypto=False)
    while len(sims) < num:
        new_sims = [run_simulation(current_prices) for _ in range(num)]
        assert isinstance(new_sims[0], float)
        sims = pd.concat([sims, pd.Series(new_sims)], ignore_index=True)
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
    current_prices = TickerUtils.get_current_prices(TickerUtils.get_comparison_stocks(), is_crypto=False)
    with Pool(cpu_count()//2) as workers:
        sims = pd.Series(tqdm(workers.imap_unordered(partial(get_sim, current_prices=current_prices), range(num)), total=num))
    sims.to_csv("monte_carlo_sims.csv", index=False)
    return sims
