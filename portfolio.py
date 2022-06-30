import pandas as pd
from IPython.display import display

from ticker_utils import TickerUtils


def get_cost_basis_per_share():
    purchases_df = pd.read_csv("purchases_df.csv")
    grouped_by_ticker = purchases_df.groupby(['ticker_sym'])
    cost_basis_per_share = grouped_by_ticker[['qty', 'cost']].sum().reset_index()
    cost_basis_per_share["cost_per_share"] = cost_basis_per_share["cost"].divide(cost_basis_per_share["qty"])
    cost_basis_per_share["current_price"] = (cost_basis_per_share["ticker_sym"].apply(lambda ticker: TickerUtils.get_stock_spot_price(ticker)))
    cost_basis_per_share["pl_per_share"] = cost_basis_per_share["current_price"].subtract(cost_basis_per_share["cost_per_share"])
    cost_basis_per_share["current_value"] = cost_basis_per_share["current_price"].multiply(cost_basis_per_share["qty"])
    cost_basis_per_share["current_pl"] = cost_basis_per_share["current_value"].subtract(cost_basis_per_share["cost"])
    cost_basis_per_share["roi"] = (cost_basis_per_share["current_value"]
                                   .divide(cost_basis_per_share["cost"])
                                   .subtract(1))
    cost_basis_per_share = cost_basis_per_share[["ticker_sym",
                                                 "qty",
                                                 "cost",
                                                 "current_value",
                                                 "current_pl",
                                                 "roi",
                                                 "cost_per_share",
                                                 "current_price",
                                                 "pl_per_share"]]
    return cost_basis_per_share


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
    current_prices = TickerUtils.get_current_prices(comparison_tickers, is_crypto=False)
    current_prices.update(TickerUtils.get_current_prices(["BTC"], is_crypto=True))
    # print(json.dumps(current_prices, indent=4))

    rois = {}
    for ticker in comparison_tickers:
        col = TickerUtils.add_suffix(ticker, "qty")
        value = None
        for df in all_purchases:
            if col in df.columns:
                value = current_prices[ticker] * df[col].sum()
                break
        if value is None:
            raise Exception(f"unknown ticker - {col}")
        rois[TickerUtils.add_suffix(ticker, "roi")] = (value/total_cost)-1
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
