An ongoing analysis of my trust funds portfolio constructed by a "financial advisor" for a 1% fee.

Current there are three notebooks with corresponding python file for solidified code.

purchases.ipynb is the initial notebook where the initial excel file recieved is processed and cleaned into a useable pandas dataframe. Also constructed in this notebook is dataframes for comarison purchases, alt, btc, and etf.

portfolio.ipynb is a basic overview anaylsis. One table created is the overall P/L, ROI, and cost of the portfolio along with the comparison ROI of the major indexes, gold, a diverse commodity etf, 20 year bonds, and bitcoin. The second table is a break down of the portfolio performance grouped by each mutual fund.

monte_carlo.ipynb - I ran monte carlo simulations of this portfolio versus random stock picks. To elaborate on the methodology I used, for each simulation, I replaced each separate mutual fund with a random stock, all purchases were one-to-one, made on the same days and with the same total cost. The pool of stocks picked from is the stocks listed in the S&P500, Nasdaq, and Dow. Some stocks I could not get all the needed info for, whether they were delisted or some other reason, hence were excluded. This left the pool at a size of approximately 4000 stocks. A stock was only allowed to be picked once per simulation.