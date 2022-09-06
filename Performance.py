import pandas as pd

import Logger
import math
from scipy.stats import norm


def create_summary_stats(log: pd.DataFrame):
    # total return, sharpe ratio, max drawdown, drawdown duration
    pass


def max_drawdown(log: pd.DataFrame):
    # find max
    max_ret = log["Daily Return"].max()
    max_date = log.loc[log["Daily Return"] == max_ret]["Time"].iloc[0]
    max_val = log.loc[log["Daily Return"] == max_ret]["Value"].iloc[0]
    # find min value after max_date
    min_val = log[log["Time"] > max_date]["Value"].min()
    return (max_val - min_val) / max_val


def sharpe_ratio(log: pd.DataFrame):
    return log["Daily Return"].mean() / log["Daily Return"].std()


def prob_sharpe(log: pd.DataFrame):
    skew = log["Daily Return"].skew()
    kurt = log["Daily Return"].kurtosis()
    shrp = sharpe_ratio(log)
    shrp_std = math.sqrt(1 / (len(log["Daily Return"]) - 1) * (
                1 + .5 * shrp ** 2 - skew * sharpe_ratio(log) + (kurt - 3) / 4 * shrp ** 2))
    return norm.cdf(shrp / shrp_std)
