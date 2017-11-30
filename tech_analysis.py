#!/usr/bin/python3
# coding: utf-8
import pandas as pd
import numpy as np


def calc_sma(history_df, window_list):
    for n in window_list:
        col_name = '{}ma'.format(n)
        history_df[col_name] = history_df.close.rolling(n).mean()


def calc_bband(history_df, window=22):
    r = history_df.close.rolling(window)
    col_name_ma = '{}ma'.format(window)
    history_df[col_name_ma] = r.mean()
    col_name_std = '{}std'.format(window)
    history_df[col_name_std] = r.std()
    history_df['bband_up'] = history_df[col_name_ma] + 2 * history_df[col_name_std]
    history_df['bband_low'] = history_df[col_name_ma] - 2 * history_df[col_name_std]


def calc_kd(history_df):
    rsv = 100.0 * (history_df['close'] - history_df['low'].rolling(9).min()) / \
                (history_df['high'].rolling(9).max() - history_df['low'].rolling(9).min())
    rsv.fillna(method='bfill', inplace=True)

    k = [50]
    d = [50]
    for i in range(1, len(history_df.index)):
        k.append((2 * k[i-1] + 1 * rsv[i] ) / 3)
        d.append((2 * d[i-1] + 1 * k[i]) / 3)
    history_df['k'] = k
    history_df['d'] = d
