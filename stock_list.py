#!/usr/bin/python3
# coding: utf-8
'''
 This file gets all stock list from either TWSE website or saved csv file.
 #上市: http://isin.twse.com.tw/isin/C_public.jsp?strMode=2
 #上櫃: http://isin.twse.com.tw/isin/C_public.jsp?strMode=4
'''
import pandas as pd
import numpy as np
import argparse
import logging

twse_list_url = 'http://isin.twse.com.tw/isin/C_public.jsp?strMode=2'


class StockList:

    def from_twse(url):
        # ToDo: error checking
        orig_df = pd.read_html(twse_list_url, encoding='big5hkscs', header=0,
                               flavor='html5lib')[0]

        # filter out interesting part
        df = orig_df.drop(orig_df.columns[[1, 6]], axis=1)
        df = df[(df['CFICode']=='ESVUFR') | (df['CFICode']=='CEOGEU') | (df['CFICode']=='CEOGDU')]
        # split id and name
        df.columns = ['id_and_name', 'begin date', 'type', 'class', 'CFICode']
        df['id'], df['name'] = df['id_and_name'].str.split(u'　').str

        df.reset_index(inplace=True, drop=True)
        df.loc[df['CFICode']=='CEOGEU', 'class'] = 'ETF'
        df.loc[df['CFICode']=='CEOGDU', 'class'] = 'ETF'
        df.drop(['CFICode', 'id_and_name'], axis=1, inplace=True)
        df = df.reindex_axis(['id', 'name', 'type', 'class', 'begin date'], axis=1)

        return df


    def from_csv_url(url=None):
        import requests
        from io import StringIO

        if url is None:
            url = stock_list_gist_url
        res = requests.get(url)
        csv_str = StringIO(res.text)
        df = pd.read_csv(csv_str)
        return df


    def from_csv_file(f):
        df = pd.read_csv(f)
        # ToDo: error checking
        return df


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description='Obtain stock list.')
    parser.add_argument('-s', '--src', type=str, choices=['twse', 'web', 'file'],
                        help='Specify where to get the list. Default=%(default)s',
                        default='twse')
    parser.add_argument('-f', '--file', type=argparse.FileType('r'),
                        help='Read stock list from a file in csv format')
    parser.add_argument('-u', '--url', type=str,
                        help='Read stock list from a url that contains a csv')
    parser.add_argument('-o', '--output', type=argparse.FileType('w'),
                        help='Output result to file in csv format')
    parser.add_argument('-v', '--verbose', action="store_true", default=False,
                        help='Enable verbose mode')
    args = parser.parse_args()

    if args.src == 'twse' :
        stock_df = StockList.from_twse(twse_list_url)
        if args.file is not None or args.url is not None:
            logging.warning("Input file or url is ignored in web mode.")
    elif args.src == 'web' :
        if args.url is None:
            logging.info("Url is not specified.")
            quit()
        stock_df = StockList.from_csv_url(args.url)
    elif args.src == 'file' :
        if args.file is not None:
            stock_df = StockList.from_csv_file(args.file)
        else:
            logging.critical("You have to specify input file.")

    if args.verbose == True:
        print(stock_df)
        logging.info(stock_df)

    if args.output is not None:
        stock_df.to_csv(args.output, index=False)
        logging.info("Write stock list to %s." % args.output.name)

