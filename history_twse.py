#!/usr/bin/python3
# coding: utf-8
import requests
from io import StringIO
import pandas
from datetime import date,datetime
from dateutil.relativedelta import relativedelta
import os
import logging

col = ['date', 'volume', 'open', 'high', 'low', 'close', 'change']

base_url='http://www.twse.com.tw/exchangeReport/STOCK_DAY?response=csv&date={}&stockNo={}'
twse_col = ['date', 'volume', 'turnover', 'open', 'high', 'low', 'close', 'change', 'nr', 'none']

class HistoryTWSE:
    def __init__(self, stock_id, csv_path=None):
        self.stock_id = stock_id
        if csv_path != None:
            self.csv_path = csv_path
        else:
            self.csv_path = './storage/{}/history.csv'.format(self.stock_id)

        self.history_df = self.load_history()
        if self.history_df is None:
            self.history_df = self.build_stock_history_1y()


    def get_history_csv_twse(self, date):
        global base_url, twse_col
        req = requests.session()
        query_url = base_url.format(date, self.stock_id)
        logging.debug('query_url={}'.format(query_url))
        # Get the cookie first
        res = req.get('http://mis.twse.com.tw/stock/index.jsp', headers={'Accept-Language': 'zh-TW'})
        res = req.get(query_url)
        if not res.ok:
            return None

        raw = ','.join(twse_col) + '\n' + '\n'.join(res.text.split('\r\n')[2:-6])
        return raw


    def get_month_data(self, date):
        logging.debug('get_month_data stock_id={}, date={}'.format(self.stock_id, date))
        raw = self.get_history_csv_twse(date)
        df = pandas.read_csv(StringIO(raw))

        #discard unwanted columns
        df.drop('none', axis=1, inplace=True)
        df.drop('nr', axis=1, inplace=True)
        df.drop('turnover', axis=1, inplace=True)

        # convert comma separated string to int
        f = lambda x: int(x.replace(',',''))
        df['volume'] = df['volume'].map(f)

        f2 = lambda d: str(int(d.split('/')[0])+1911) + '-' + '-'.join(d.split('/')[1:])
        df['date'] = df['date'].map(f2)

        return df


    # build from begin until end
    def get_stock_history(self, begin, end):
        global col
        df = pandas.DataFrame(columns=col)
        __day = begin
        if end > date.today():
            end = date.today()

        while(__day <= end):
            df = df.append(self.get_month_data(__day.strftime('%Y%m%d')), ignore_index=True)
            __day += relativedelta(months=+1)

        # Get remaining if __day.month == end.month, we probably go over 'end'
        if (__day.month == end.month):
            df = df.append(self.get_month_data(end.strftime('%Y%m%d')), ignore_index=True)

        return df


    def build_stock_history_1y(self):
        today = date.today()
        begin = today + relativedelta(years=-1)
        return self.get_stock_history(begin, today)


    def save_history(self):
        if not os.path.exists(os.path.dirname(self.csv_path)):
            os.makedirs(os.path.dirname(self.csv_path))

        f = open(self.csv_path, 'w')
        f.write(self.history_df.to_csv(index=False))
        f.close()


    def load_history(self):
        if not os.path.exists(self.csv_path):
            return None

        f = open(self.csv_path, 'r')
        df = pandas.read_csv(f)
        f.close()

        if len(df.columns.difference(col)) != 0:
            logging.error('file %s format mismatch!' % self.csv_path)
            return None
        else:
            logging.debug('History is loaded from %s' % self.csv_path)
            return df


    def update_history(self):
        logging.info('Require update stock_id=%s' % self.stock_id)
        last_day_str = self.history_df.tail(1).iloc[0]['date']
        last_day = datetime.strptime(last_day_str, '%Y-%m-%d').date()

        if ( last_day < date.today()):
            new_df = self.get_stock_history(last_day + relativedelta(days=1), date.today())

        # remove duplicate date
        for idx, row in new_df.iterrows():
            __day = datetime.strptime(row['date'], '%Y-%m-%d').date()
            if __day <= last_day:
                logging.debug('Remove duplicate date: %s' % __day)
                new_df.drop(idx, inplace=True)

        self.history_df = self.history_df.append(new_df, ignore_index=True)


    def get_history_df(self):
        return self.history_df.copy()


if __name__ == '__main__':
    import argparse
    logging.basicConfig(level=logging.DEBUG)
    parser = argparse.ArgumentParser(description='Obtain price histroy from TWSE.')
    parser.add_argument('-s', '--stock', type=str,
                        help='Specify stock ID you want to get history.')
    parser.add_argument('-p', '--path', type=str,
                        help='Specify path to history csv for reading/writing. '
                        'Default: ./storage/[stock_id]/history.csv')
    args = parser.parse_args()

    if args.stock == None:
        print('You did not specify stock id\n');
        quit()

    if args.path is None:
        history = HistoryTWSE(args.stock)
    else:
        history = HistoryTWSE(args.stock, csv_path=args.path)
    history.update_history()
    history.save_history()

