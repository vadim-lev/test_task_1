import os
from datetime import datetime

import requests
import sqlite3

URL_list = 'https://www.investing.com/search/service/searchTopBar'
headers = {'X-Requested-With': 'XMLHttpRequest',
           'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                         'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/'
                         '84.0.4147.89 Safari/537.36'}

data = {'search_text': 'Manufacturing Purchasing Managers',
        'limit': '270'}

try:
    path = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                        'DB-table.db')
    os.remove(path)
except FileNotFoundError:
    print('File does not exist')

timeseries = requests.post(URL_list, data=data, headers=headers).json()

link_timeseries = [[x['dataID'], 'https://www.investing.com' + x['link'],
                    x['name']] for x in timeseries['ec_event']]

conn = sqlite3.connect('DB-table.db')
c = conn.cursor()

c.execute('''CREATE TABLE timeseries(id text, name text)''')
c.execute('''CREATE TABLE timeseries_value(id text, date text ,
            value text)''')
conn.commit()

c.executemany('INSERT INTO timeseries VALUES (?,?)',
              [[x[0], x[2]] for x in link_timeseries])
conn.commit()
print('The Economic Events entry is complete\n')

for id in [i[0] for i in link_timeseries]:
    URL_timeseries = 'https://sbcharts.investing.com/events_charts/us/' +\
                     id + '.json'

    timeseries_value = requests.get(URL_timeseries,
                                    headers=headers).json()['attr']

    data_result = []

    for x in timeseries_value:
        data = datetime.fromtimestamp(int(x['timestamp']) //
                                      1000).strftime('%d-%m-%Y %H:%M:%S')

        data_result.append([id, data, x['actual']])

    print('{:<15}{:>6}{:>25}'.format('timeseries', id,
                                     'recording is complete'))

    c.executemany('INSERT INTO timeseries_value VALUES (?,?,?)', data_result)

c.execute('''SELECT COUNT(date) FROM timeseries_value;''')
totalRows = c.fetchone()
print("\nTotal rows are in the timeseries_value table:  ", totalRows[0])
conn.commit()
