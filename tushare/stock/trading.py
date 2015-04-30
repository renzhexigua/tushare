# -*- coding:utf-8 -*- 
"""
交易数据接口 
Created on 2014/07/31
@author: Jimmy Liu
@group : waditu
@contact: jimmysoa@sina.cn
"""
from __future__ import division

import time
import json
import lxml.html
from lxml import etree
import pandas as pd
import numpy as np
from tushare.stock import cons as ct
from pandas.util.testing import _network_error_classes
import re
from pandas.compat import StringIO
from tushare.util import dateu as du
try:
    from urllib.request import urlopen, Request
except ImportError:
    from urllib2 import urlopen, Request


def get_tick_data(code=None, start=None, end=None, retry_count=3, pause=0.001):
    """
        获取分笔数据
    Parameters
    ------
        code:string
                  股票代码 e.g. 600848
        date:string
                  日期 format：YYYY-MM-DD
        retry_count : int, 默认 3
                  如遇网络等问题重复执行的次数
        pause : int, 默认 0
                 重复请求数据过程中暂停的秒数，防止请求间隔时间太短出现的问题
     return
     -------
        DataFrame 当日所有股票交易数据(DataFrame)
              属性:成交时间、成交价格、价格变动，成交手、成交金额(元)，买卖类型
    """
    if code is None or len(code)!=6 or start is None or end is None:
        return None
    date_list = pd.date_range(start, end)
    symbol = _code_to_symbol(code)
    global cols
    cols = list(ct.TODAY_TICK_COLUMNS)
    cols.insert(0,'Date')
    data = pd.DataFrame(columns=cols)
    for _ in range(retry_count):
        time.sleep(pause)
        try:
            for date_i in date_list:
                data = pd.DataFrame(columns=cols)
                date = date_i.strftime("%Y-%m-%d")
                re = Request(ct.TODAY_TICKS_PAGE_URL % (ct.P_TYPE['http'], ct.DOMAINS['vsf'], 
                                    ct.PAGES['jv'], date,
                                    symbol))
                data_str = urlopen(re, timeout=10).read()
                if data_str == '({__ERROR:"MYSQL",__ERRORNO:1146})':
                    continue
                data_str = data_str.decode('GBK')
                data_str = data_str[1:-1]
                data_str = eval(data_str, type('Dummy', (dict,), 
                                           dict(__getitem__ = lambda s, n:n))())
                data_str = json.dumps(data_str)
                data_str = json.loads(data_str)
                pages = len(data_str['detailPages'])
                ct._write_head()
                for pNo in range(1,pages+1):
                    data = data.append(_today_ticks(symbol, date, pNo, retry_count, pause), ignore_index=True)
                data.to_excel('/home/rjx/%s_%s.xlsx'%(symbol,date))
        except Exception as er:
            print(str(er))
        else:
            return data
    raise IOError("获取失败，请检查网络")


def _today_ticks(symbol, tdate, pageNo, retry_count, pause):
    ct._write_console()
    for _ in range(retry_count):
        time.sleep(pause)
        try:
            html = lxml.html.parse(ct.TODAY_TICKS_URL % (ct.P_TYPE['http'],
                                                         ct.DOMAINS['vsf'], ct.PAGES['t_ticks'],
                                                         symbol, tdate, pageNo
                                ))  
            res = html.xpath('//table[@id=\"datatbl\"]/tbody/tr')
            if ct.PY3:
                sarr = [etree.tostring(node).decode('utf-8') for node in res]
            else:
                sarr = [etree.tostring(node) for node in res]
            sarr = ''.join(sarr)
            sarr = '<table>%s</table>'%sarr
            sarr = sarr.replace('--', '0')
            #df = pd.DataFrame(columns=ct.TODAY_TICK_COLUMNS)
            df = pd.read_html(StringIO(sarr), infer_types=False)[0]
            df.columns = list(ct.TODAY_TICK_COLUMNS)
            df['Pchange'] = df['Pchange'].map(lambda x : x.replace('%',''))
            df.insert(0,'Date',tdate)
            #cols = ct.TODAY_TICK_COLUMNS.insert(0,'Date')
            df = df[cols]
        except _network_error_classes:
            pass
        else:
            return df
    raise IOError("获取失败，请检查网络" )


def _code_to_symbol(code):
    """
        生成symbol代码标志
    """
    if code in ct.INDEX_LABELS:
        return ct.INDEX_LIST[code]
    else:
        if len(code) != 6 :
            return ''
        else:
            return 'sh%s'%code if code[:1] == '6' else 'sz%s'%code

if __name__ == "__main__":
    for code in ['002135', '601021', '600029', '000520', '300272', '002598', '002591', '002435', '002342', '603099',
            '601888', '300039', '002434', '002594', '600616', '300005', '002154', '002321', '002173', '002205',
            '000973', '002136', '603799', '601069', '600832', '600845', '300020', '002232', '300227', '002463',
            '002402', '300081', '300367', '002210', '002573', '002518', '002523', '002158', '300068', '002112']:
        print(get_h_data(code, start='2014-01-01', end='2015-04-24'))
