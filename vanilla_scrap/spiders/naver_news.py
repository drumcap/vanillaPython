# -*- coding: utf-8 -*-
import re
from datetime import date, timedelta
from urllib.parse import urlparse, parse_qs, parse_qsl, urlencode, urlunparse
from vanilla_scrap.items import NaverNewsItem

import scrapy

########################################################################
# Constants
########################################################################
# URL_TMPL = 'http://news.naver.com/main/ranking/popularDay.nhn?rankingType=popular_day&sectionId=000&date={date}'  # noqa
# URL_TMPL = 'http://news.naver.com/main/ranking/popularDay.nhn?rankingType=popular_day&date={date}'  # noqa


URL_TMPL = 'http://m.news.naver.com/rankingList.nhn?sid1=100&date={date}'  # 모바일

# DESKTOP
# LINK_CSS_SELECTOR = 'td.content dt > a'
# SELECTOR_TITLE = ''

# MOBILE
SELECTOR_LINK = 'div.ranking_news ul > li > a'
SELECTOR_TITLE = ' .commonlist_tx_headline'



# START_DATE = date(2004, 4, 20)
START_DATE = date(2018, 5, 27)
END_DATE = date.today()
DATE_FMT = '%Y%m%d'
DATE_RE = re.compile('&date=(.*?)$', re.DOTALL) #date 파라미터 가져오는 값 가져오기

CATEGORIES = range(100,106)

# get_qs = lambda q, news_url: parse_qs(urlparse(news_url).query).get(q)[0]

def get_query_field(url, field):
    try:
        return parse_qs(urlparse(url).query)[field][0]
    except KeyError:
        return ""

def set_query_field(url, field, value, replace=False):
    # Parse out the different parts of the URL.
    components = urlparse(url)
    query_pairs = parse_qsl(urlparse(url).query)

    if replace:
        query_pairs = [(f, v) for (f, v) in query_pairs if f != field]
    query_pairs.append((field, value))

    new_query_str = urlencode(query_pairs)

    # Finally, construct the new URL
    new_components = (
        components.scheme,
        components.netloc,
        components.path,
        components.params,
        new_query_str,
        components.fragment
    )
    return urlunparse(new_components)

########################################################################
# Codes
########################################################################


def _date_to_str_date(date):
    return date.strftime(DATE_FMT)


def _daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


def yield_start_urls():
    for dt in _daterange(START_DATE, END_DATE + timedelta(days=1)):
        yield URL_TMPL.format(date=_date_to_str_date(dt))


class NaverNewsSpider(scrapy.Spider):
    name = 'naver_news'
    # allowed_domains = ['http://news.naver.com', 'http://m.news.naver.com']

    def start_requests(self):
        for url in yield_start_urls():
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        # date = DATE_RE.findall(response.url)[0]
        date = get_query_field(response.url, 'date')
        urls = response.css(SELECTOR_LINK + '::attr(href)').extract()
        titles = response.css(SELECTOR_LINK + SELECTOR_TITLE + '::text').extract()
        iterable = enumerate(zip(urls, titles), start=1)

        for rank, (news_url, news_title) in iterable:
            url_qs = parse_qs(urlparse(news_url).query)
            url = response.urljoin(news_url) #self.allowed_domains[1] + news_url
            item = NaverNewsItem()
            item['rank'] = rank
            item['aid'] = url_qs['aid'][0]
            item['oid'] = url_qs['oid'][0]
            item['sid1'] = url_qs['sid1'][0]
            item['rank'] = rank
            item['url'] = url
            item['title'] = news_title
            item['date'] = date
            yield item
            yield response.follow(url=url, callback=self.parse_naver_detail)

        sid1 = int(get_query_field(response.url, 'sid1')) + 1
        if (sid1 in CATEGORIES):
            next_url = set_query_field(response.url, 'sid1', sid1, replace=True)
            yield response.follow(url=next_url, callback=self.parse)

    def parse_naver_detail(self, response):
        detail_txt = ' '.join(response.css("#dic_area::text").extract())
        yield {
            'type':'detail',
            'detail_txt':detail_txt
        }