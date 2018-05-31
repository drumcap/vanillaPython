# -*- coding: utf-8 -*-
import re
from datetime import date, timedelta, datetime
from urllib.parse import urlparse, parse_qs, parse_qsl, urlencode, urlunparse
from bs4 import BeautifulSoup
from vanilla_scrap.items import NaverNewsItem

import scrapy

########################################################################
# Constants
########################################################################
URL_TMPL = 'http://news.naver.com/main/ranking/popularDay.nhn?rankingType=popular_day&sectionId=100&date={date}'  # noqa
# URL_TMPL = 'http://news.naver.com/main/ranking/popularDay.nhn?rankingType=popular_day&date={date}'  # noqa

# ENTER_RANK = 'http://m.entertain.naver.com/ranking.json?rankingDate=20180528'
# MOVIE = 'http://m.entertain.naver.com/movie'
# SPORTS = 'http://m.sports.naver.com/ranking.nhn?date=20180526'
# SPORTS_KBASEBALL = 'http://sports.news.naver.com/kbaseball/news/index.nhn?date=20150727&type=popular&isphoto=N'

# SPORTS_WBASEBALL = 'http://sports.news.naver.com/wbaseball/news/index.nhn?date=20150727&type=popular&isphoto=N'
# SPORTS_KFOOTBALL = 'http://sports.news.naver.com/kfootball/news/index.nhn?date=20150727&type=popular&isphoto=N'
# SPORTS_WFOOTBALL = 'http://sports.news.naver.com/wfootball/news/index.nhn?date=20150727&type=popular&isphoto=N'
# SPORTS_WFOOTBALL = 'http://sports.news.naver.com/basketball/news/index.nhn?date=20150727&type=popular&isphoto=N'
# SPORTS_WFOOTBALL = 'http://sports.news.naver.com/volleyball/news/index.nhn?date=20150727&type=popular&isphoto=N'
# SPORTS_WFOOTBALL = 'http://sports.news.naver.com/golf/news/index.nhn?date=20150727&type=popular&isphoto=N'

# URL_TMPL = 'http://m.news.naver.com/rankingList.nhn?sid1=100&date={date}'  # 모바일

# DESKTOP
# LINK_CSS_SELECTOR = 'td.content dt > a'
LINK_CSS_SELECTOR = 'td.content .ranking_item .ranking_headline > a'

# MOBILE
# SELECTOR_LINK = 'div.ranking_news ul > li > a'
# SELECTOR_TITLE = ' .commonlist_tx_headline'

START_DATE = date(2004, 4, 20)
# END_DATE = date(2014, 12, 31)
# START_DATE = date(2018, 5, 30)
END_DATE = date.today()
DATE_FMT = '%Y%m%d'
DATE_RE = re.compile('&date=(.*?)$', re.DOTALL)  # date 파라미터 가져오는 값 가져오기

CATEGORIES = range(100, 106)
SPORTS_CATEGORIES = ['kbaseball', 'wbaseball', 'kfootball', 'wfootball', 'basketball', 'volleyball', 'golf', 'general', 'esports']

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

    def start_requests(self):
        for url in yield_start_urls():
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        urls = response.css(LINK_CSS_SELECTOR + '::attr(href)').extract()
        titles = response.css(LINK_CSS_SELECTOR + '::text').extract()
        iterable = enumerate(zip(urls, titles), start=1)

        for rank, (news_url, news_title) in iterable:
            url = response.urljoin(news_url)  # self.allowed_domains[1] + news_url
            yield response.follow(url=url, callback=self.parse_naver_detail)
            # url_qs = parse_qs(urlparse(rank_url).query)
            # item = NaverNewsItem()
            # item['aid'] = url_qs['aid'][0]
            # item['oid'] = url_qs['oid'][0]
            # item['sid1'] = url_qs['sid1'][0]
            # item['rank'] = url_qs['rank'][0]
            # item['date'] = url_qs['date'][0]
            # item['url'] = rank_url
            # item['title'] = news_title
            # yield item

        next_sid = int(get_query_field(response.url, 'sectionId')) + 1
        if next_sid in CATEGORIES:
            next_url = set_query_field(response.url, 'sectionId', next_sid, replace=True)
            yield response.follow(url=next_url, callback=self.parse)

    def parse_naver_detail(self, response):
        soup = BeautifulSoup(response.text, 'html.parser')
        title = soup.find("meta",  property="og:title")["content"].strip()
        for s in soup(['script', 'style']):
            s.decompose()
        detail_txt = soup.find(id="articleBodyContents").get_text().strip()
        url_qs = parse_qs(urlparse(response.url).query)
        item = NaverNewsItem()
        item['aid'] = url_qs['aid'][0]
        item['oid'] = url_qs['oid'][0]
        item['sid1'] = url_qs['rankingSectionId'][0]
        item['rank'] = int(url_qs['rankingSeq'][0])
        item['date'] = datetime.strptime(url_qs['date'][0],'%Y%m%d').astimezone().isoformat()
        item['url'] = response.url
        item['title'] = title
        item['detail_txt'] = detail_txt.strip()
        yield item
