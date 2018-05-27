# -*- coding: utf-8 -*-
import json_lines

__author__ = 'drumcap'

import scrapy

from vanilla_scrap.items import MovieCommentItem
from urllib.parse import urlparse, parse_qs
from datetime import datetime
import re
import random
import time
import json

extract_nums = lambda s: re.search('\d+', s).group(0)
sanitize_str = lambda s: s.strip()
rand_sleep = lambda max: time.sleep(int(random.randrange(1, max)))

NAVER_BASEURL     = 'http://movie.naver.com/movie/point/af/list.nhn'
NAVER_RATINGURL   = NAVER_BASEURL + '?&page=%s'
NAVER_MOVIEURL    = NAVER_BASEURL + '?st=mcode&target=after&sword=%s&page=%s'

NAVER_MOVIE_RANK  = 'https://movie.naver.com/movie/sdb/rank/rmovie.nhn?sel=pnt&tg=%s&page=%s'

class MovieCommentSpider(scrapy.Spider):
    name = "movie-comment"

    def extract_nums(self, s): return re.search('\d+', s).group(0)

    def start_requests(self):
        filename = 'movie-info-items.jl'

        with json_lines.open(filename) as f:
            for item in f:
                yield scrapy.Request(NAVER_MOVIEURL % (item.get('movie_id'), 1), self.parse_naver_cmt)

    def parse_naver_cmt(self, response):
        dtnow = datetime.now()
        for sel in response.css('#old_content > table > tbody > tr'):
            item = MovieCommentItem()
            item['source'] = 'naver'
            item['review_id'] = sel.xpath('./td[@class="ac num"]/text()').extract_first()
            item['rating'] = sel.xpath('./td[@class="point"]/text()').extract_first()
            item['movie_id'] = extract_nums(sel.xpath('./td[@class="title"]/a/@href').extract_first())
            item['movie_name'] = sel.xpath('./td[@class="title"]/a/text()').extract_first()
            item['review_txt'] = ' '.join(sel.xpath('./td[@class="title"]/text()').extract()).strip()
            item['author'] = sel.xpath('./td[@class="num"]/a/text()').extract_first()
            item['date'] = datetime.strptime(sel.xpath('./td[@class="num"]/text()').extract_first(),'%y.%m.%d').astimezone().isoformat()
            yield item

        next_page = response.css('.paging .pg_next::attr(href)').extract_first()
        next_page_n = parse_qs(urlparse(next_page).query).get('page')
        next_page_num = int(next_page_n[0]) if next_page_n is not None else 0
        if next_page is not None and next_page_num < 1000:
            print("2 ######## go next page {}".format(next_page))
            yield response.follow(next_page, callback=self.parse_naver_cmt)