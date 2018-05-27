# -*- coding: utf-8 -*-
import os

__author__ = 'drumcap'

import scrapy

from vanilla_scrap.items import MovieScrapItem
from urllib.parse import urlparse, parse_qs
from datetime import datetime
import re
import random
import time

extract_nums = lambda s: re.search('\d+', s).group(0)
sanitize_str = lambda s: s.strip()
rand_sleep = lambda max: time.sleep(int(random.randrange(1, max)))


NAVER_MOVIE_RANK  = 'https://movie.naver.com/movie/sdb/rank/rmovie.nhn?sel=pnt&tg=%s&page=%s'

class MovieCommentSpider(scrapy.Spider):
    name = "movie-rank"

    def start_requests(self):
        for i in range(1, 20, 1):
            yield scrapy.Request(NAVER_MOVIE_RANK % (i, 1), self.parse_naver_rank)

    def parse_naver_rank(self, response):
        for sel in response.css('#old_content > table > tbody > tr > td.title > div > a'):
            item = MovieScrapItem()
            movie_url = sel.css('a::attr(href)').extract_first()
            item['movie_id'] = parse_qs(urlparse(movie_url).query).get('code')[0]
            item['movie_name'] = sel.css('a::attr(title)').extract_first()
            yield item

        next_page = response.css('.pagenavigation .next > a::attr(href)').extract_first()
        next_page_num = parse_qs(urlparse(next_page).query).get('page')
        if next_page is not None:
            print("1 ######## go next page {}".format(next_page))
            yield response.follow(next_page, callback=self.parse_naver_rank)