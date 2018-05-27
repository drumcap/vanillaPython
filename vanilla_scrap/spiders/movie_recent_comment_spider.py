# -*- coding: utf-8 -*-
import json_lines

__author__ = 'drumcap'

import scrapy

from vanilla_scrap.items import MovieCommentItem
from datetime import datetime
import sys
import re
import random
import time

extract_nums = lambda s: re.search('\d+', s).group(0)
sanitize_str = lambda s: s.strip()
rand_sleep = lambda max: time.sleep(int(random.randrange(1, max)))

NAVER_BASEURL = 'http://movie.naver.com/movie/point/af/list.nhn'

class MovieCommentSpider(scrapy.Spider):
    name = "movie-recent-comment"

    def extract_nums(self, s): return re.search('\d+', s).group(0)

    def start_requests(self):
        yield scrapy.Request(NAVER_BASEURL, self.parse_naver_cmt, dont_filter=True)

    def parse_naver_cmt(self, response):
        for sel in response.css('#old_content > table > tbody > tr'):
            item = MovieCommentItem()
            item['source'] = 'naver'
            item['review_id'] = sel.xpath('./td[@class="ac num"]/text()').extract_first()
            item['rating'] = int(sel.xpath('./td[@class="point"]/text()').extract_first())
            item['movie_id'] = extract_nums(sel.xpath('./td[@class="title"]/a/@href').extract_first())
            item['movie_name'] = sel.xpath('./td[@class="title"]/a/text()').extract_first()
            item['review_txt'] = ' '.join(sel.xpath('./td[@class="title"]/text()').extract()).strip()
            item['author'] = sel.xpath('./td[@class="num"]/a/text()').extract_first()
            item['date'] = datetime.strptime(sel.xpath('./td[@class="num"]/text()').extract_first(),'%y.%m.%d').astimezone().isoformat()
            yield item