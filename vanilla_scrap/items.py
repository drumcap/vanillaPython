# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class MovieScrapItem(scrapy.Item):
    # define the fields for your item here like:
    movie_id = scrapy.Field()
    movie_name = scrapy.Field()
    rating = scrapy.Field()
    source = scrapy.Field()
    pass


class MovieCommentItem(scrapy.Item):
    # define the fields for your item here like:
    review_id = scrapy.Field()
    rating = scrapy.Field()
    movie_id = scrapy.Field()
    movie_name = scrapy.Field()
    review_title = scrapy.Field()
    review_txt = scrapy.Field()
    author = scrapy.Field()
    date = scrapy.Field()
    source = scrapy.Field()
    pass


class CommunityItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    source = scrapy.Field()
    category = scrapy.Field()
    title = scrapy.Field()
    url = scrapy.Field()
    hits = scrapy.Field()
    date = scrapy.Field()
    pass


class NaverNewsItem(scrapy.Item):
    aid = scrapy.Field()
    oid = scrapy.Field()
    sid1 = scrapy.Field()
    rank = scrapy.Field()
    type = scrapy.Field()
    url = scrapy.Field()
    title = scrapy.Field()
    detail_txt = scrapy.Field()
    date = scrapy.Field()
    pass
