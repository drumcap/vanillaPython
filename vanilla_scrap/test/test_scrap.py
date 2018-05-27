# Intellij Scrapy 테스트용 실행파일


from scrapy import cmdline

# cmdline.execute("scrapy crawl quotes".split())
cmdline.execute("scrapy crawl naver_news".split())
# cmdline.execute("scrapy crawl movie-rank".split())                        #영화 랭킹정보
# cmdline.execute("scrapy crawl movie-comment".split())                       #영화댓글 전체 크롤링
# cmdline.execute("scrapy crawl movie-recent-comment".split())                       #영화댓글 전체 크롤링