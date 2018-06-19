# -*- coding: utf-8 -*-

# Scrapy settings for esparser project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://doc.scrapy.org/en/latest/topics/settings.html
#     https://doc.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://doc.scrapy.org/en/latest/topics/spider-middleware.html
import os

BOT_NAME = 'esparser'

SPIDER_MODULES = ['jsearch.esparser.spiders']
NEWSPIDER_MODULE = 'jsearch.esparser.spiders'


# Crawl responsibly by identifying yourself (and your website) on the user-agent
# USER_AGENT = 'esparser (+http://www.yourdomain.com)'

# Obey robots.txt rules
ROBOTSTXT_OBEY = True

# Configure maximum concurrent requests performed by Scrapy (default: 16)
# CONCURRENT_REQUESTS = 32

# Configure a delay for requests for the same website (default: 0)
# See https://doc.scrapy.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
DOWNLOAD_DELAY = 0.5
# The download delay setting will honor only one of:
# CONCURRENT_REQUESTS_PER_DOMAIN = 16
# CONCURRENT_REQUESTS_PER_IP = 16

# Disable cookies (enabled by default)
# COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
# TELNETCONSOLE_ENABLED = False

# Override the default request headers:
# DEFAULT_REQUEST_HEADERS = {
#   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
#   'Accept-Language': 'en',
# }

# Enable or disable spider middlewares
# See https://doc.scrapy.org/en/latest/topics/spider-middleware.html
# SPIDER_MIDDLEWARES = {
#    'esparser.middlewares.EsparserSpiderMiddleware': 543,
# }

# Enable or disable downloader middlewares
# See https://doc.scrapy.org/en/latest/topics/downloader-middleware.html
# DOWNLOADER_MIDDLEWARES = {
#    'jsearch.esparser.middlewares.EsparserDownloaderMiddleware': 50,
# }

# Enable or disable extensions
# See https://doc.scrapy.org/en/latest/topics/extensions.html
# EXTENSIONS = {
#    'scrapy.extensions.telnet.TelnetConsole': None,
# }

# Configure item pipelines
# See https://doc.scrapy.org/en/latest/topics/item-pipeline.html
ITEM_PIPELINES = {
    'jsearch.esparser.pipelines.ContractPipeline': 300,
}


# Retry many times since proxies often fail
RETRY_TIMES = int(os.environ.get('SCRAPY_RETRY_TIMES', 20))
# Retry on most error codes since proxies fail for different reasons
RETRY_HTTP_CODES = [500, 503, 504, 523, 400, 403, 404, 408]

DOWNLOADER_MIDDLEWARES = {
    'scrapy.downloadermiddlewares.retry.RetryMiddleware': 90,
    'jsearch.esparser.middlewares.EsparserDownloaderMiddleware': 100,
    'scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware': 110,
}

# Proxy list containing entries like
# http://host1:port
# http://username:password@host2:port
# http://host3:port
# ...
# PROXY_LIST = os.path.join(os.path.dirname(__file__), 'proxy.list')

# Proxy mode
# 0 = Every requests have different proxy
# 1 = Take only one proxy from the list and assign it to every requests
# 2 = Put a custom proxy to use in the settings
PROXY_MODE = 0

PROXY_USER = 'mezrin'
PROXY_PASS = '3tuXHTFTj97tY'
PROXY_LIST = [
    '209.205.219.28:333',
    '209.205.219.27:333',
    '209.205.219.30:333',
    '209.205.219.29:333',
    '209.205.219.26:333',
    '209.205.219.26:1000',
    '209.205.219.26:1001',
    '209.205.219.26:1002',
    '209.205.219.26:1003',
    '209.205.219.26:1004',
    '209.205.219.26:1005',
    '209.205.219.26:1006',
    '209.205.219.26:1007',
    '209.205.219.26:1008',
    '209.205.219.26:1009',
    '209.205.219.26:1010',
    '209.205.219.26:1011',
    '209.205.219.26:1012',
    '209.205.219.26:1013',
    '209.205.219.26:1014',
    '209.205.219.26:1015',
    '209.205.219.26:1016',
    '209.205.219.26:1017',
    '209.205.219.26:1018',
    '209.205.219.26:1019',
    '209.205.219.26:1020',
    '209.205.219.26:1021',
    '209.205.219.26:1022',
    '209.205.219.26:1023',
    '209.205.219.26:1024',
    '209.205.219.26:1025',
    '209.205.219.26:1026',
    '209.205.219.26:1027',
    '209.205.219.26:1028',
    '209.205.219.26:1029',
    '209.205.219.26:1030',
    '209.205.219.26:1031',
    '209.205.219.26:1032',
    '209.205.219.26:1033',
    '209.205.219.26:1034',
    '209.205.219.26:1035',
    '209.205.219.26:1036',
    '209.205.219.26:1037',
    '209.205.219.26:1038',
    '209.205.219.26:1039',
    '209.205.219.26:1040',
    '209.205.219.26:1041',
    '209.205.219.26:1042',
    '209.205.219.26:1043',
    '209.205.219.26:1044',
    '209.205.219.26:1045',
    '209.205.219.26:1046',
    '209.205.219.26:1047',
    '209.205.219.26:1048',
    '209.205.219.26:1049',
    '209.205.219.26:1050',

]

SENTRY_DSN = 'https://8be30c1f50d648549d4726b55f2d06de:2831f3e43de04f1c82ed755c2a256206@sentry.io/1188901'

EXTENSIONS = {
    "scrapy_sentry.extensions.Errors": 10,
}
