# -*- coding: utf-8 -*-
import scrapy
from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging
from twisted.internet import reactor
import json
import w3lib.html
import boto3
import base64
import hashlib
import datetime
import logging
import time

class ArticleScraperPipeline:
    def process_item(self, item, spider):
        #send to processFinanceArticle queue via SQS
        for field in item:
            if not item[field]:
                if field == 'stockTicker':
                    item['field'] = ''
                else:
                    return item

        if ('content' in item) and len(item['content']) > 200:
            finance.send_message(MessageBody = json.dumps(item))
            logger.info(f"accepted: {item['originalUrl']}")
            logger.info(f"content: {item['content'][:500]}")
        else:
            logger.info(f"rejected: {item.get('originalUrl','none')}")
        return item

def hashUrl(url):
    hasher = hashlib.sha1(url.encode('utf-8'))
    return base64.urlsafe_b64encode(hasher.digest()[:10])[:-2].decode("utf-8")

def getUrlsToScrape(response):
    #remove duplicate urls and move to single list
    urls = set()
    for i in range(len(response['Messages'])):
        for url in json.loads(response['Messages'][i]['Body'])['urls']:
            urls.add(url)
    urls = list(urls)

    if len(urls) == 0:
        return None

    #remove urls which we don't support scraping for, and
    #remove urls which we have already indexed through lexis-nexis
    allowedUrls = []
    i = 0
    while i < len(urls):
        keys = []
        while (i < len(urls)) and (len(keys) < 10):
            if 'fool.com' in urls[i]:
                keys.append(urls[i])
            i += 1

        if len(keys) == 0:
            continue

        query = {
                'FinanceArticles': {
                    'Keys': [{'originalUrl' : x} for x in keys],
                    'ProjectionExpression' : 'originalUrl'
                    }
            }

        dynamo_response = dynamodb.batch_get_item(
            RequestItems=query,
            ReturnConsumedCapacity='TOTAL'
        )

        indexedUrls = set()
        for j in range(len(dynamo_response['Responses']['FinanceArticles'])):
            indexedUrls.add(dynamo_response['Responses']['FinanceArticles'][j]['originalUrl'])

        for url in keys:
            if (url not in indexedUrls) and (url not in alreadySeen):
                allowedUrls.append(url)
                alreadySeen.add(url) #move to before database query

    return allowedUrls

def setup(runner):
    logger.info('checking SQS for messages')
    time.sleep(1) #take a break on recur
    #take in urls passed by API through SQS
    response = sqs.receive_message(
        QueueUrl=queue_url,
        AttributeNames=['SentTimestamp'],
        MaxNumberOfMessages=10,
        MessageAttributeNames=['All'],
        VisibilityTimeout=60,
        WaitTimeSeconds=20
    )

    allowedUrls = []

    if 'Messages' not in response:
        logger.info('No messages found')
        time.sleep(5)
    else:
        logger.info(f'Found {len(response["Messages"])} new messages')

        allowedUrls = getUrlsToScrape(response)

        for i in range(len(response['Messages'])):
                receipt_handle = response['Messages'][i]['ReceiptHandle']
                sqs.delete_message(
                    QueueUrl=queue_url,
                    ReceiptHandle=receipt_handle
                )

    d = runner.crawl(FoolArticleSpider,start_urls=allowedUrls)
    d.addBoth(lambda _: setup(runner))
    return d

class FoolArticleSpider(scrapy.Spider):
    name = "fool-article"

    def parse(self, response):
        data = {}
        try:
            data = {
            'originalUrl' : response.url,
            'url' : response.url, #to match lexis format
            'title': response.css(".article-header h1::text").extract_first(),
            'content': w3lib.html.remove_tags(response.css(".article-content").get()).strip(),
            'author': {'name' : response.css(".author-name a::text").extract_first()},
            'publishedDate': response.xpath("//meta[@name='date']/@content").get(),
            'estimatedPublishedDate': response.xpath("//meta[@name='date']/@content").get(),
            'harvestDate': datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            'stockTicker': response.xpath("//meta[@name='tickers']/@content").get().split(',')[0],
            'duplicateGroupId' : 'S' + str(hashUrl(response.url)), #signify it came from scrapy and not lexis
            'index-date' : response.xpath("//meta[@name='date']/@content").get()[:10],
            'languageCode' : response.xpath("/html/@lang").get()
            }
            if data['author']['name'] is None:
                data['author']['name'] = 'unknown'
        except:
            logger.exception('could not generate response fields')
            pass
        yield data

logging.basicConfig(filename='scraper.txt',
                            filemode='a',
                            format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                            datefmt='%H:%M:%S',
                            level=logging.INFO)

logger = logging.getLogger()

#configure_logging({'LOG_FORMAT': '%(levelname)s: %(message)s', 'LOG_FILE' : 'log.txt'})

sqs = boto3.client('sqs')
finance = boto3.resource('sqs').Queue('https://sqs.us-east-1.amazonaws.com/051011752121/nlp-finance')
dynamodb = boto3.resource("dynamodb", region_name='us-east-1')
financeArticles = dynamodb.Table('FinanceArticles')

queue_url = 'https://sqs.us-east-1.amazonaws.com/051011752121/finance-api-to-url-scraper'
alreadySeen = set()

settings = {}
settings['ITEM_PIPELINES'] = {'__main__.ArticleScraperPipeline': 1}

runner = CrawlerRunner(settings=settings)
setup(runner)
reactor.run() # the script will block here until the crawling is finished

