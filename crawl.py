import time

import yaml
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
import nltk
import schedule

from news_scraper.spiders.cnn import CNNSpider
from news_scraper.spiders.foxnews import FoxNewsSpider
from news_scraper.spiders.nbcnews import NBCNewsSpider
from utils.kafka import create_kafka_topic

nltk.download("punkt")

with open('config.yaml', 'r') as f:
    config = yaml.load(f, yaml.Loader)

create_kafka_topic(
    "news",
    num_partitions=config['kafka']['num_partitions'],
    replication_factor=config['kafka']['replication_factor'],
    broker={"bootstrap_servers": config['kafka']["bootstrap_servers"]},
)


def crawl() -> None:
    """
    Starts the crawler process for the spiders
    """
    process = CrawlerProcess(settings=get_project_settings())
    process.crawl(FoxNewsSpider)
    process.crawl(NBCNewsSpider)
    process.crawl(CNNSpider)
    process.start()


# first initial crawl
crawl()

# set up the crawl to happen every hour
schedule.every().hour.do(crawl)

# loop to run the crawl job
while True:
    schedule.run_pending()
    time.sleep(1)
