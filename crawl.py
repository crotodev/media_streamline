from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
import nltk

from news_scraper.spiders.cnn import CNNSpider
from news_scraper.spiders.foxnews import FoxNewsSpider
from news_scraper.spiders.nbcnews import NBCNewsSpider

nltk.download("punkt")

process = CrawlerProcess(settings=get_project_settings())
process.crawl(FoxNewsSpider)
process.crawl(NBCNewsSpider)
process.crawl(CNNSpider)
process.start()
