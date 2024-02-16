# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

import json
import yaml

import kafka

from itemadapter import ItemAdapter

with open('config.yaml', 'r') as f:
    config = yaml.load(f, yaml.Loader)


class KafkaPipeline:
    producer = None

    def open_spider(self, spider):
        self.producer = kafka.KafkaProducer(
            bootstrap_servers=config['kafka']["bootstrap_servers"]
        )

    def process_item(self, item, spider):
        self.producer.send(
            "news", json.dumps(ItemAdapter(item).asdict()).encode("utf-8")
        )
        return item

    def close_spider(self, spider):
        self.producer.close()
