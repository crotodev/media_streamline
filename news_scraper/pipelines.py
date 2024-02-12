# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

import json

import kafka

from news_scraper.utils.kafka import create_kafka_topic

# useful for handling different item types with a single interface
from itemadapter import ItemAdapter


class KafkaPipeline:
    producer = None
    broker = {"bootstrap_servers": "localhost:9092"}
    num_partitions = 1
    replication_factor = 1

    def open_spider(self, spider):
        create_kafka_topic(
            "news",
            num_partitions=self.num_partitions,
            replication_factor=self.replication_factor,
            broker=self.broker,
        )
        self.producer = kafka.KafkaProducer(
            bootstrap_servers=self.broker["bootstrap_servers"]
        )

    def process_item(self, item, spider):
        self.producer.send(
            "news", json.dumps(ItemAdapter(item).asdict()).encode("utf-8")
        )
        return item
