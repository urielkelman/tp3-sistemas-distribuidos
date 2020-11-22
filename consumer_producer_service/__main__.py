from typing import Dict, List
from config.load_config import load_config
from tp2_utils.rabbit_utils.rabbit_consumer_producer import RabbitQueueConsumerProducer
from functools import partial
import argparse
from datetime import datetime
import logging.config

def date_to_weekday(date):
    day_name = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    weekday_i = datetime.strptime(date, "%Y-%m-%d %H:%M:%S").weekday()
    return day_name[weekday_i]

def leq_than_50(number):
    return number >= 50

def leq_than_5(number):
    return number >= 5

def equal_to_5(number):
    return number==5

def is_true(boolean):
    return boolean

def is_1(number):
    return number == 1

def consume_func(message_pipeline, item: Dict) -> List[Dict]:
    return message_pipeline.process(item)

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )
    parser = argparse.ArgumentParser(description='Rabbit producer consumer')
    parser.add_argument('--config', help="The config file to use", required=True)
    args = parser.parse_args()
    config = load_config(args.config,
                         {'date_to_weekday': date_to_weekday,
                          'leq_than_50': leq_than_50,
                          'leq_than_5': leq_than_5,
                          'equal_to_5': equal_to_5,
                          'is_true': is_true,
                          'is_1': is_1})
    consume_func = partial(consume_func, config.message_pipeline)
    consumer = RabbitQueueConsumerProducer(host=config.host,consume_queue=config.consume_from,
                                           response_queues=config.produce_to,
                                           messages_to_group=config.messages_to_group,
                                           consume_func=consume_func, logger=logging.getLogger('root'),
                                           publisher_sharding=config.publisher_sharding)
    consumer()
