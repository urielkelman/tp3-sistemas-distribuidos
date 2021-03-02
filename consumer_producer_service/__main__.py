import argparse
import logging.config
import os
from datetime import datetime
from multiprocessing import Process
from time import sleep
from typing import Dict, List

from config.load_config import load_config
from pika.exceptions import AMQPConnectionError

from tp2_utils.leader_election.ack_process import AckProcess
from tp2_utils.rabbit_utils.rabbit_consumer_producer import RabbitQueueConsumerProducer

ACK_LISTENING_PORT = 8000


def date_to_weekday(date):
    day_name = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    weekday_i = datetime.strptime(date, "%Y-%m-%d %H:%M:%S").weekday()
    return day_name[weekday_i]


def leq_than_50(number):
    return number >= 50


def leq_than_5(number):
    return number >= 5


def leq_than_1(number):
    return number >= 1


def equal_to_5(number):
    return number == 5


def is_true(boolean):
    return boolean


def consume_func(message_pipeline, item: Dict) -> List[Dict]:
    return message_pipeline.process(item)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )

    ack_process = AckProcess(ACK_LISTENING_PORT, os.getpid())
    ack_process_aux = Process(target=ack_process.run)
    ack_process_aux.start()

    parser = argparse.ArgumentParser(description='Rabbit producer consumer')
    parser.add_argument('--config', help="The config file to use", required=True)
    args = parser.parse_args()
    config = load_config(args.config,
                         {'date_to_weekday': date_to_weekday,
                          'leq_than_50': leq_than_50,
                          'leq_than_5': leq_than_5,
                          'equal_to_5': equal_to_5,
                          'is_true': is_true,
                          'leq_than_1': leq_than_1})
    logger = logging.getLogger()
    consumer = RabbitQueueConsumerProducer(host=config.host, consume_queue=config.consume_from,
                                           response_queues=config.produce_to,
                                           messages_to_group=config.messages_to_group,
                                           callable_commiter=config.message_pipeline, logger=logger,
                                           publisher_sharding=config.publisher_sharding)
    while True:
        try:
            consumer()
        except AMQPConnectionError:
            sleep(2)
            logger.info("Retrying connection to rabbit...")
        except Exception as e:
            logger.exception("Fatal error in consumer")
            ack_process_aux.terminate()
            raise e
