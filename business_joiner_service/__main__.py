import os
import pickle
import socket
from functools import partial
from pathlib import Path
import logging
from tp2_utils.blocking_socket_transferer import BlockingSocketTransferer
from tp2_utils.message_pipeline.message_pipeline import WINDOW_END_MESSAGE, message_is_end
from tp2_utils.rabbit_utils.rabbit_consumer_producer import RabbitQueueConsumerProducer
from tp2_utils.rabbit_utils.special_messages import BroadcastMessage
from tp2_utils.interfaces.dummy_state_commiter import DummyStateCommiter
from time import sleep
from multiprocessing import Process

BUSINESS_NOTIFY_END = 'notify_business_load_end'
BUSINESSES_READY_PATH = "%s/DOWNLOAD_READY"
BUSINESSES_DONE_PATH = "%s/DOWNLOAD_DONE"
JOINING_DONE_PATH = "%s/JOINING_DONE"
ENDED_PATH = "%s/ENDED"
PATH_TO_SAVE_BUSINESSES = "%s/businesses.pickle"

logger = logging.getLogger("root")

def wait_for_file_ready(data_path, item):
    if message_is_end(item):
        Path(BUSINESSES_READY_PATH % data_path).touch()
        return [], True
    return [], False

def add_location_to_businesses(business_locations, data_path, item):
    if message_is_end(item):
        Path(JOINING_DONE_PATH % data_path).touch()
        return [BroadcastMessage(WINDOW_END_MESSAGE)], True
    item['city'] = business_locations[item['business_id']]
    return [item], False

def run_process(downloader_host, downloader_port,
                join_from_queue, output_joined_queue,
                rabbit_host,
                data_path = "data"):
    while True:
        if not os.path.exists(BUSINESSES_READY_PATH % data_path):
            print("Waiting for downloader to be ready")

            cp = RabbitQueueConsumerProducer(rabbit_host, BUSINESS_NOTIFY_END,
                                             [BUSINESS_NOTIFY_END],
                                             DummyStateCommiter(partial(wait_for_file_ready, data_path)),
                                             messages_to_group=1, logger=logger)
            cp()

        if not os.path.exists(BUSINESSES_DONE_PATH % data_path):
            while True:
                try:
                    print("Downloading file")

                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.connect((downloader_host, downloader_port))
                    socket_transferer = BlockingSocketTransferer(sock)
                    socket_transferer.send_plain_text("SEND FILE")
                    with open(PATH_TO_SAVE_BUSINESSES % data_path, "wb") as business_file:
                        socket_transferer.receive_file_data(business_file)
                    socket_transferer.receive_plain_text()
                    socket_transferer.close()

                    with open(PATH_TO_SAVE_BUSINESSES % data_path, "rb") as business_file:
                        business_locations = pickle.load(business_file)
                    Path(BUSINESSES_DONE_PATH % data_path).touch()
                    break
                except Exception as e:
                    logger.exception("Excception while downloading businesses")
                    sleep(1)

        print("Starting consumer to join")
        if not os.path.exists(JOINING_DONE_PATH % data_path):
            with open(PATH_TO_SAVE_BUSINESSES % data_path, "rb") as business_file:
                business_locations = pickle.load(business_file)
            cp = RabbitQueueConsumerProducer(rabbit_host, join_from_queue,
                                             [output_joined_queue],
                                             DummyStateCommiter(partial(add_location_to_businesses,
                                                                        business_locations, data_path)),
                                             messages_to_group=1000, logger=logger)
            cp()
        if not os.path.exists(ENDED_PATH % data_path):
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((downloader_host, downloader_port))
            socket_transferer = BlockingSocketTransferer(sock)
            socket_transferer.send_plain_text("END")
            assert socket_transferer.receive_plain_text() == "REGISTERED"
            Path(ENDED_PATH % data_path).touch()
            socket_transferer.close()
        os.remove(BUSINESSES_READY_PATH % data_path)
        os.remove(BUSINESSES_DONE_PATH % data_path)
        os.remove(JOINING_DONE_PATH % data_path)
        os.remove(ENDED_PATH % data_path)

if __name__ == "__main__":
    downloader_host = os.getenv('DOWNLOADER_HOST')
    downloader_port = int(os.getenv('DOWNLOADER_PORT'))
    join_from_queue = os.getenv('JOIN_FROM_QUEUE')
    output_joined_queue = os.getenv('OUTPUT_JOINED_QUEUE')
    rabbit_host = os.getenv('RABBIT_HOST')
    run_process(downloader_host, downloader_port, join_from_queue,
                output_joined_queue, rabbit_host)