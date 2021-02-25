import os
import pickle
import socket
from functools import partial
from multiprocessing import Process

from tp2_utils.blocking_socket_transferer import BlockingSocketTransferer
from tp2_utils.message_pipeline.message_pipeline import WINDOW_END_MESSAGE
from tp2_utils.rabbit_utils.rabbit_consumer_producer import RabbitQueueConsumerProducer
from tp2_utils.rabbit_utils.special_messages import BroadcastMessage
from tp2_utils.interfaces.dummy_state_commiter import DummyStateCommiter

BUSINESS_NOTIFY_END = 'notify_business_load_end'
BUSINESSES_READY_PATH = "data/DOWNLOAD_READY"
PATH_TO_SAVE_BUSINESSES = "data/businesses.pickle"

downloader_host = os.getenv('DOWNLOADER_HOST')
downloader_port = int(os.getenv('DOWNLOADER_PORT'))
join_from_queue = os.getenv('JOIN_FROM_QUEUE')
output_joined_queue = os.getenv('OUTPUT_JOINED_QUEUE')
rabbit_host = os.getenv('RABBIT_HOST')

def wait_for_file_ready(item):
    if item == WINDOW_END_MESSAGE:
        open(BUSINESSES_READY_PATH, "wb")
        return [], True
    return [], False

def add_location_to_businesses(business_locations, item):
    if item == WINDOW_END_MESSAGE:
        return [BroadcastMessage(WINDOW_END_MESSAGE)], True
    item['city'] = business_locations[item['business_id']]
    return [item], False

while True:
    if not os.path.exists(BUSINESSES_READY_PATH):
        print("Waiting for downloader to be ready")

        cp = RabbitQueueConsumerProducer(rabbit_host, BUSINESS_NOTIFY_END,
                                         [BUSINESS_NOTIFY_END],
                                         DummyStateCommiter(wait_for_file_ready),
                                         messages_to_group=1)
        cp()

    print("Downloading file")

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((downloader_host, downloader_port))
    socket_transferer = BlockingSocketTransferer(sock)
    socket_transferer.send_plain_text("SEND FILE")
    with open(PATH_TO_SAVE_BUSINESSES, "wb") as business_file:
        socket_transferer.receive_file_data(business_file)
    socket_transferer.receive_plain_text()
    socket_transferer.close()

    with open(PATH_TO_SAVE_BUSINESSES, "rb") as business_file:
        business_locations = pickle.load(business_file)

    print("Starting consumer to join")

    cp = RabbitQueueConsumerProducer(rabbit_host, join_from_queue,
                                     [output_joined_queue],
                                     DummyStateCommiter(partial(add_location_to_businesses, business_locations)),
                                     messages_to_group=1000)
    cp()

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((downloader_host, downloader_port))
    socket_transferer = BlockingSocketTransferer(sock)
    socket_transferer.send_plain_text("END")
    socket_transferer.close()
    os.remove(BUSINESSES_READY_PATH)
