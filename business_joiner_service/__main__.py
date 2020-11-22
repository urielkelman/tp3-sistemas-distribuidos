from tp2_utils.rabbit_utils.rabbit_consumer_producer import RabbitQueueConsumerProducer
from tp2_utils.rabbit_utils.rabbit_producer import RabbitQueueProducer
from tp2_utils.message_pipeline.message_pipeline import WINDOW_END_MESSAGE
from tp2_utils.blocking_socket_transferer import BlockingSocketTransferer
from functools import partial
from multiprocessing import Process
import socket
import os
import pickle

BUSINESS_NOTIFY_END = 'notify_business_load_end'
PATH_TO_SAVE_BUSINESSES = "tmp/businesses.pickle"

downloader_host = os.getenv('DOWNLOADER_HOST')
downloader_port = int(os.getenv('DOWNLOADER_PORT'))
join_from_queue = os.getenv('JOIN_FROM_QUEUE')
output_joined_queue = os.getenv('OUTPUT_JOINED_QUEUE')

print("Waiting for downloader to be ready")

def wait_for_file_ready(item):
    if item == WINDOW_END_MESSAGE:
        return [WINDOW_END_MESSAGE], True
    return [], False

cp = RabbitQueueConsumerProducer("rabbit", BUSINESS_NOTIFY_END,
                                 [BUSINESS_NOTIFY_END],
                                 wait_for_file_ready,
                                 messages_to_group=1)
p = Process(target=cp)
p.start()
p.join()

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

def add_location_to_businesses(business_locations, item):
    if item == WINDOW_END_MESSAGE:
        return [], True
    item['city'] = business_locations[item['business_id']]
    return [item], False

print("Starting consumer to join")

cp = RabbitQueueConsumerProducer("rabbit", join_from_queue,
                                 [output_joined_queue],
                                 partial(add_location_to_businesses, business_locations),
                                 messages_to_group=1000)
p = Process(target=cp)
p.start()
p.join()