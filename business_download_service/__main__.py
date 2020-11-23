import os
import pickle
import socket
from multiprocessing import Process, Semaphore
import signal
from tp2_utils.blocking_socket_transferer import BlockingSocketTransferer
from tp2_utils.message_pipeline.message_pipeline import WINDOW_END_MESSAGE
from tp2_utils.rabbit_utils.rabbit_consumer_producer import RabbitQueueConsumerProducer
from tp2_utils.rabbit_utils.special_messages import BroadcastMessage

BUSINESSES_QUEUE = 'yelp_businesses_news'
BUSINESS_NOTIFY_END = 'notify_business_load_end'
PATH_TO_SAVE_BUSINESSES = "data/businesses.pickle"

port = int(os.getenv('PORT'))
listen_backlog = int(os.getenv('LISTEN_BACKLOG'))
rabbit_host = os.getenv('RABBIT_HOST')
clients = int(os.getenv('CLIENTS'))


class SocketDataDownloader():
    def __init__(self, port, listen_backlog, clients):
        self.port = port
        self.listen_backlog = listen_backlog
        self.process_list = []
        self.clients_to_end = Semaphore(clients - 1)

    def open_socket_for_download(self):
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        while True:
            client_sock = self.__accept_new_connection()
            p = Process(target=self.__handle_client_connection, args=(client_sock,self.clients_to_end, os.getpid()))
            p.start()
            client_sock.close()
            self.process_list = [p for p in self.process_list if p.is_alive()] + [p]

    @staticmethod
    def __handle_client_connection(client_sock, clients_to_end, parent_pid):
        """
        Read message from a specific client socket and closes the socket

        If a problem arises in the communication with the client, the
        client socket will also be closed
        """
        socket_transferer = BlockingSocketTransferer(client_sock)
        try:
            msg = socket_transferer.receive_plain_text()
            if msg == "END":
                # https://bugs.python.org/issue23484
                if not clients_to_end.acquire(block=False):
                    os.kill(parent_pid, signal.SIGTERM)
                socket_transferer.close()
                return
            if msg != "SEND FILE":
                socket_transferer.close()
                return
        except (OSError, TimeoutError) as e:
            socket_transferer.abort()
            return
        socket_transferer.send_file(PATH_TO_SAVE_BUSINESSES)
        socket_transferer.send_plain_text("ALL SENT")
        socket_transferer.close()
        return

    def __accept_new_connection(self):
        """
        Accept new connections

        Function blocks until a connection to a client is made.
        Then connection created is printed and returned
        """
        c, addr = self._server_socket.accept()
        return c


# simple fail if file is not accesible
open(PATH_TO_SAVE_BUSINESSES, 'wb').close()


class DataGatherer:
    def __init__(self):
        self.business_locations = {}

    def gather_business_locations(self, item):
        if item == WINDOW_END_MESSAGE:
            with open(PATH_TO_SAVE_BUSINESSES, 'wb') as business_file:
                pickle.dump(self.business_locations, business_file)
            return [WINDOW_END_MESSAGE], True
        else:
            self.business_locations[item['business_id']] = item['city']
        return [], False


def notify_data_available(item):
    if item == WINDOW_END_MESSAGE:
        return [BroadcastMessage(WINDOW_END_MESSAGE)], False
    return [], False


cp = RabbitQueueConsumerProducer(rabbit_host, BUSINESSES_QUEUE,
                                 [BUSINESS_NOTIFY_END],
                                 DataGatherer().gather_business_locations,
                                 messages_to_group=1)
p = Process(target=cp)
p.start()
p.join()

print("Starting download service")

def empty_queue(item):
    if item == WINDOW_END_MESSAGE:
        return [], True
    return [], False

def close_and_exit():
    print("Stoping downloader service")
    cp = RabbitQueueConsumerProducer(rabbit_host, BUSINESS_NOTIFY_END,
                                     [],
                                     empty_queue,
                                     messages_to_group=1)
    p = Process(target=cp)
    p.start()
    p.join()
    exit(0)

signal.signal(signal.SIGTERM, close_and_exit)

SocketDataDownloader(port, listen_backlog, clients).open_socket_for_download()
