import os
import pickle
import socket
from pathlib import Path
from tp2_utils.blocking_socket_transferer import BlockingSocketTransferer
from tp2_utils.message_pipeline.message_pipeline import WINDOW_END_MESSAGE, message_is_end
from tp2_utils.rabbit_utils.rabbit_consumer_producer import RabbitQueueConsumerProducer
from tp2_utils.rabbit_utils.special_messages import BroadcastMessage
from tp2_utils.interfaces.dummy_state_commiter import DummyStateCommiter
from multiprocessing import Process
import json
import logging

BUSINESSES_QUEUE = 'yelp_businesses_news'
BUSINESS_NOTIFY_END = 'notify_business_load_end'
PATH_TO_SAVE_BUSINESSES = "%s/businesses.pickle"
PATH_TO_SAVE_LOGFILE = '%s/logfile'
BUSINESSES_READY = '%s/BUSINESSES_READY'

logger = logging.getLogger("root")


class SocketDataDownloader():
    def __init__(self, port, listen_backlog, clients, data_path):
        self.port = port
        self.listen_backlog = listen_backlog
        self.clients_to_end = clients
        self.clients_ended = 0
        self.data_path = data_path
        self.process_list = []
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)

    def start_download_listening(self):
        while self.clients_ended < self.clients_to_end:
            client_sock = self.__accept_new_connection()
            self.__handle_client_connection(client_sock)
        self.clients_ended = 0

    def close(self):
        self._server_socket.close()

    def __handle_client_connection(self, client_sock):
        """
        Read message from a specific client socket and closes the socket

        If a problem arises in the communication with the client, the
        client socket will also be closed
        """
        socket_transferer = BlockingSocketTransferer(client_sock)
        try:
            msg = socket_transferer.receive_plain_text()
            if msg == "END":
                self.clients_ended += 1
                socket_transferer.close()
                return
            if msg != "SEND FILE":
                socket_transferer.close()
                return
        except (OSError, TimeoutError) as e:
            socket_transferer.abort()
            return
        socket_transferer.send_file(PATH_TO_SAVE_BUSINESSES % self.data_path)
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

class DataGatherer:
    def __init__(self, data_path):
        self.business_locations = {}
        self.logfile = None
        self.data_path = data_path
        self._restore_and_open_logfile()

    def _restore_and_open_logfile(self):
        if os.path.exists(PATH_TO_SAVE_LOGFILE % self.data_path):
            logfile = open(PATH_TO_SAVE_LOGFILE % self.data_path, "r")
            line = logfile.readline()
            while line:
                try:
                    item = json.loads(line)
                    self.business_locations[item['business_id']] = item['city']
                except Exception:
                    pass
                line = logfile.readline()
            logfile.close()
        self.logfile = open(PATH_TO_SAVE_LOGFILE % self.data_path, "w")

    def gather_business_locations(self, item):
        if message_is_end(item):
            with open(PATH_TO_SAVE_BUSINESSES % self.data_path, 'wb') as business_file:
                pickle.dump(self.business_locations, business_file)
            Path(BUSINESSES_READY % self.data_path).touch()
            self.logfile.close()
            os.remove(PATH_TO_SAVE_LOGFILE % self.data_path)
            return [BroadcastMessage(WINDOW_END_MESSAGE)], True
        else:
            self.logfile.write("%s\n" % json.dumps(item))
            self.logfile.flush()
            self.business_locations[item['business_id']] = item['city']
        return [], False


def notify_data_available(item):
    if message_is_end(item):
        return [BroadcastMessage(WINDOW_END_MESSAGE)], False
    return [], False

def empty_queue(item):
    if item == WINDOW_END_MESSAGE:
        return [], True
    return [], False

def run_process(port, listen_backlog, rabbit_host, clients,
                data_path = "data"):
    # simple fail if file is not accesible
    open(PATH_TO_SAVE_BUSINESSES % data_path, 'wb').close()
    socket_downloader = SocketDataDownloader(port, listen_backlog, clients, data_path)
    while True:
        if not os.path.exists(BUSINESSES_READY % data_path):
            print("Consuming businesses")
            data_gatherer = DataGatherer(data_path)
            cp = RabbitQueueConsumerProducer(rabbit_host, BUSINESSES_QUEUE,
                                             [BUSINESS_NOTIFY_END],
                                             DummyStateCommiter(data_gatherer.gather_business_locations),
                                             messages_to_group=1, logger=logger)
            try:
                cp()
            except Exception as e:
                logger.exception("Error while consuming businesses")
                raise e

        try:
            print("Starting download service")
            socket_downloader.start_download_listening()
        except Exception as e:
            logger.exception("Error accepting connections for downloading")
            raise e
        print("Stoping downloader service")
        os.remove(BUSINESSES_READY % data_path)

if __name__ == "__main__":
    port = int(os.getenv('PORT'))
    listen_backlog = int(os.getenv('LISTEN_BACKLOG'))
    rabbit_host = os.getenv('RABBIT_HOST')
    clients = int(os.getenv('CLIENTS'))
    run_process(port, listen_backlog, rabbit_host, clients)