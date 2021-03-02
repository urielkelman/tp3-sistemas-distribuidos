import logging
import os
import pickle
import socket
from functools import partial
from multiprocessing import Process
from pathlib import Path
from time import sleep
from typing import Optional, NoReturn, Any, Callable

from pika.exceptions import AMQPConnectionError

from tp2_utils.blocking_socket_transferer import BlockingSocketTransferer
from tp2_utils.interfaces.dummy_state_commiter import DummyStateCommiter
from tp2_utils.interfaces.state_commiter import StateCommiter
from tp2_utils.leader_election.ack_process import AckProcess
from tp2_utils.message_pipeline.message_pipeline import WINDOW_END_MESSAGE, message_is_end
from tp2_utils.rabbit_utils.rabbit_consumer_producer import RabbitQueueConsumerProducer
from tp2_utils.rabbit_utils.special_messages import BroadcastMessage

BUSINESS_NOTIFY_END = 'notify_business_load_end'
BUSINESSES_READY_PATH = "%s/DOWNLOAD_READY"
BUSINESSES_DONE_PATH = "%s/DOWNLOAD_DONE"
JOINING_DONE_PATH = "%s/JOINING_DONE"
ENDED_PATH = "%s/ENDED"
PATH_TO_SAVE_BUSINESSES = "%s/businesses.pickle"
PATH_TO_COMMITS_NUMBERS = "%s/COMMITS_NUMBERS"

PATH_TO_MESSAGE_SET = "%s/message_set"

ACK_LISTENING_PORT = 8000

logger = logging.getLogger()


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


class BusinessJoiner(StateCommiter):
    def __init__(self, func: Callable, idempotency_set, data_path):
        super().__init__()
        self.func = func
        self.idempotency_set = idempotency_set
        self.flush_at_next_commit = False
        self.commit_number = 1
        self.data_path = data_path
        self.recover_state()

    def prepare(self, data: Any) -> Any:
        """
        Applies the function to the data
        :param data: the data
        """
        if self.idempotency_set and data and data in self.idempotency_set:
            return [], False
        if self.idempotency_set and data:
            self.idempotency_set.prepare(data)
        if message_is_end(data):
            self.flush_at_next_commit = True
        return self.func(data)

    def flush(self) -> NoReturn:
        """
        Flushes all the state
        """
        self.idempotency_set.flush()

    def commit(self) -> int:
        """
        Commits the prepared changes
        :return: a commit number
        """
        cn = self.commit_number
        if self.idempotency_set:
            cn = self.idempotency_set.commit()
            self.commit_number = cn
        if os.path.exists(PATH_TO_COMMITS_NUMBERS % self.data_path):
            file = open(PATH_TO_COMMITS_NUMBERS % self.data_path, "a")
        else:
            file = open(PATH_TO_COMMITS_NUMBERS % self.data_path, "w")
        file.write(str(cn) + '\n')
        file.close()
        self.commit_number += 1
        return cn

    def recover_state(self, commit_number: Optional[int] = None):
        """
        Recovers the state, the last one or a determined one

        :param commit_number: the commit number from which to restore the state
        """
        if os.path.exists(PATH_TO_COMMITS_NUMBERS % self.data_path):
            file = open(PATH_TO_COMMITS_NUMBERS % self.data_path, "r")
            commits = []
            line = file.readline()
            while line:
                try:
                    commits.append(int(line))
                except Exception:
                    pass
                line = file.readline()
            self.idempotency_set.recover_state(max(commits))
        else:
            self.idempotency_set.recover_state()

    def commit_done_cleanup(self):
        """
        Cleans up after commit
        """
        if self.flush_at_next_commit:
            self.flush()
            self.flush_at_next_commit = False


def run_process(downloader_host, downloader_port,
                join_from_queue, output_joined_queue,
                rabbit_host,
                data_path="data"):
    while True:
        if not os.path.exists(BUSINESSES_READY_PATH % data_path):
            logger.info("Waiting for downloader to be ready")

            cp = RabbitQueueConsumerProducer(rabbit_host, BUSINESS_NOTIFY_END,
                                             [BUSINESS_NOTIFY_END],
                                             DummyStateCommiter(partial(wait_for_file_ready, data_path)),
                                             messages_to_group=1, logger=logger)
            cp()
        logger.info("Downloader is ready")

        if not os.path.exists(BUSINESSES_DONE_PATH % data_path):
            while True:
                try:
                    logger.info("Downloading file")

                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.connect((downloader_host, downloader_port))
                    socket_transferer = BlockingSocketTransferer(sock)
                    socket_transferer.send_plain_text("SEND FILE")
                    with open(PATH_TO_SAVE_BUSINESSES % data_path, "wb") as business_file:
                        socket_transferer.receive_file_data(business_file)
                    socket_transferer.receive_plain_text()
                    socket_transferer.close()
                    Path(BUSINESSES_DONE_PATH % data_path).touch()
                    break
                except Exception as e:
                    logger.exception("Excception while downloading businesses")
                    sleep(1)

        logger.info("File is downloaded")
        logger.info("Starting consumer to join")
        if not os.path.exists(JOINING_DONE_PATH % data_path):
            with open(PATH_TO_SAVE_BUSINESSES % data_path, "rb") as business_file:
                business_locations = pickle.load(business_file)
            joiner_state_commiter = DummyStateCommiter(
                partial(add_location_to_businesses, business_locations, data_path))
            cp = RabbitQueueConsumerProducer(rabbit_host, join_from_queue,
                                             [output_joined_queue],
                                             joiner_state_commiter,
                                             messages_to_group=1000, logger=logger)
            cp()
        logger.info("Ending stream")
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
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )

    ack_process = AckProcess(ACK_LISTENING_PORT, os.getpid())
    ack_process_aux = Process(target=ack_process.run)
    ack_process_aux.start()
    logger.info("Starting business joiner")
    downloader_host = os.getenv('DOWNLOADER_HOST')
    downloader_port = int(os.getenv('DOWNLOADER_PORT'))
    join_from_queue = os.getenv('JOIN_FROM_QUEUE')
    output_joined_queue = os.getenv('OUTPUT_JOINED_QUEUE')
    rabbit_host = os.getenv('RABBIT_HOST')
    while True:
        try:
            logger.info("Running business joiner service")
            run_process(downloader_host, downloader_port, join_from_queue,
                        output_joined_queue, rabbit_host)
        except AMQPConnectionError:
            sleep(2)
            logger.info("Retrying connection to rabbit...")
        except Exception as e:
            logger.exception("Fatal error in consumer")
            ack_process_aux.terminate()
            raise e
