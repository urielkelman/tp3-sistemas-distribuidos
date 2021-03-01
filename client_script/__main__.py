import argparse
import json
from datetime import datetime
from multiprocessing import Process

from tp2_utils.interfaces.dummy_state_commiter import DummyStateCommiter
from tp2_utils.message_pipeline.message_pipeline import message_is_end, WINDOW_END_MESSAGE
from tp2_utils.rabbit_utils.rabbit_consumer_producer import RabbitQueueConsumerProducer
from tp2_utils.rabbit_utils.rabbit_producer import RabbitQueueProducer

REVIEWS_QUEUE = 'yelp_reviews_news'
BUSINESSES_QUEUE = 'yelp_businesses_news'
MESSAGE_GROUPING = 100000
RABBIT_HOST = 'localhost'

MORE_THAN_50_REVIEWS_PATH = "users_more_than_50_reviews.txt"
MORE_THAN_50_REVIEWS_5_STARS_PATH = "users_more_than_50_reviews_5_stars.txt"
MORE_THAN_5_REVIEWS_SAME_TEXT_PATH = "users_more_than_5_reviews_same_text.txt"
DAY_FREQ_PATH = "day_histogram.txt"
FUNNY_CITIES_PATH = "top_10_funny_cities.txt"

parser = argparse.ArgumentParser(description='Tp2 client')
parser.add_argument('--reviews_path', help="path to reviews", required=True)
parser.add_argument('--business_path', help="path to businesses", required=True)
args = parser.parse_args()
REVIEWS_JSON_PATH = args.reviews_path
BUSINESSES_JSON_PATH = args.business_path


def print_w_timestamp(text):
    print("%s - %s" % (datetime.now().isoformat(), text))


print_w_timestamp("Start publishing of businesses")
producer = RabbitQueueProducer(host=RABBIT_HOST, publish_queue=BUSINESSES_QUEUE)

with open(BUSINESSES_JSON_PATH) as json_file:
    buffer = []
    line = json_file.readline()
    while line:
        item = json.loads(line)
        if not item:
            line = json_file.readline()
            continue
        buffer.append(item)
        if len(buffer) >= MESSAGE_GROUPING:
            producer.publish(buffer)
            buffer = []
        line = json_file.readline()
    if buffer:
        producer.publish(buffer)

producer.publish(WINDOW_END_MESSAGE)
producer.close()
print_w_timestamp("End publishing of businesses")

print_w_timestamp("Start publishing of reviews")
producer = RabbitQueueProducer(host=RABBIT_HOST, publish_queue=REVIEWS_QUEUE)

with open(REVIEWS_JSON_PATH) as json_file:
    buffer = []
    line = json_file.readline()
    while line:
        item = json.loads(line)
        if not item:
            line = json_file.readline()
            continue
        buffer.append(item)
        if len(buffer) >= MESSAGE_GROUPING:
            producer.publish(buffer)
            buffer = []
        line = json_file.readline()
    if buffer:
        producer.publish(buffer)

producer.publish(WINDOW_END_MESSAGE)
producer.close()
print_w_timestamp("End publishing of reviews")


class DataGatherer:
    def __init__(self, output_path: str):
        self.file = open(output_path, 'w')

    def gather_users(self, item):
        if message_is_end(item):
            self.file.close()
            return [], True
        else:
            self.file.write("%s\n" % item['user_id'])
            return [], False

    def gather_day_freq(self, item):
        if message_is_end(item):
            self.file.close()
            return [], True
        else:
            self.file.write("%s %d\n" % (item['day'], item['count']))
            return [], False

    def gather_funny_cities(self, item):
        if message_is_end(item):
            self.file.close()
            return [], True
        else:
            self.file.write("%s %d\n" % (item['city'], item['count']))
            return [], False


cp = RabbitQueueConsumerProducer(RABBIT_HOST, 'yelp_users_50_or_more_reviews',
                                 [],
                                 DummyStateCommiter(DataGatherer(MORE_THAN_50_REVIEWS_PATH).gather_users),
                                 messages_to_group=1)
p = Process(target=cp)
p.start()
p.join()
print_w_timestamp("The result of users with 50 or more reviews are in %s" % MORE_THAN_50_REVIEWS_PATH)
cp = RabbitQueueConsumerProducer(RABBIT_HOST, 'yelp_users_50_or_more_reviews_and_5_stars',
                                 [],
                                 DummyStateCommiter(DataGatherer(MORE_THAN_50_REVIEWS_5_STARS_PATH).gather_users),
                                 messages_to_group=1)
p = Process(target=cp)
p.start()
p.join()
print_w_timestamp(
    "The result of users with 50 or more reviews and all 5 stars are in %s" % MORE_THAN_50_REVIEWS_5_STARS_PATH)
cp = RabbitQueueConsumerProducer(RABBIT_HOST, 'yelp_users_5_or_more_reviews_and_same_text',
                                 [],
                                 DummyStateCommiter(DataGatherer(MORE_THAN_5_REVIEWS_SAME_TEXT_PATH).gather_users),
                                 messages_to_group=1)
p = Process(target=cp)
p.start()
p.join()
print_w_timestamp(
    "The result of users with 5 or more reviews and same text are in %s" % MORE_THAN_5_REVIEWS_SAME_TEXT_PATH)
cp = RabbitQueueConsumerProducer(RABBIT_HOST, 'yelp_reviews_histogram_finished',
                                 [],
                                 DummyStateCommiter(DataGatherer(DAY_FREQ_PATH).gather_day_freq),
                                 messages_to_group=1)
p = Process(target=cp)
p.start()
p.join()
print_w_timestamp("Review's counts by day are in %s" % DAY_FREQ_PATH)
cp = RabbitQueueConsumerProducer(RABBIT_HOST, 'yelp_top_10_funny_cities',
                                 [],
                                 DummyStateCommiter(DataGatherer(FUNNY_CITIES_PATH).gather_funny_cities),
                                 messages_to_group=1)
p = Process(target=cp)
p.start()
p.join()
print_w_timestamp("Top 10 funny cities are in %s" % FUNNY_CITIES_PATH)
