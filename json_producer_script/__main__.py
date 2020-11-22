import json
from tp2_utils.rabbit_utils.rabbit_producer import RabbitQueueProducer
from tp2_utils.rabbit_utils.rabbit_consumer_producer import RabbitQueueConsumerProducer
from tp2_utils.message_pipeline.message_pipeline import WINDOW_END_MESSAGE
from multiprocessing import Process, Pipe
from functools import partial

REVIEWS_QUEUE = 'yelp_reviews_news'
BUSINESSES_QUEUE = 'yelp_businesses_news'
REVIEWS_JSON_PATH = 'data/yelp_academic_dataset_review.json'
BUSINESSES_JSON_PATH = 'data/yelp_academic_dataset_business.json'
MESSAGE_GROUPING = 1000

MORE_THAN_50_REVIEWS_PATH = "data/users_more_than_50_reviews.txt"
MORE_THAN_50_REVIEWS_5_STARS_PATH = "data/users_more_than_50_reviews_5_stars.txt"
MORE_THAN_5_REVIEWS_SAME_TEXT_PATH = "data/users_more_than_5_reviews_same_text.txt"
DAY_FREQ_PATH = "data/day_histogram.txt"

print("Start publishing of businesses")
producer = RabbitQueueProducer(host='rabbit', publish_queue=BUSINESSES_QUEUE)

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
            buffer=[]
        line = json_file.readline()
    if buffer:
        producer.publish(buffer)

producer.publish(WINDOW_END_MESSAGE)
print("End publishing of businesses")

print("Start publishing of reviews")
producer = RabbitQueueProducer(host='rabbit', publish_queue=REVIEWS_QUEUE)

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
            buffer=[]
        line = json_file.readline()
    if buffer:
        producer.publish(buffer)

producer.publish(WINDOW_END_MESSAGE)
print("End publishing of reviews")

class DataGatherer:
    def __init__(self, output_path: str):
        self.file = open(output_path, 'w')

    def gather_users(self, item):
        if item == WINDOW_END_MESSAGE:
            self.file.close()
            return [], True
        else:
            self.file.write("%s\n" % item['user_id'])
            return [], False

    def gather_day_freq(self, item):
        if item == WINDOW_END_MESSAGE:
            self.file.close()
            return [], True
        else:
            self.file.write("%s %d\n" % (item['day'], item['count']))
            return [], False


cp = RabbitQueueConsumerProducer("rabbit", 'yelp_users_50_or_more_reviews',
                                 [],
                                 DataGatherer(MORE_THAN_50_REVIEWS_PATH).gather_users,
                                 messages_to_group=1)
p = Process(target=cp)
p.start()
p.join()
print("The result of users with 50 or more reviews is in %s" % MORE_THAN_50_REVIEWS_PATH)
cp = RabbitQueueConsumerProducer("rabbit", 'yelp_users_50_or_more_reviews_and_5_stars',
                                 [],
                                 DataGatherer(MORE_THAN_50_REVIEWS_5_STARS_PATH).gather_users,
                                 messages_to_group=1)
p = Process(target=cp)
p.start()
p.join()
print("The result of users with 50 or more reviews and all 5 stars is in %s" % MORE_THAN_50_REVIEWS_5_STARS_PATH)
cp = RabbitQueueConsumerProducer("rabbit", 'yelp_users_5_or_more_reviews_and_same_text',
                                 [],
                                 DataGatherer(MORE_THAN_5_REVIEWS_SAME_TEXT_PATH).gather_users,
                                 messages_to_group=1)
p = Process(target=cp)
p.start()
p.join()
print("The result of users with 5 or more reviews and same text is in %s" % MORE_THAN_5_REVIEWS_SAME_TEXT_PATH)
cp = RabbitQueueConsumerProducer("rabbit", 'yelp_reviews_histogram_finished',
                                 [],
                                 DataGatherer(DAY_FREQ_PATH).gather_day_freq,
                                 messages_to_group=1)
p = Process(target=cp)
p.start()
p.join()
print("Review's counts by day is in %s" % DAY_FREQ_PATH)

