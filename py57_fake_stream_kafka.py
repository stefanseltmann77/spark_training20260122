"""if not yet running:
/usr/local/kafka/kafka_2.13-3.1.1/bin/zookeeper-server-start.sh /usr/local/kafka/kafka_2.13-3.1.1/config/zookeeper.properties
/usr/local/kafka/kafka_2.13-3.1.1/bin/kafka-server-start.sh /usr/local/kafka/kafka_2.13-3.1.1/config/server.properties
"""
import datetime
import json
from random import randint, random
from time import sleep
from typing import Any

import numpy as np
from confluent_kafka import Producer


def build_consumption_event() -> dict[str, Any]:
    drinker = ['Watson', 'Skinner', 'Zimbardo', 'Pavlow']
    consumer = np.random.choice(drinker,
                                replace=False, p=[0.2, 0.3, 0.35, 0.15])
    # in this case every one orders for the others
    consum = {'drinks': [{'beer_id': randint(1, 30),
                          'volume': round(random() * 0.6 + 0.7, 4)}
                         for i in range(len(drinker))],
              'trink_dts': str(datetime.datetime.now()),
              'consumer': consumer}
    return consum


TOPIC_NAME: str = "beerhall"

if __name__ == '__main__':
    p = Producer({'bootstrap.servers': 'ip-172-31-44-96.eu-central-1.compute.internal:9092'})
    for i in range(10000):
        sleep(randint(1, 4))
        event = build_consumption_event()
        p.produce(TOPIC_NAME, value=json.dumps(event))