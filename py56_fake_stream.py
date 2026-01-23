import datetime
import logging
import time
from pathlib import Path
from random import randint, random

import numpy as np


def build_consumption_event():
    consumer = np.random.choice(['Watson', 'Skinner',
                                 "Zimbardo", "Pavlow"],
                                replace=False, p=[0.2, 0.3, 0.35, 0.15])
    consum = randint(1, 30), datetime.datetime.now(), random() * 0.3 * len(consumer) / 6 + 1, consumer
    return consum


STREAM_PATH = Path('.', 'tmp_data', 'streaming_demo')

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    logging.info("Start stream")
    data_path = STREAM_PATH
    if not data_path.exists():
        data_path.mkdir(parents=True)
    for y in range(10000):
        content = "\n".join(["\t".join([str(element)
                                        for element in build_consumption_event()])
                             for i in range(10)])
        file_path = data_path / Path(f'beer_consumptions_{y}.csv')
        file_path.write_text(content)
        logging.info(f"Added streaming batch to {file_path}")
        time.sleep(10)
