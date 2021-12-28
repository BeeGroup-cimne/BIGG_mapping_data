import json

import pandas as pd

import GPG.__main__
from utils import read_from_kafka

if __name__ == '__main__':
    with open("config.json") as f:
        config = json.load(f)

    for x in read_from_kafka('harmonize-raw-data', config['kafka']):
        message = x.value
        if message['source'] == "gpg":
            df = pd.DataFrame.from_records(message['data'])
            GPG.__main__.map_data(message['data'], message['organization_name'], message['namespace'],
                                  message['user'], False)
