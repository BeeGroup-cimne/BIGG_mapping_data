import json
import pandas as pd

import GPG.mapper_static as gpg_mapper
import Gemweb.mapper_static as gemweb_mapper
from utils import read_from_kafka

if __name__ == '__main__':
    with open("config.json") as f:
        config = json.load(f)
        config['neo4j']['auth'] = tuple(config['neo4j']['auth'])

    for x in read_from_kafka('raw-harmonize', "group_harmonize", config['kafka']):
        message = x.value
        df = pd.DataFrame.from_records(message['data'])
        mapper = None
        if message['source'] == "gpg":
            mapper = gpg_mapper
        elif message['source'] == "gemweb":
            mapper = gemweb_mapper

        mapper.map_data(message['data'], message['organization_name'], message['namespace'],
                        message['user'], False)
