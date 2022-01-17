import json
import pandas as pd

import GPG.mapper_static as gpg_mapper
import Gemweb.mapper_static as gemweb_mapper
import settings
from utils import read_from_kafka, read_config

if __name__ == '__main__':
    config = read_config(settings.conf_file)

    for x in read_from_kafka(config['kafka']['topic'], config["kafka"]['group'], config['kafka']['connection']):
        message = x.value
        dfs = []
        for i, col_type in enumerate(message['collection_type']):
            dfs.append(pd.DataFrame.from_records(message['data'][i]))
        ## todo: Find a way to create a single df.
        df = dfs[0]
        mapper = None
        kwargs_function = {}
        if message['source'] == "gpg":
            mapper = gpg_mapper
            kwargs_function = {
                "organization_name": message['organization_name'],
                "namespace": message['namespace'],
                "user": message['user'],
                "organizations": False,
                "config": config
            }
        elif message['source'] == "gemweb":
            mapper = gemweb_mapper
        else:
            print(f"not implemented type recieved: {message['source']}")
            continue
        print("mapping data")
        mapper.map_data(df.to_dict(orient="records"), **kwargs_function)
