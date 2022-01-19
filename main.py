import json
from datetime import datetime

import pandas as pd

import GPG.mapper_static as gpg_mapper
import Gemweb.mapper_static as gemweb_mapper
import Datadis.mapper_static as datadis_mapper
import EEM.mapper as eem_mapper
import settings
from utils import read_from_kafka, read_config, mongo_logger

if __name__ == '__main__':
    config = read_config(settings.conf_file)
    for x in read_from_kafka(config['kafka']['topic'], config["kafka"]['group'], config['kafka']['connection']):
        message = x.value
        df = pd.DataFrame.from_records(message['data'])
        mapper = None
        kwargs_function = {}
        if 'logger' in message:
            mongo_logger.import_log(message['logger'], "harmonize")
        message_part = ""
        if 'message_part' in message:
            message_part = message['message_part']
        mongo_logger.log(f"received part {message_part} from {message['source']} to harmonize")
        if message['source'] == "gpg":
            mapper = gpg_mapper
            kwargs_function = {
                "namespace": message['namespace'],
                "user": message['user'],
                "organizations": True,
                "config": config
            }
        elif message['source'] == "gemweb":
            mapper = gemweb_mapper
            kwargs_function = {
                "namespace": message['namespace'],
                "user": message['user'],
                "config": config
            }
        elif message['source'] == "datadis":
            mapper = datadis_mapper
            kwargs_function = {
                "namespace": message['namespace'],
                "user": message['user'],
                "config": config,
                "source": message['source']
            }
        elif message['source'] == "genercat":
            mapper = eem_mapper
            kwargs_function = {
                "namespace": message['namespace'],
                "user": message['user'],
                "config": config,
                "source": message['source']
            }
        else:
            mongo_logger.log(f"not implemented type received: {message['source']}")
            print(f"not implemented type received: {message['source']}")
            continue
        print("mapping data")
        try:
            mapper.map_data(df.to_dict(orient="records"), **kwargs_function)
            mongo_logger.log(f"part {message_part} from {message['source']} harmonized successfully")
        except Exception as e:
            mongo_logger.log(f"part {message_part} from {message['source']} harmonized error: {e}")
