#!/usr/bin/env python

'''
Usage:
    tksend --config <config_file> --topic <topic> --format <format> [--datafile <file>]
 
'''

import os, sys
import uuid
import json
import pickle
import datetime
from snap import snap
from snap import common
from mercury import journaling as jrnl
import docopt
from telekast import core as tkcore
from pykafka import KafkaClient


def default_json_serializer(message, partition_key):
    return (json.dumps(message).encode(), partition_key)


def default_dict_serializer(message, partition_key):
    return (pickle.dumps(message), partition_key)


def main(args):
    configfile = args['<config_file>']
    yaml_config = common.read_config_file(configfile)
    services = common.ServiceObjectRegistry(snap.initialize_services(yaml_config))

    topic_name = args['<topic>']
    tkservice = services.lookup('telekast')
    topic = tkservice.get_topic(topic_name)

    hfactory = tkcore.PipelineRecordHeaderFactory('pipeline_name', 'record_type')
    rfactory = tkcore.PipelineRecordFactory(payload_field_name='data')

    header = hfactory.create(pipeline_name='cdm_test', record_type='cdm')
    record = rfactory.create(header, {'message': 'test'})


    msg_count = 1000000
    time_log = jrnl.TimeLog()

    with topic.get_producer(use_rdkafka=True,
                            serializer=default_dict_serializer,
                            min_queued_messages=250000,
                            max_queued_messages=500000,
                            linger_ms=5) as producer:
        
        payload = uuid.uuid4()
        with jrnl.stopwatch('ingest_records', time_log):
            for i in range(msg_count):
                header = hfactory.create(pipeline_name='test',
                                        record_type='test_record')
                record = rfactory.create(header, **{'message': payload, 'tag': i})                
                producer.produce(record)
                if not i % 100000:
                    print('%d messages sent.' % i)

    print('%d messages sent to Kafka topic %s.' % (msg_count, topic_name))
    print(time_log.readout)


if __name__ == '__main__':
    args = docopt.docopt(__doc__)
    main(args)