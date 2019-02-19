#!/usr/bin/env python

'''
Usage:
    tksend --config <config_file> --topic <topic> --format <format> [--datafile <file>]
 
'''

import os, sys
import json
import datetime
from snap import common
from mercury import journaling as jrnl
import docopt
from telekast import core as tkcore
from pykafka import KafkaClient


def default_json_serializer(message, partition_key):
    return (json.dumps(message).encode(), partition_key)


def main(args):
    print(common.jsonpretty(args))
    nodes = [
        tkcore.KafkaNode('10.142.0.86'),
        tkcore.KafkaNode('10.142.0.87'),
        tkcore.KafkaNode('10.142.0.88')
    ]

    connect_string = ','.join([str(n) for n in nodes])    
    kclient = KafkaClient(hosts=connect_string)
    print(kclient.topics)

    topic_name = args['<topic>'].encode()
    if not topic_name in kclient.topics.keys():
        print('No topic "%s" listed.' % topic_name)
        return

    topic = kclient.topics[topic_name]

    hfactory = tkcore.PipelineRecordHeaderFactory('pipeline_name', 'timestamp', 'record_type')
    rfactory = tkcore.PipelineRecordFactory(payload_field_name='data')

    msg_count = 100000
    time_log = jrnl.TimeLog()

    
    with topic.get_producer(use_rdkafka=True,
                            serializer=default_json_serializer) as producer:
        with jrnl.stopwatch('ingest_records', time_log):
            for i in range(msg_count):
                header = hfactory.create(pipeline_name='test',
                                        timestamp=datetime.datetime.now().isoformat(),
                                        record_type='test_record')
                record = rfactory.create(header, **{'message': 'telekast test message', 'tag': i})
                producer.produce(record)

    print('%d messages sent to Kafka topic %s.' % (msg_count, topic_name))
    print(time_log.readout)


if __name__ == '__main__':
    args = docopt.docopt(__doc__)
    main(args)