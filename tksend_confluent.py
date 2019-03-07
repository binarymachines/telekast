#!/usr/bin/env python

'''
Usage:
    tksend_confluent --config <config_file> --topic <topic> --format <format> [--datafile <file>]
 
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
from confluent_kafka import Producer


errors = []

def default_json_serializer(message, partition_key):
    return (json.dumps(message).encode(), partition_key)


def default_dict_serializer(message, partition_key):
    return (pickle.dumps(message), partition_key)


def delivery_report(err, msg):
    if err is not None:
        errors.append(msg)


def main(args):
    configfile = args['<config_file>']
    yaml_config = common.read_config_file(configfile)
    services = common.ServiceObjectRegistry(snap.initialize_services(yaml_config))


    topic_name = args['<topic>']
    tkservice = services.lookup('telekast')
    topic = tkservice.get_topic(topic_name)

    hfactory = tkcore.PipelineRecordHeaderFactory('pipeline_name', 'record_type')
    rfactory = tkcore.PipelineRecordFactory(payload_field_name='data')

    msg_count = 2000000
    time_log = jrnl.TimeLog()

    nodes = [
        tkcore.KafkaNode('10.142.0.86'),
        tkcore.KafkaNode('10.142.0.87'),
        tkcore.KafkaNode('10.142.0.88')
    ]

    connect_string = ','.join([str(n) for n in nodes] )

    prod_config = {
                "on_delivery": delivery_report,
                "bootstrap.servers": connect_string,
                "group.id": "python_injector",
                "retry.backoff.ms": 3000,
                "retries": 5,
                "default.topic.config": {"request.required.acks": "1"},
                "max.in.flight.requests.per.connection": 1,
                "queue.buffering.max.messages": 100000,
                "batch.num.messages": 50000,
                "message.max.bytes": 2000000
            }

    
    producer = Producer(**prod_config)
    
        
    payload = uuid.uuid4()
    with jrnl.stopwatch('ingest_records', time_log):
        
        for i in range(msg_count):
            
            producer.poll(0)
            header = hfactory.create(pipeline_name='test',
                                        record_type='test_record')
            record = rfactory.create(header, **{'message': payload, 'tag': i})                
            producer.produce(topic_name, pickle.dumps(record), callback=delivery_report)
            if not i % 100000:
                
                print('%d messages sent.' % i)

        producer.flush()

    print('%d messages sent to Kafka topic %s.' % (msg_count, topic_name))
    print(time_log.readout)
    if len(errors):
        print('!!! Errors sending messages:')
        print('\n'.join(errors))


if __name__ == '__main__':
    args = docopt.docopt(__doc__)
    main(args)
