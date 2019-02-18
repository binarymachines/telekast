#!/usr/bin/env python

'''
Usage:
    tksend --config <config_file> --topic <topic> --format <format> [--datafile <file>]
 
'''

import os, sys
import datetime
from snap import common
import docopt
from telekast import core as tkcore
from pykafka import KafkaClient
#>>> client = KafkaClient(hosts="127.0.0.1:9092,127.0.0.1:9093,...")



def main(args):
    print(common.jsonpretty(args))

    # construct header for ingest records
    hfactory = tkcore.PipelineRecordHeaderFactory('pipeline_name', 'timestamp', 'record_type')
    header = hfactory.create(pipeline_name='test',
                             timestamp=datetime.datetime.now().isoformat(),
                             record_type='test_record')

    # construct record to be ingested
    rfactory = tkcore.PipelineRecordFactory(payload_field_name='data')
    test_data = {'message': 'Hello World from telekast!'}
    record = rfactory.create(header, **test_data)

    print(common.jsonpretty(record))
    


if __name__ == '__main__':
    args = docopt.docopt(__doc__)
    main(args)