#!/usr/bin/env python

import os, sys
from snap import snap
from snap import common
from telekast import core as tkcore
from pykafka import KafkaClient


INIT_PARAMS = ['nodes']

class TelekastService(object):
    def __init__(self, **kwargs):
        kwreader = common.KeywordArgReader('kafka_nodes')
        kwreader.read(**kwargs)        
        node_strings = kwreader.get_value('kafka_nodes')
        nodes = [tkcore.KafkaNode(ns) for ns in node_strings]
        
        self.connect_string = ','.join([str(n) for n in nodes])    
        self.kafka_client = KafkaClient(hosts=self.connect_string)
    
        #hfactory = tkcore.PipelineRecordHeaderFactory('pipeline_name', 'record_type')
        #rfactory = tkcore.PipelineRecordFactory(payload_field_name='data')

    def get_topic(self, topic_name):
        topic_id = topic_name.encode()
        if not topic_id in self.kafka_client.topics.keys():
            raise Exception('No topic "%s" available.' % topic_name)    
        return self.kafka_client.topics[topic_id]
