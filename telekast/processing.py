#!/usr/bin/env python

import os
import sys
import threading
import time
import datetime
import json


def json_deserializer(value):
    return json.loads(value.decode('utf-8'))


class CheckpointTimer(threading.Thread):
    def __init__(self, checkpoint_function, **kwargs):
        threading.Thread.__init__(self)
        kwreader = common.KeywordArgReader('checkpoint_interval').read(**kwargs)
        self._seconds = 0
        self._stopped = True
        self._checkpoint_function = checkpoint_function
        self._interval = kwreader.get_value('checkpoint_interval')
        self._checkpoint_function_args = kwargs

        
    def run(self):        
        self._stopped = False
        log.info('starting checkpoint timer at %s.' % datetime.datetime.now().isoformat())
        while not self._stopped:
            time.sleep(1)
            self._seconds += 1
            if self. _seconds >= self._interval:
                self._checkpoint_function(**self._checkpoint_function_args)
                self.reset()


    @property
    def elapsed_time(self):
        return self._seconds


    def reset(self):        
        self._seconds = 0
        log.info('resetting checkpoint timer at %s.' % datetime.datetime.now().isoformat())


    def stop(self):
        self._stopped = True
        log.info('stopping checkpoint timer at %s.' % datetime.datetime.now().isoformat())



def filter(filter_function, records, **kwargs):
    return (rec for rec in records if filter_function(rec, **kwargs))


def file_generator(**kwargs):
    filename = kwargs.get('filename')
    with open(filename) as f:
        while True:
            line = f.readline()
            if line:
                yield line
            else:
                return


class RecordSource(object):
    def __init__(self, **kwargs):
        kwreader = common.KeywordArgReader('generator', 'services')
        kwreader.read(**kwargs)
        self._generator = kwargs['generator']
        self._services = kwargs['services']


    def records(self, **kwargs):
        kwargs.update({'services': self._services})
        data = self._generator(**kwargs)
        filter_func = kwargs.get('filter')
        if filter_func:
            return (filter(filter_func, data, **kwargs))
        else:
            return (record for record in data)


class TopicFork(object):
    def __init__(self, **kwargs):
        kwreader = common.KeywordArgReader('accept_topic',
                                           'reject_topic',
                                           'qualifier',
                                           'service_objects')
        kwreader.read(**kwargs)
        self.accept_topic = kwargs['accept_topic']
        self.reject_topic = kwargs['reject_topic']
        self.qualify = kwargs['qualifier']
        self.services = kwargs['services']


    def split(self, record_generator, **kwargs):
        kwreader = common.KeywordArgReader('kafka_writer')
        kwreader.read(**kwargs)
        kafka_writer = kwargs['kafka_writer']
        kwargs.update({'services': self.services})
        for record in record_generator:
            if self.qualify(record, **kwargs):
                kafka_writer.write(self.accept_topic, record)
            else:
                kafka_writer.write(self.reject_topic, record)


class TopicFilter(object):
    def __init__(self, **kwargs):
        kwreader = common.KeywordArgReader('target_topic',
                                           'qualifier',
                                           'services')
        kwreader.read(**kwargs)
        self.target_topic = kwargs['target_topic']
        self.qualify = kwargs['qualifier']
        self.services = kwargs['services']


    def process(self, record_generator, **kwargs):
        kwreader = common.KeywordArgReader('kafka_writer')
        kwreader.read(**kwargs)
        kafka_writer = kwargs['kafka_writer']
        kwargs.update({'services': self.services})
        for record in record_generator:
            if self.qualify(record, **kwargs):
                kafka_writer.write(self.target_topic, record)


class TopicRouter(object):
    def __init__(self, **kwargs):
        kwreader = common.KeywordArgReader('services')
        kwreader.read(**kwargs)
        self.services = kwargs['services']
        self.targets = {}


    def register_target(self, qualifier_function, target_topic, precedence=0):
        self.targets[(precedence, qualifier_function)] = target_topic

        # The default behavior is to test each record against each qualifier in
        # self.targets. In some cases it may be helpful to attempt matches in a definite order.
        # We do this by registering higher precedence numbers to the qualifiers we wish to call first.


    def process(self,
                record_generator,
                kafka_writer,
                respect_precedence_order=False,
                allow_multi_target_match=True,
                **kwargs):
        if not self.targets:
            raise Exception('Empty target table. You must register at least one target in order to process records.')
        
        default_topic = kwargs.get('default_topic')
        kwargs.update({'services': self.services})

        for record in record_generator:

            # The default behavior is that one record may be dispatched to multiple target topics if it passes
            # multiple qualifiers. If we don't want to test a record against all registered qualifiers, but instead break
            # after the first match, then we set the allow_multi_target_match param to False. Then we will
            # break out of the inner loop as soon as a qualifier returns True, or after all qualifiers
            # have returned False.
            #
            # If the respect_precedence_order param is True, the record will be tested against qualifiers
            # in descending order of precedence.

            if respect_precedence_order:
                qualifier_tuples = sorted(self.targets.keys(), reverse=True)
            else:
                qualifier_tuples = self.targets.keys()

            for key in qualifier_tuples:
                qualifier_func = key[1]
                topic = self.targets[key]
                matching_target_found = False
                if qualifier_func(record, **kwargs):
                    matching_target_found = True
                    kafka_writer.write(topic, record)
                    if not allow_multi_target_match:
                        break

            if not matching_target_found and default_topic:
                kafka_writer.write(default_topic, record)


class KafkaIngestRecordReader(object):
    def __init__(self,
                 topic,
                 *kafka_nodes,
                 **kwargs):

        self._topic = topic
        # commit on every received message by default
        self._commit_interval = kwargs.get('commit_interval', 1)
        deserializer = kwargs.get('deserializer', json_deserializer)
        group = kwargs.get('group', None)
        self._consumer = KafkaConsumer(group_id=group,
                                       bootstrap_servers=','.join([n() for n in kafka_nodes]),
                                       value_deserializer=deserializer,
                                       auto_offset_reset='earliest',
                                       consumer_timeout_ms=5000)


    def read(self, data_relay, **kwargs): # insist on passing a checkpoint_frequency as kwarg?

        # if we are in checkpoint mode, issue a checkpoint signal every <interval> seconds
        # or every <frequency> records, whichever is shorter
        checkpoint_frequency = int(kwargs.get('checkpoint_frequency', -1))
        checkpoint_interval = int(kwargs.get('checkpoint_interval', 300))

        checkpoint_mode = False
        checkpoint_timer = None
        if checkpoint_frequency > 0:
            checkpoint_mode = True
            checkpoint_timer = CheckpointTimer(data_relay.checkpoint, **kwargs)
            checkpoint_timer.start()

        message_counter = 0
        num_commits = 0
        error_count = 0

        for message in self._consumer:
            try:
                data_relay.send(message)
                if message_counter % self._commit_interval == 0:
                    self._consumer.commit()
                    num_commits += 1
                if checkpoint_mode and message_counter % checkpoint_frequency == 0:
                    data_relay.checkpoint(**kwargs)
                    checkpoint_timer.reset()
            except Exception as err:
                log.debug('Kafka message reader threw an exception from its DataRelay while processing message %d: %s' % (message_counter, str(err)))
                log.debug('Offending message: %s' % str(message))
                #traceback.print_exc()
                error_count += 1
            finally:
                message_counter += 1

        log.info('%d committed reads from topic %s, %d messages processed, %d errors.' % (num_commits, self._topic, message_counter, error_count))
        
        # issue a last checkpoint on our way out the door
        # TODO: find out if what's really needed here is a Kafka consumer
        # timeout exception handler

        if checkpoint_mode:
            try:
                log.info('Kafka message reader issuing final checkpoint command at %s...' % datetime.datetime.now().isoformat())
                data_relay.checkpoint(**kwargs)
            except Exception as err:
                log.error('Final checkpoint command threw an exception: %s' % str(err))
            finally:    
                checkpoint_timer.stop()

        return num_commits


    @property
    def commit_interval(self):
        return self._commit_interval


    @property
    def num_commits_issued(self):
        return self._num_commits


    @property
    def consumer(self):
        return self._consumer


    @property
    def topic(self):
        return self._topic



class DataRelay(object):
    def __init__(self, **kwargs):
        self._transformer = kwargs.get('transformer')


    def pre_send(self, src_message_header, **kwargs):
        pass


    def post_send(self, src_message_header, **kwargs):
        pass


    def _send(self, src_message_header, data, **kwargs):
        '''Override in subclass
        '''
        pass


    def _checkpoint(self, **kwargs):
        '''Override in subclass'''
        pass


    def checkpoint(self, **kwargs):
        try:
            self._checkpoint(**kwargs)
            log.info('data relay passed checkpoint at %s' % datetime.datetime.now().isoformat())
        except Exception as err:
            log.error('data relay failed checkpoint with error:')
            log.error(err)


    def send(self, kafka_message, **kwargs):
        header_data = {}
        header_data['topic'] = kafka_message.topic
        header_data['partition'] = kafka_message.partition
        header_data['offset'] = kafka_message.offset
        header_data['key'] = kafka_message.key

        kmsg_header = KafkaMessageHeader(header_data)
        self.pre_send(kmsg_header, **kwargs)
        if self._transformer:
            data_to_send = self._transformer.transform(kafka_message.value['body'])
            print('## Data to send: %s \n\n' % str(data_to_send))
        else:
            data_to_send = kafka_message.value['body']
        self._send(kmsg_header, data_to_send, **kwargs)
        self.post_send(kmsg_header, **kwargs)


class ConsoleRelay(DataRelay):
    def __init__(self, **kwargs):
        DataRelay.__init__(self, **kwargs)


    def _send(self, src_message_header, message_data):
        print('### record at offset %d: %s' % (src_message_header.offset, message_data))
