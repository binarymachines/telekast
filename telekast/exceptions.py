#!/usr/bin/env python



class NoSuchPartition(Exception):
    def __init__(self, partition_id):
        Exception.__init__(self, 'The target Kafka cluster has no partition "%s".' % partition_id)


class UnregisteredTransferFunction(Exception):
    def __init__(self, op_name):
        Exception.__init__(self, 'No transfer function registered under the name "%s" with transfer agent.' % op_name)

