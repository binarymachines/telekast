#!/usr/bin/env python

'''
Usage:
    tkrcv --config <config_file> --topic <topic> [--datafile <file>]

'''

import os, sys
from snap import common
import docopt


def main(args):
    print(common.jsonpretty(args))


if __name__ == '__main__':
    args = docopt.docopt(__doc__)
    main(args)