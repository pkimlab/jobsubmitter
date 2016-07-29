#!/usr/bin/env python
import sys
import time
import json


def eprint(text, file=sys.stderr, **kwargs):
    print(text, file=sys.stderr, **kwargs)


def main(**kwargs):
    eprint(sys.executable)
    eprint(sys.version)
    eprint('main({})'.format(kwargs))
    eprint('Before taking a nice nap...')
    time.sleep(30)
    eprint('Fresh after my nap :).')
    eprint('Done!')
    print(json.dumps(kwargs))


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--job_id')
    args = parser.parse_args()
    main(**args.__dict__)
