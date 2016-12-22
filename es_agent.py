#!/usr/bin/env python
# encoding: utf-8

"""
Author: Rosen
Mail: rosenluov@gmail.com
File: es_agent.py
Created Time: 12/21/16 14:34
"""

from __future__ import print_function

import datetime
import sys
import time

import requests

from daemonize import Daemon
from settings import (
    traps1,
    traps2,
    GAUGE,
    COUNTER,
    pidfile,
    stderr,
    stdout,
    HOSTNAME,
    IP,
    PORT
)

"""
    Run without parameters to debug open-falcon
"""


def es_data(endpoint, metric, timestamp, value, counterType, tags):
    structure = {
        'endpoint': endpoint,
        'metric': metric,
        'timestamp': timestamp,
        'step': 10,
        'value': value,
        'counterType': counterType,
        'tags': tags
    }
    return structure


# read specified keys from json data
def get_keys(stats, traps):
    tags = []
    data = []
    temp = stats.get('attributes', {})
    ts = int(time.time())
    for i in temp:
        tags.append(i + '=' + temp.get(i, ''))
    tags = ','.join(tags)
    for t in traps:
        if t == 'status':
            value = stats.get(t, '')
            if value == 'green':
                stats[t] = 0
            elif value == 'yellow':
                stats[t] = 1
            elif value == 'red':
                stats[t] = 2

        c = t.split('.')
        s = stats
        while len(c):
            s = s.get(c.pop(0), {})

        if s == {}:
            continue

        metric = 'es.' + t
        if t in GAUGE:
            data.append(es_data(HOSTNAME, metric, ts, s, 'GAUGE', tags))
        elif t in COUNTER:
            data.append(es_data(HOSTNAME, metric, ts, s, 'COUNTER', tags))

    return data


class MyDaemon(Daemon):
    @staticmethod
    def run():
        while True:
            start_time = time.time()
            main()
            end_time = time.time()
            st = end_time - start_time
            sleep_time = 10 - st
            time.sleep(sleep_time)


def write_log(out=None):
    if out is None:
        out = []
    sys.stdout.write("{date} -  metrics: {num}\n}".format(date=datetime.datetime.now(), num=str(len(out))))
    sys.stdout.flush()


def main():
    # load json data
    node = {}
    try:
        f = requests.get("http://{IP}:{PORT}/_cluster/health".format(IP=IP, PORT=PORT))
        health = f.json()
        f = requests.get("http://{IP}:{PORT}/_nodes/_local/stats?all=true".format(IP=IP, PORT=PORT))
        all = f.json()
        # only for current node
        for node_id in all.get('nodes', {}).keys():
            if all['nodes'][node_id]['host'].startswith(IP):
                node = all['nodes'][node_id]
                if len(sys.argv) == 1:
                    print("node found")

    except Exception as e:
        print(e.__str__() + "and Unable to load JSON data!")
        sys.exit(1)

    out = get_keys(health, traps1)  # getting health values
    out.extend(get_keys(node, traps2))  # getting stats  values

    write_log(out)

    # os.remove(tmp)


if __name__ == "__main__":
    myDaemon = MyDaemon(pidfile=pidfile,
                        stdout=stdout,
                        stderr=stderr)
    args = sys.argv
    if len(args) == 2:
        if 'start' == args[1]:
            myDaemon.start()
        elif 'stop' == args[1]:
            myDaemon.stop()
        elif 'restart' == args[1]:
            myDaemon.restart()
        else:
            print('*** Unknown command')
            sys.exit(2)
        sys.exit(0)
    else:
        print('Usage: {} start|stop|restart'.format(args[0]))
        sys.exit(2)
