#!/usr/bin/env python3

from kazoo.client import KazooClient
import json
import logging
import os
from subprocess import Popen
import time
from urllib.request import urlopen
from urllib.error import URLError

CLUSTER_LIST_ENDPOINT = '/exhibitor/v1/cluster/list'
CLUSTER_LIST_TIMEOUT = 3

PROPERTIES_TEMPLATE = '/kafka/config/server.properties.template'
PROPERTIES_DEFAULTS = {
    'KAFKA_BROKER_ID': '-1',
    'KAFKA_CREATE_TOPICS_ENABLE': 'true',
    'KAFKA_REPLICATION_FACTOR': '1',
    'KAFKA_ADVERTISED_HOST_NAME': os.environ.get('IP', ''),
    'KAFKA_DELETE_TOPIC_ENABLE': 'false',
    'KAFKA_PORT': '9092',
    'KAFKA_ADVERTISED_PORT': '9092',
    'KAFKA_LOG_DIR': '/data',
    'KAFKA_NUM_PARTITIONS': '1',
    'KAFKA_LOG_RETENTION_HOURS': '168',
    'KAFKA_LOG_FLUSH_INTERVAL_MS': '3000',
    'KAFKA_LOG_FLUSH_INTERVAL_MESSAGES': '10000',
    'ZOOKEEPER_CHROOT': '/kafka',
    'ZOOKEEPER_CONNECTION_TIMEOUT_MS': '10000',
    'ZOOKEEPER_SESSION_TIMEOUT_MS': '10000',
}
PROPERTIES_PATH = '/kafka/config/server.properties'

START_COMMAND = '/kafka/bin/kafka-server-start.sh'

logger = logging.getLogger('kafka')


def main(loops=-1, loop_interval=60, restart_time=30):
    """
    :param loops: Number of loops. (set <0 for infinite)
    :param loop_interval: Time to sleep per loop (seconds).
    :param restart_time: Time to sleep after a restart (seconds).
    :return: Return code for process.
    """
    exhibitor = os.environ.get('EXHIBITOR_BASE')
    if not exhibitor:
        logger.error('Variable EXHIBITOR_BASE not found')
        return -1

    base_properties = server_template()

    # Discover ZK and start server:
    zk_conn = zk_conn_string(exhibitor)
    kafka_pid = start_kafka(base_properties, zk_conn)

    # Loop:
    while loops != 0:
        # If Kafka has died, stop:
        kafka_pid.poll()
        if kafka_pid.returncode:
            logger.info('Kafka died: %s', kafka_pid.returncode)
            return kafka_pid.returncode

        # Poll Exhibitor for current ensemble:
        cur_zk = zk_conn_string(exhibitor)
        if cur_zk != zk_conn and len(cur_zk) >= len(zk_conn):
            logger.info('ZooKeeper ensemble change: %s', cur_zk)
            # If ensemble has changed, acquire lock:
            zk = KazooClient(hosts=','.join(cur_zk))
            try:
                zk.start()
                with zk.Lock('/kafka-exhibitor/%s' % exhibitor):
                    logger.info('Restart lock acquired, restarting...')
                    # Lock acquired, restart:
                    kafka_pid.terminate()
                    kafka_pid.wait()
                    kafka_pid = start_kafka(base_properties, cur_zk)
                    zk_conn = cur_zk

                    time.sleep(restart_time)
            finally:
                zk.stop()

        # Loop:
        time.sleep(loop_interval)
        loops -= 1
    return 0


def server_template():
    with open(PROPERTIES_TEMPLATE) as template_in:
        props_template = template_in.read()
    # Expand template:
    for key, default_value in PROPERTIES_DEFAULTS.items():
        value = os.environ.get(key, default_value)
        props_template = props_template.replace('{{%s}}' % key, value)
    return props_template


def zk_conn_string(exhibitor):
    exhibitor_url = '%s%s' % (exhibitor, CLUSTER_LIST_ENDPOINT)
    try:
        cluster_info = urlopen(exhibitor_url, timeout=CLUSTER_LIST_TIMEOUT)
        exhibitor_hosts = json.loads(cluster_info.read().decode('utf-8'))
    except URLError:
        logger.error('Connection failure: %s', exhibitor_url)
        return []

    zk_servers = sorted(exhibitor_hosts['servers'])
    zk_port = exhibitor_hosts['port']
    return ['%s:%d' % (zk_server, zk_port) for zk_server in zk_servers]


def start_kafka(base_properties, zk_conn):
    properties = base_properties.replace('{{ZOOKEEPER_CONNECTION_STRING}}',
                                         ','.join(zk_conn))
    with open(PROPERTIES_PATH, 'w') as template_out:
        template_out.write(properties)

    logger.info('Starting kafka: %s', zk_conn)
    kafka_pid = Popen([START_COMMAND, PROPERTIES_PATH])
    return kafka_pid


if __name__ == '__main__':
    import sys

    logging.basicConfig(level=logging.DEBUG)

    return_code = main()
    sys.exit(return_code)
