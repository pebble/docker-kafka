from io import BytesIO
from mock import MagicMock, patch, ANY
import os
from subprocess import Popen
import tempfile
import unittest
from urllib.error import URLError

import start
from start import main, server_template, start_kafka, zk_conn_string

EXHIBITOR_HOST = 'http://test-exhibitor'
EXHIBITOR_RESPONSE = ('{"servers":["10.2.103.50","10.2.101.144",' +
                      '"10.2.102.25"],"port":2181}')
# Sorted, ports spliced in:
ZK_CONN_STRING = ['10.2.101.144:2181', '10.2.102.25:2181', '10.2.103.50:2181']


class TestStart(unittest.TestCase):
    def setUp(self):
        os.environ['EXHIBITOR_BASE'] = EXHIBITOR_HOST
        self.og_template = start.PROPERTIES_TEMPLATE
        start.PROPERTIES_TEMPLATE = 'config/server.properties.template'

    def tearDown(self):
        start.PROPERTIES_TEMPLATE = self.og_template

    def test_server_template(self):
        os.environ['KAFKA_REPLICATION_FACTOR'] = '3'
        template = server_template()
        # Expanded with default:
        self.assertIn('auto.create.topics.enable=true', template)
        # Expanded with variable:
        self.assertIn('default.replication.factor=3', template)
        # Not expanded:
        self.assertIn('{{ZOOKEEPER_CONNECTION_STRING}}', template)

    @patch('start.urlopen')
    def test_zk_conn_string(self, mock_urlopen):
        mock_urlopen.return_value = BytesIO(bytes(EXHIBITOR_RESPONSE, 'utf-8'))

        conn_string = zk_conn_string(EXHIBITOR_HOST)
        mock_urlopen.assert_called_with(
            'http://test-exhibitor/exhibitor/v1/cluster/list', timeout=3)

        self.assertEqual(ZK_CONN_STRING, conn_string)

    @patch('start.urlopen')
    def test_zk_conn_string_error(self, mock_urlopen):
        mock_urlopen.side_effect = URLError('Kaboom')
        conn_string = zk_conn_string(EXHIBITOR_HOST)
        self.assertEqual([], conn_string)

    @patch('start.Popen')
    def test_start_kafka(self, mock_popen):
        _, temp_path = tempfile.mkstemp('kafka_test')
        og_path = start.PROPERTIES_PATH
        try:
            start.PROPERTIES_PATH = temp_path
            start_kafka('', ZK_CONN_STRING)
            mock_popen.assert_called_with([
                start.START_COMMAND, temp_path
            ])
        finally:
            start.PROPERTIES_PATH = og_path
            os.unlink(temp_path)

    @patch('start.KazooClient')
    @patch('start.start_kafka')
    def test_main_kafka_dies(self, mock_kafka_start, mock_kazoo):
        mock_popen = MagicMock(spec=Popen)
        mock_kafka_start.return_value = mock_popen
        mock_popen.returncode = -1

        main_return = main()

        self.assertEqual(-1, main_return)
        mock_kazoo.assert_not_called()

    @patch('start.KazooClient')
    @patch('start.start_kafka')
    @patch('start.zk_conn_string')
    def test_main_static(self, mock_conn_string, mock_kafka_start, mock_kazoo):
        mock_conn_string.return_value = []
        mock_popen = MagicMock(spec=Popen)
        mock_kafka_start.return_value = mock_popen
        mock_popen.returncode = None

        main_return = main(loops=1, loop_interval=0.001)

        self.assertEqual(0, main_return)
        mock_kazoo.assert_not_called()

    @patch('start.KazooClient')
    @patch('start.start_kafka')
    @patch('start.zk_conn_string')
    def test_main_restart(self, mock_conn_string, mock_kafka_start, mock_kazoo):
        mock_conn_string.side_effect = [
            ['1.1.1.1', '2.2.2.2'],
            ['2.2.2.2', '3.3.3.3']
        ]
        mock_popen = MagicMock(spec=Popen)
        mock_kafka_start.return_value = mock_popen
        mock_popen.returncode = None

        main_return = main(loops=1, loop_interval=0.001, restart_time=0.001)

        self.assertEqual(0, main_return)
        mock_kazoo.assert_called_with(hosts=ANY)
        self.assertEqual(2, mock_kafka_start.call_count)


if __name__ == '__main__':
    unittest.main()
