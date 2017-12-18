
import mock
from six.moves import queue
import threading
from unittest import TestCase

from . import util


QUEUE_NAME = 'test.event.queue'


class _ConfigMock(object):
    def get(self, section, name):
        if section == 'addresses' and name == 'event_queue':
            return QUEUE_NAME

        return None


class _ConnectionMock(object):
    SUB_ID = '12345678-1234-1234-1234-123456789012'

    def __init__(self):
        self._queue = None
        self._event = threading.Event()

    def subscribe(self, queue_name, queue_obj):
        self._queue = queue_obj
        self._event.set()
        return self.SUB_ID

    def unsubscribe(self, sub_id):
        if sub_id == self.SUB_ID:
            self._queue.put(None)
            self._queue = None
            self._event.clear()

    def is_subscribed(self):
        return self._queue is not None

    def receive_msg(self, msg):
        if self._queue is not None:
            self._queue.put(msg)

    def wait_for_subscription(self, timeout):
        return self._event.wait(timeout)


class BroadcasterTest(TestCase):

    TIMEOUT = 2

    def _get_with_timouot(self, q):
        try:
            return q.get(timeout=self.TIMEOUT)
        except queue.Empty:
            self.fail("Queue timed out")

    @mock.patch('vdsm.config.config', _ConfigMock())
    @mock.patch('ovirt_hosted_engine_ha.lib.util.connect_vdsm_json_rpc')
    @mock.patch('ovirt_hosted_engine_ha.lib.util'
                '.get_vdsm_json_rpc_wo_reconnect')
    def test_broadcast(self, mock_connect_vdsm, mock_get_vdsm):
        connection = _ConnectionMock()
        mock_connect_vdsm.return_value = connection
        mock_get_vdsm.return_value = connection

        broadcaster = util._EventBroadcaster()

        self.assertFalse(connection.is_subscribed())

        queue1 = queue.Queue()
        queue2 = queue.Queue()
        broadcaster.register_queue(queue1)
        broadcaster.register_queue(queue2)

        if not connection.wait_for_subscription(self.TIMEOUT):
            self.fail("Waiting for subscription timed out")

        self.assertTrue(connection.is_subscribed())

        message = 'Test message'
        connection.receive_msg(message)

        self.assertEqual(
            self._get_with_timouot(queue1),
            message
        )

        self.assertEqual(
            self._get_with_timouot(queue2),
            message
        )

        del queue1
        del queue2

        broadcaster.close()

        self.assertFalse(connection.is_subscribed())
