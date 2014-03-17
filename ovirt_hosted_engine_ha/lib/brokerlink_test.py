import brokerlink
import unittest


class BrokerlinkTests(unittest.TestCase):
    def test_checked_communicate_no_data(self):
        """
        rhbz#1075126
        """
        b = brokerlink.BrokerLink()
        b._communicate = lambda req: "success"
        ret = b._checked_communicate("dummy request")
        self.assertEqual("", ret)

    def test_checked_communicate_fail_no_data(self):
        b = brokerlink.BrokerLink()
        b._communicate = lambda req: "failure"
        self.assertRaises(brokerlink.RequestError,
                          b._checked_communicate, "dummy request")

    def test_checked_communicate_fail_data(self):
        b = brokerlink.BrokerLink()
        b._communicate = lambda req: "failure data"
        self.assertRaises(brokerlink.RequestError,
                          b._checked_communicate, "dummy request")

    def test_checked_communicate_data(self):
        b = brokerlink.BrokerLink()
        b._communicate = lambda req: "success data"
        ret = b._checked_communicate("dummy request")
        self.assertEqual("data", ret)
