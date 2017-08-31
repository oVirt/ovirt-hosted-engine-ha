import unittest

from mock import Mock, call

from .storage_backends import FilesystemBackend, BackendFailureException
from ..env import constants


class TestLVMBackend(unittest.TestCase):
    def test_lvmcreate_new(self):
        dev = FilesystemBackend("uuid", "type")
        dev.lvsize = Mock()
        dev.lvsize.return_value = None
        dev.lvcreate = Mock()
        dev.lvcreate.return_value = True
        dev.lvzero = Mock()
        dev._create_block("test", 500, False)

        # Test if the create call happened
        self.assertEqual(
            [call("uuid", constants.SD_METADATA_DIR + "-test", 500)],
            dev.lvcreate.mock_calls)

        # Test if the zero call happened
        self.assertEqual(
            [call("uuid", constants.SD_METADATA_DIR + "-test", 500)],
            dev.lvzero.mock_calls)

    def test_lvmcreate_enough(self):
        dev = FilesystemBackend("uuid", "type")
        dev.lvsize = Mock()
        dev.lvsize.return_value = 600
        dev.lvcreate = Mock()
        dev.lvcreate.return_value = True
        dev.lvzero = Mock()
        dev._create_block("test", 500, False)

        # Test if the create call was skipped
        self.assertEqual(
            [],
            dev.lvcreate.mock_calls)

        # Test if the zero call was skipped
        self.assertEqual(
            [],
            dev.lvzero.mock_calls)

    def test_lvmcreate_enough_force(self):
        dev = FilesystemBackend("uuid", "type")
        dev.lvsize = Mock()
        dev.lvsize.return_value = 600
        dev.lvcreate = Mock()
        dev.lvcreate.return_value = True
        dev.lvzero = Mock()
        dev._create_block("test", 500, True)

        # Test if the create call was skipped
        self.assertEqual(
            [],
            dev.lvcreate.mock_calls)

        # Test if the zero call happened
        self.assertEqual(
            [call("uuid", constants.SD_METADATA_DIR + "-test", 500)],
            dev.lvzero.mock_calls)

    def test_lvmcreate_not_enough(self):
        dev = FilesystemBackend("uuid", "type")
        dev.lvsize = Mock()
        dev.lvsize.return_value = 400
        dev.lvcreate = Mock()
        dev.lvcreate.return_value = True
        dev.lvzero = Mock()

        self.assertRaises(BackendFailureException,
                          dev._create_block, "test", 500, False)

        # Test if the create call was skipped
        self.assertEqual(
            [],
            dev.lvcreate.mock_calls)

        # Test if the zero call was skipped
        self.assertEqual(
            [],
            dev.lvzero.mock_calls)
