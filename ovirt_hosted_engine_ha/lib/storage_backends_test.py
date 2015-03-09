import unittest
import io
import struct
import zlib

from mock import Mock, call

# Mock vdsm.vdscli to be able to build on machine with no VDSM
import sys
sys.modules["vdsm"] = Mock()
sys.modules["vdsm.vdscli"] = Mock()

from .storage_backends import FilesystemBackend, BackendFailureException
from .storage_backends import BlockBackend
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


class StorageBackendTests(unittest.TestCase):
    SIGNATURE = "HEs0"

    def test_single_bad_block_decode(self):
        raw = struct.pack("!4sQ64pQQQQQQL",
                          "HEs0",
                          1, "test",
                          1, 100,
                          102, 100,
                          0, 0,
                          0)
        b = BlockBackend("/dev/null", "test-1")
        block = b.parse_meta_block(raw)
        self.assertEqual(block, BlockBackend.BlockInfo(
            self.SIGNATURE, 1, "test", ((1, 100), (102, 100)), False))

    def test_service_creation(self):
        b = BlockBackend("/dev/null", "test-1")
        blocks = b.create_info_blocks({"test1": 300,
                                       "test2": 512,
                                       "test3": 1024 * 1024 * 50})

        self.assertEqual(3, len(blocks))

        test1 = struct.pack("!4sQ64pQQQQ",
                            self.SIGNATURE,
                            1, "test1",
                            3, 1,
                            0, 0)
        test1crc = struct.pack("!L", zlib.crc32(test1) & 0xffffffff)
        test2 = struct.pack("!4sQ64pQQQQ",
                            self.SIGNATURE,
                            2, "test2",
                            4, 1,
                            0, 0)
        test2crc = struct.pack("!L", zlib.crc32(test2) & 0xffffffff)
        test3 = struct.pack("!4sQ64pQQQQ",
                            self.SIGNATURE,
                            0, "test3",
                            5, 102400,
                            0, 0)
        test3crc = struct.pack("!L", zlib.crc32(test3) & 0xffffffff)

        expected = [
            test1 + test1crc,
            test2 + test2crc,
            test3 + test3crc
        ]

        self.assertEqual(expected, blocks)

    def test_service_creation_with_offset(self):
        b = BlockBackend("/dev/null", "test-1")
        blocks = b.create_info_blocks({"test1": 300,
                                       "test2": 512,
                                       "test3": 1024 * 1024 * 50},
                                      first_free=1000)

        self.assertEqual(3, len(blocks))

        test1 = struct.pack("!4sQ64pQQQQ",
                            self.SIGNATURE,
                            1001, "test1",
                            1003, 1,
                            0, 0)
        test1crc = struct.pack("!L", zlib.crc32(test1) & 0xffffffff)
        test2 = struct.pack("!4sQ64pQQQQ",
                            self.SIGNATURE,
                            1002, "test2",
                            1004, 1,
                            0, 0)
        test2crc = struct.pack("!L", zlib.crc32(test2) & 0xffffffff)
        test3 = struct.pack("!4sQ64pQQQQ",
                            self.SIGNATURE,
                            0, "test3",
                            1005, 102400,
                            0, 0)
        test3crc = struct.pack("!L", zlib.crc32(test3) & 0xffffffff)

        expected = [
            test1 + test1crc,
            test2 + test2crc,
            test3 + test3crc
        ]

        self.assertEqual(expected, blocks)

    def test_create_block(self):
        raw = struct.pack("!4sQ64pQQQQQQ",
                          self.SIGNATURE,
                          1, "test",
                          1, 100,
                          102, 100,
                          0, 0)
        rawcrc = struct.pack("!L", zlib.crc32(raw) & 0xffffffff)
        b = BlockBackend("/dev/null", "test-1")
        block = b.create_block([(1, 100), (102, 100)], 1, "test")
        self.assertEqual(raw + rawcrc, block)

    def test_create_empty_block(self):
        raw = struct.pack("!4sQ64pQQ",
                          self.SIGNATURE,
                          1, "test",
                          0, 0)
        rawcrc = struct.pack("!L", zlib.crc32(raw) & 0xffffffff)
        b = BlockBackend("/dev/null", "test-1")
        block = b.create_block([], 1, "test")
        self.assertEqual(raw + rawcrc, block)

    def test_single_good_block_decode(self):
        raw = struct.pack("!4sQ64pQQQQQQ",
                          self.SIGNATURE,
                          1, "test",
                          1, 100,
                          102, 100,
                          0, 0)
        rawcrc = struct.pack("!L", zlib.crc32(raw) & 0xffffffff)
        b = BlockBackend("/dev/null", "test-1")
        block = b.parse_meta_block(raw + rawcrc)
        self.assertEqual(block, BlockBackend.BlockInfo(
            self.SIGNATURE, 1, "test", ((1, 100), (102, 100)), True))

    def test_dm_table(self):
        block = BlockBackend.BlockInfo(self.SIGNATURE, 1, "test",
                                       ((1, 100), (102, 100)), True)
        b = BlockBackend("/dev/null", "test-1")
        table = b.compute_dm_table(block.pieces)
        expected = ("0 100 linear /dev/null 1\n"
                    "100 100 linear /dev/null 102")
        self.assertEqual(expected, table)

    def test_get_services(self):
        raw1 = struct.pack("!4sQ64pQQQQQQ",
                           self.SIGNATURE,
                           1, "test",
                           1, 100,
                           102, 100,
                           0, 0)
        raw1crc = struct.pack("!L", zlib.crc32(raw1) & 0xffffffff)

        raw2 = struct.pack("!4sQ64pQQQQQQ",
                           self.SIGNATURE,
                           0, "test2",
                           2, 200,
                           202, 200,
                           0, 0)
        raw2crc = struct.pack("!L", zlib.crc32(raw2) & 0xffffffff)

        b = BlockBackend("/dev/null", "test-1")
        blockdev = io.BytesIO()
        blockdev.write(raw1)
        blockdev.write(raw1crc)
        blockdev.seek(b.blocksize)
        blockdev.write(raw2)
        blockdev.write(raw2crc)
        blockdev.seek(0)
        expected = ({'test': [(1, 100), (102, 100)],
                    'test2': [(2, 200), (202, 200)]},
                    1,
                    402)
        services = b.get_services(blockdev)
        self.assertEqual(expected, services)

    def test_updates(self):
        b = BlockBackend("/dev/null", "test-1")
        b._services = {
            "a": [(0, 5), (15, 10)],
            "b": [(20, 20)],
            "d": [(0, 1)]
        }
        updates = b._compute_updates({
            "a": 15 * b.blocksize + 5,  # should add another 5B
            "b": 5,  # no update, the space is already there
            "c": 5,  # new service
            "d": 5  # won't update because we always allocate a whole block
        })
        expected = {
            "a": 5,
            "c": 5
        }
        self.assertEqual(expected, updates)

    def test_write_start(self):
        b = BlockBackend("/dev/null", "test-1")
        dev = io.BytesIO()
        info = ['\0', '\1']
        b._write(dev, info)
        self.assertEqual('\0' * 512 + '\1', dev.getvalue())

    def test_write_update(self):
        b = BlockBackend("/dev/null", "test-1")
        dev = io.BytesIO()
        dev.write(b.create_block([], 1, "test"))
        dev.seek(b.blocksize)
        dev.write(b.create_block([], 0, "test2"))

        new_info = [b.create_block([], 0, "test3")]
        b._write(dev, new_info, last_info=1, first_free=2)

        expected = io.BytesIO()
        expected.write(b.create_block([], 1, "test"))
        expected.seek(b.blocksize)
        expected.write(b.create_block([], 2, "test2"))
        expected.seek(2 * b.blocksize)
        expected.write(b.create_block([], 0, "test3"))
        self.assertEqual(expected.getvalue(), dev.getvalue())
