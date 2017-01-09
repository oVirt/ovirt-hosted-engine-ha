import copy
import os
from unittest import TestCase
import ovf2VmParams

OVF = open(
    os.path.join(
        os.path.dirname(__file__),
        'ovf_test.xml'
    )
).read()

OVF_w_max_vcpu = open(
    os.path.join(
        os.path.dirname(__file__),
        'ovf_test_max_vcpu.xml'
    )
).read()

EXPECTED_VM_CONF_DICT = {
    'vmId': '50e59bbd-f829-4761-ba1b-6db1a6acc3b8',
    'memSize': '4096',
    'display': 'vnc',
    'vmName': 'HostedEngine',
    'smp': '1',
    'cpuType': 'SandyBridge',
    'emulatedMachine': 'rhel6.5.0',
    'spiceSecureChannels': (
        'smain,sdisplay,sinputs,scursor,splayback,srecord,'
        'ssmartcard,susbredir'
    ),
    'devices': [
        {
            'index': '0',
            'iface': 'ide',
            'format': 'raw',
            'poolID': '00000000-0000-0000-0000-000000000000',
            'volumeID': '05f1031c-6c9a-48c3-9a62-930984e52e11',
            'imageID': '1b52d184-a370-48a7-8335-a4755b01ec0a',
            'readonly': 'false',
            'domainID': '475e7578-33a2-474f-8647-aa46f01200f9',
            'deviceId': '1b52d184-a370-48a7-8335-a4755b01ec0a',
            'address': (
                '{slot:0x06, bus:0x00, domain:0x0000, '
                'type:pci, function:0x0}'
            ),
            'device': 'disk',
            'type': 'disk',
            'bootOrder': '2',
            'propagateErrors': 'off',
            'shared': 'exclusive'
        },
        {
            'index': '2',
            'iface': 'ide',
            'address': '{ controller:0, target:0,unit:0, bus:1, type:drive}',
            'readonly': 'true',
            'deviceId': '8c3179ac-b322-4f5c-9449-c52e3665e0ae',
            'path': '',
            'device': 'cdrom',
            'shared': 'false',
            'type': 'disk'
        },
        {
            'device': 'scsi',
            'model': 'virtio-scsi',
            'type': 'controller',
            'address': (
                '{bus:0x00, slot:0x06, domain:0x0000, type:pci, '
                'function:0x0}'
            ),
            'deviceId': '243e6a18-ae41-47dc-af9e-29f2df1b3713'
        },
        {
            'nicModel': 'pv',
            'macAddr': '00:16:3e:43:ed:b5',
            'network': 'ovirtmgmt',
            'deviceId': '7442c4a6-c63d-4734-8a7e-3b2878efd7df',
            'address': (
                '{slot:0x03, bus:0x00, domain:0x0000, type:pci, '
                'function:0x0}'
            ),
            'device': 'bridge',
            'type': 'interface',
            'linkActive': 'true'
        },
        {
            'nicModel': 'pv',
            'macAddr': '00:16:3e:43:ed:b6',
            'network': 'dummynet',
            'deviceId': '7442c4a6-c63d-4734-8a7e-3b2878efd7ef',
            'address': (
                '{slot:0x04, bus:0x00, domain:0x0000, type:pci, '
                'function:0x0}'
            ),
            'device': 'bridge',
            'type': 'interface',
            'linkActive': 'false'
        },
        {
            'address': (
                '{slot:0x01, bus:0x00, domain:0x0000, type:pci, '
                'function:0x2}'
            ),
            'device': 'usb',
            'type': 'controller',
            'deviceId': '9ae636d9-f23f-4050-84c3-096c798b4851'
        },
        {
            'address': (
                '{slot:0x01, bus:0x00, domain:0x0000, type:pci, '
                'function:0x1}'
            ),
            'device': 'ide',
            'type': 'controller',
            'deviceId': '90cf38c4-ae28-45be-a38f-42f19ac83929'
        },
        {
            'address': (
                '{slot:0x05, bus:0x00, domain:0x0000, type:pci, '
                'function:0x0}'
            ),
            'device': 'virtio-serial',
            'type': 'controller',
            'deviceId': 'e354e922-b612-49ca-9474-adacc2dc388e'
        },
        {
            'address': (
                '{slot:0x02, bus:0x00, domain:0x0000, type:pci, '
                'function:0x0}'
            ),
            'device': 'cirrus',
            'type': 'video',
            'alias': 'video0',
            'deviceId': 'd3c45090-2863-4faa-a9c4-58fdd1d06f19',
        },
        {
            'address': (
                '{slot:0x07, bus:0x00, domain:0x0000, type:pci, '
                'function:0x0}'
            ),
            'device': 'virtio',
            'type': 'rng',
            'alias': 'rng0',
            'deviceId': 'e61f8f3b-8bf1-4b5f-9e3a-22d4e60a11ca',
            'specParams': {'source': 'urandom'}
        },
    ]
}

EXPECTED_VM_CONF_DICT_W_MAX_VCPU = copy.deepcopy(EXPECTED_VM_CONF_DICT)
EXPECTED_VM_CONF_DICT_W_MAX_VCPU['maxVCpus'] = '16'


class Ovf2vmConfTest(TestCase):

    def test_convert_to_dict(self):
        self.maxDiff = None
        self.assertDictEqual(
            EXPECTED_VM_CONF_DICT,
            ovf2VmParams.toDict(OVF))

    def test_convert_to_dict_with_max_vcpu(self):
        self.maxDiff = None
        self.assertDictEqual(
            EXPECTED_VM_CONF_DICT_W_MAX_VCPU,
            ovf2VmParams.toDict(OVF_w_max_vcpu))


# vim: expandtab tabstop=4 shiftwidth=4
