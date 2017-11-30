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

OVF_42 = open(
    os.path.join(
        os.path.dirname(__file__),
        'ovf_test_v4.2.xml'
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
            'address': None,
            'device': 'spice',
            'type': 'graphics',
            'deviceId': '546172ad-e415-400b-b079-91a033efd4b1',
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
            'model': 'virtio',
            'device': 'virtio',
            'type': 'rng',
            'alias': 'rng0',
            'deviceId': 'e61f8f3b-8bf1-4b5f-9e3a-22d4e60a11ca',
            'specParams': {'source': 'urandom'}
        },
        {
            'device': 'console',
            'type': 'console',
        },
    ]
}

EXPECTED_VM_CONF_DICT_W_MAX_VCPU = copy.deepcopy(EXPECTED_VM_CONF_DICT)
EXPECTED_VM_CONF_DICT_W_MAX_VCPU['maxVCpus'] = '16'

EXPECTED_VM_CONF_DICT_42 = {
    'vmId': 'b4491372-8f8a-4e08-8524-f8fb986a548a',
    'vmName': 'HostedEngine',
    'cpuType': 'Haswell-noTSX',
    'devices': [
        {
            'address': '{type:pci, slot:0x06, bus:0x00, domain:0x0000,'
                       ' function:0x0}',
            'bootOrder': '1',
            'device': 'disk',
            'deviceId': 'add49103-89de-4a7e-b22f-9768a324b8f9',
            'domainID': '1f71b5f9-5cfa-459c-8ec2-ee48c9731f60',
            'format': 'cow',
            'iface': 'virtio',
            'imageID': 'add49103-89de-4a7e-b22f-9768a324b8f9',
            'index': '0',
            'poolID': '00000000-0000-0000-0000-000000000000',
            'propagateErrors': 'off',
            'readonly': 'false',
            'shared': 'exclusive',
            'type': 'disk',
            'volumeID': '81487a81-d27d-41b8-b3d1-139d38437f42'
        },
        {
            'address': '{type:pci, slot:0x03, bus:0x00, domain:0x0000,'
                       ' function:0x0}',
            'device': 'bridge',
            'deviceId': '542e02d2-b4fe-4241-8a27-1f7ba42dba7c',
            'linkActive': 'true',
            'macAddr': '00:16:3e:62:28:65',
            'network': 'ovirtmgmt',
            'nicModel': 'pv',
            'type': 'interface'
        },
        {
            'address': None,
            'alias': 'video0',
            'device': 'qxl',
            'deviceId': '8644313b-fb30-44f5-b01a-5e0555a5d9ac',
            'specParams': {
                'heads': '1',
                'ram': '65536',
                'vgamem': '16384',
                'vram': '32768'
            },
            'type': 'video'
        },
        {
            'address': None,
            'device': 'spice',
            'deviceId': '7115e321-26af-4b2c-a0bf-d468561164a6',
            'type': 'graphics'
        },
        {
            'address': '{ controller:0, target:0,unit:0, bus:1, type:drive}',
            'device': 'cdrom',
            'deviceId': '8c3179ac-b322-4f5c-9449-c52e3665e0ae',
            'iface': 'ide',
            'index': '2',
            'path': '',
            'readonly': 'true',
            'shared': 'false',
            'type': 'disk'
        },
        {
            'address': '{type:pci, slot:0x01, bus:0x00, domain:0x0000,'
                       ' function:0x1}',
            'device': 'ide',
            'deviceId': '7ec1353f-1d5e-4327-9ce6-06848b83492a',
            'specParams': {
                'index': '0'
            },
            'type': 'controller'
        },
        {
            'address': '{type:pci, slot:0x07, bus:0x00, domain:0x0000,'
                       ' function:0x0}',
            'alias': 'rng0',
            'device': 'virtio',
            'deviceId': '7f75c30c-7ece-48bc-8166-566809b543f9',
            'model': 'virtio',
            'specParams': {
                'source': 'urandom'
            },
            'type': 'rng'
        },
        {
            'address': '{type:pci, slot:0x01, bus:0x00, domain:0x0000,'
                       ' function:0x2}',
            'device': 'usb',
            'deviceId': 'd7a1852f-621a-40e9-a617-ae9d98efa5b1',
            'specParams': {
                'index': '0',
                'model': 'piix3-uhci'
            },
            'type': 'controller'
        },
        {
            'address': '{type:pci, slot:0x05, bus:0x00, domain:0x0000,'
                       ' function:0x0}',
            'device': 'virtio-serial',
            'deviceId': '5af61c67-0c82-4feb-a083-dcfcf670ad5a',
            'type': 'controller'
        },
        {
            'address': '{type:pci, slot:0x04, bus:0x00, domain:0x0000,'
                       ' function:0x0}',
            'device': 'scsi',
            'deviceId': '7a856b05-598f-46ec-996a-3c71ebb19014',
            'model': 'virtio-scsi',
            'type': 'controller'
        },
        {
            'device': 'console', 'type': 'console'
        }
    ],
    'display': 'qxl',
    'emulatedMachine': 'pc-i440fx-rhel7.3.0',
    'maxVCpus': '64',
    'memSize': '4',
    'smp': '4',
    'spiceSecureChannels': 'smain,sdisplay,sinputs,scursor,splayback,srecord,'
                           'ssmartcard,susbredir'
}


class Ovf2vmConfTest(TestCase):

    def test_convert_to_dict(self):
        self.maxDiff = None
        self.assertDictEqual(
            EXPECTED_VM_CONF_DICT,
            ovf2VmParams.toDict(OVF))

    def test_convert_to_dict_42(self):
        self.maxDiff = None
        self.assertDictEqual(
            EXPECTED_VM_CONF_DICT_42,
            ovf2VmParams.toDict(OVF_42))

    def test_convert_to_dict_with_max_vcpu(self):
        self.maxDiff = None
        self.assertDictEqual(
            EXPECTED_VM_CONF_DICT_W_MAX_VCPU,
            ovf2VmParams.toDict(OVF_w_max_vcpu))


# vim: expandtab tabstop=4 shiftwidth=4
