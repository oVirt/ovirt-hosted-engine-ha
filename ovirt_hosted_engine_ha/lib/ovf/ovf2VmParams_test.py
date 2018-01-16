import base64
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


EXPECTED_LIBVIRT_XML = \
    '<?xml version="1.0" encoding="UTF-8"?><domain type="kvm" ' \
    'xmlns:ovirt-tune="http://ovirt.org/vm/tune/1.0" ' \
    'xmlns:ovirt-vm="http://ovirt.org/vm/1.0"><name>vm</name><uuid>b36a34f1' \
    '-d1c2-4585-ae8f-33e05e4d3c19</uuid><memory>1048576</memory>' \
    '<currentMemory>1048576</currentMemory><maxMemory ' \
    'slots="16">4194304</maxMemory><vcpu current="2">16</vcpu><sysinfo ' \
    'type="smbios"><system><entry name="manufacturer">oVirt</entry><entry ' \
    'name="product">OS-NAME:</entry><entry ' \
    'name="version">OS-VERSION:</entry><entry ' \
    'name="serial">HOST-SERIAL:</entry><entry ' \
    'name="uuid">b36a34f1-d1c2-4585-ae8f-33e05e4d3c19</entry></system>' \
    '</sysinfo><clock offset="variable" adjustment="0"><timer name="rtc" ' \
    'tickpolicy="catchup"></timer><timer name="pit" ' \
    'tickpolicy="delay"></timer><timer name="hpet" ' \
    'present="no"></timer></clock><features><acpi></acpi>' \
    '</features><cpu match="exact"><model>SandyBridge</model><topology ' \
    'cores="1" threads="1" sockets="16"></topology><numa><cell cpus="0,' \
    '1" memory="1048576"></cell></numa></cpu><cputune></cputune>' \
    '<devices><input type="mouse" bus="ps2"></input><channel ' \
    'type="unix"><target type="virtio" ' \
    'name="ovirt-guest-agent.0"></target><source mode="bind" ' \
    'path="/var/lib/libvirt/qemu/channels/' \
    'b36a34f1-d1c2-4585-ae8f-33e05e4d3c19.ovirt-guest-agent.0' \
    '"></source></channel><channel type="unix"><target type="virtio" ' \
    'name="org.qemu.guest_agent.0"></target><source mode="bind" ' \
    'path="/var/lib/libvirt/qemu/channels/b36a34f1-d1c2-4585-ae8f-' \
    '33e05e4d3c19.org.qemu' \
    '.guest_agent.0"></source></channel><video><model type="qxl" ' \
    'vram="8192" heads="1" ram="65536" vgamem="16384"></model>' \
    '<address bus="0x00" domain="0x0000" function="0x0" slot="0x02" ' \
    'type="pci"></address></video><rng model="virtio"><backend ' \
    'model="random">/dev/urandom</backend></rng>' \
    '<controller type="virtio-serial" index="0" ports="16">' \
    '<address bus="0x00" domain="0x0000" function="0x0" slot="0x05" ' \
    'type="pci"></address></controller><graphics type="spice" port="-1" ' \
    'autoport="yes" passwd="*****" passwdValidTo="1970-01-01T00:00:01"' \
    ' tlsPort="-1"><listen type="network" network="vdsm-ovirtmgmt">' \
    '</listen></graphics><controller type="scsi" model="virtio-scsi" ' \
    'index="0"><address bus="0x00" domain="0x0000" ' \
    'function="0x0" slot="0x04" type="pci"></address></controller>' \
    '<controller type="ide" index="0"><address bus="0x00" domain="0x0000"' \
    ' function="0x1" slot="0x01" type="pci"></address></controller>' \
    '<controller type="usb" model="piix3-uhci" ' \
    'index="0"><address bus="0x00" domain="0x0000" function="0x2" ' \
    'slot="0x01" type="pci"></address></controller><memballoon ' \
    'model="virtio"><stats period="5"></stats><address bus="0x00" ' \
    'domain="0x0000" function="0x0" slot="0x06" ' \
    'type="pci"></address></memballoon><channel type="spicevmc"><target ' \
    'type="virtio" name="com.redhat.spice.0"></target></channel><interface ' \
    'type="bridge"><model type="virtio"></model><link ' \
    'state="up"></link><source bridge="ovirtmgmt"></source><address ' \
    'bus="0x00" domain="0x0000" function="0x0" slot="0x03" type="pci">' \
    '</address><mac address="00:1a:4a:16:01:01"></mac><filterref ' \
    'filter="vdsm-no-mac-spoofing"></filterref><bandwidth></bandwidth>' \
    '</interface><disk type="file" device="cdrom" snapshot="no">' \
    '<driver name="qemu" type="raw" error_policy="report"></driver>' \
    '<source file="" startupPolicy="optional"></source><target dev="hdc" ' \
    'bus="ide"></target><readonly></readonly><address bus="1" ' \
    'controller="0" unit="0" type="drive" target="0"></address></disk>' \
    '<disk snapshot="no" type="file" device="disk"><target dev="sda" ' \
    'bus="scsi"></target><source file="/rhev/data-center/' \
    '5a38bfb2-00d2-02f9-0120-0000000002af/a2ec0146-37d4-4c1d-901e-' \
    '7ed89cd009ff/images/73a433b7-2a84-49d0-a80f-207ff91930f3/596f868e' \
    '-6519-456d-96a0-b78a1b2eea91"></source><driver name="qemu" ' \
    'io="threads" type="raw" error_policy="stop" cache="none"></driver>' \
    '<address bus="0" controller="0" unit="0" type="drive" target="0">' \
    '</address><boot ' \
    'order="1"></boot><serial>73a433b7-2a84-49d0-a80f-207ff91930f3</serial>' \
    '</disk></devices><pm><suspend-to-disk ' \
    'enabled="no"></suspend-to-disk><suspend-to-mem ' \
    'enabled="no"></suspend-to-mem></pm><os><type arch="x86_64" ' \
    'machine="pc-i440fx-2.6">hvm</type><smbios ' \
    'mode="sysinfo"></smbios></os><metadata><ovirt-tune:qos></ovirt' \
    '-tune:qos><ovirt-vm:vm><minGuaranteedMemoryMb ' \
    'type="int">1024</minGuaranteedMemoryMb><clusterVersion>4.2' \
    '</clusterVersion><ovirt-vm:custom></ovirt-vm:custom><ovirt-vm:device ' \
    'mac_address="00:1a:4a:16:01:01"><ovirt-vm:custom></ovirt-vm:custom>' \
    '</ovirt-vm:device><ovirt-vm:device devtype="disk" ' \
    'name="sda"><ovirt-vm:imageID>73a433b7-2a84-49d0-a80f-207ff91930f3' \
    '</ovirt-vm:imageID><ovirt-vm:poolID>5a38bfb2-00d2-02f9-0120-' \
    '0000000002af</ovirt-vm:poolID><ovirt-vm:volumeID>596f868e-6519-456d-' \
    '96a0-b78a1b2eea91</ovirt-vm:volumeID><ovirt-vm:domainID>a2ec0146-37d4-' \
    '4c1d-901e-7ed89cd009ff</ovirt-vm:domainID>' \
    '</ovirt-vm:device><launchPaused>false</launchPaused><resumeBehavior>' \
    'auto_resume</resumeBehavior></ovirt-vm:vm></metadata></domain>'

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
    'memSize': '4096',
    'smp': '4',
    'spiceSecureChannels': 'smain,sdisplay,sinputs,scursor,splayback,srecord,'
                           'ssmartcard,susbredir',
    "xmlBase64": base64.standard_b64encode(EXPECTED_LIBVIRT_XML)
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
