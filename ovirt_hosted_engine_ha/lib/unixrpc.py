import os
try:
    import socketserver
except ImportError:
    import SocketServer as socketserver
try:
    import xmlrpc.server as xmlrpc_server
except ImportError:
    import SimpleXMLRPCServer as xmlrpc_server
try:
    from xmlrpc.client import ServerProxy, Transport
except ImportError:
    from xmlrpclib import ServerProxy, Transport
try:
    import http.client as http_client
except ImportError:
    import httplib as http_client
import socket
import base64


class UnixXmlRpcHandler(xmlrpc_server.SimpleXMLRPCRequestHandler):
    disable_nagle_algorithm = False


# This class implements a XML-RPC server that binds to a UNIX socket. The path
# to the UNIX socket to create methods must be provided.
class UnixXmlRpcServer(socketserver.UnixStreamServer,
                       xmlrpc_server.SimpleXMLRPCDispatcher):
    address_family = socket.AF_UNIX
    allow_address_reuse = True

    def __init__(self, sock_path, request_handler=UnixXmlRpcHandler,
                 logRequests=0):
        if os.path.exists(sock_path):
            os.unlink(sock_path)
        self.logRequests = logRequests
        xmlrpc_server.SimpleXMLRPCDispatcher.__init__(self, encoding=None,
                                                      allow_none=1)
        socketserver.UnixStreamServer.__init__(self, sock_path,
                                               request_handler)


# This class implements a XML-RPC client that connects to a UNIX socket. The
# path to the UNIX socket to create must be provided.
class UnixXmlRpcClient(ServerProxy):
    def __init__(self, sock_path, timeout=None):
        # We can't pass funny characters in the host part of a URL, so we
        # encode the socket path in base16.
        ServerProxy.__init__(
            self,
            'http://' + base64.b16encode(sock_path.encode()).decode(),
            transport=UnixXmlRpcTransport(timeout=timeout),
            allow_none=1,
        )


class UnixXmlRpcTransport(Transport):
    def __init__(self, timeout=None, *args, **kwargs):
        Transport.__init__(self, *args, **kwargs)
        self.timeout = timeout

    def make_connection(self, host):
        return UnixXmlRpcHttpConnection(host=host, timeout=self.timeout)


class UnixXmlRpcHttpConnection(http_client.HTTPConnection):
    def __init__(self, timeout=None, *args, **kwargs):
        http_client.HTTPConnection.__init__(self, *args, **kwargs)
        self.timeout = timeout

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.connect(base64.b16decode(self.host))
        if self.timeout is not None:
            self.sock.settimeout(self.timeout)
