#!/usr/bin/python3

import string, sys

from network   import Network
from threading import Thread, Lock

TERM_LOG = True

class Client:

    def __init__(self, node_id):
        self.node_id    = node_id
        self.uid        = "Client#" + str(node_id)

        self.read_set = -1 # session guarantees

        # create the network controller
        self.connected_server # which server is connected
        self.nt = Network(self.uid)
        try:
            self.t_recv = Thread(target=self.receive)
            self.t_recv.daemon = True
            self.t_recv.start()
        except:
            print(self.uid, "error: unable to start new thread")

    def receive(self):
        while 1:
            buf = self.nt.receive()
            if buf:
                if TERM_LOG:
                    print(self.uid, "handles:", str(buf))
                # TODO: parse buf

                if buf.mtype == "Put":
                    w = Write(self.node_id, "Put", None, None, buf.content)
                    m_put = Message(self.node_id, None, "Write", w)
                    self.nt.send_to_node(self.connected_server, m_put)

                elif buf.mtype == "Get":
                    m_get = Message(self.node_id, None, "Get", buf.content)
                    self.nt.send_to_node(self.connected_server, m_put)

                elif buf.mtype == "Delete":
                    w = Write(self.node_id, "Delete", None, None, buf.content)
                    m_delete = Message(self.node_id, None, "Write", w)
                    self.nt.send_to_node(self.connected_server, m_delete)

                elif buf.mtype == "GetAck":
                    (song_name, song_url, server_CSN) = buf.content
                    if (self.readset > server_CSN):
                        print song_name+":ERR_DEP"
                    else:
                        self.read_set = server_CSN
                        print song_name+":"+song_url
                    

if __name__ == "__main__":
    cmd= sys.argv
    node_id = int(cmd[1])
    c = Client(node_id)
    if TERM_LOG:
        print(c.uid, "started")
    c.t_recv.join()
