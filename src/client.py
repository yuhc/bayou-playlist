#!/usr/bin/python3

import string, sys

from network   import Network
from threading import Thread, Lock, Condition

TERM_LOG = True
c_send_to_server = Condition()

class Client:

    def __init__(self, node_id):
        self.node_id    = node_id
        self.uid        = "Client#" + str(node_id)

        self.read_set = -1 # session guarantees

        # create the network controller
        self.connected_server # which server is connected
        self.nt = Network(self.uid)
        self.can_send_to_server = True
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
                    w = Write(self.node_id, None, "Put", None, None,
                              buf.content)
                    m_put = Message(self.node_id, None, "WritePut", w)
                    c_can_send_to_server.acquire()
                    while True:
                        if self.can_send_to_server:
                            break
                        c_can_send_to_server.wait()
                    c_can_send_to_server.release()
                    self.nt.send_to_node(self.connected_server, m_put)

                elif buf.mtype == "Get":
                    m_get = Message(self.node_id, None, "Get", buf.content)
                    c_can_send_to_server.acquire()
                    while True:
                        if self.can_send_to_server:
                            break
                        c_can_send_to_server.wait()
                    c_can_send_to_server.release()
                    self.nt.send_to_node(self.connected_server, m_get)

                elif buf.mtype == "Delete":
                    w = Write(self.node_id, None, "Delete", None, None,
                              buf.content)
                    m_delete = Message(self.node_id, None, "WriteDelete", w)
                    c_can_send_to_server.acquire()
                    while True:
                        if self.can_send_to_server:
                            break
                        c_can_send_to_server.wait()
                    c_can_send_to_server.release()
                    self.nt.send_to_node(self.connected_server, m_delete)

                elif buf.mtype == "GetAck":
                    (song_name, song_url, server_CSN) = buf.content
                    get_content = ""
                    if (self.readset > server_CSN):
                        get_content = song_name+":ERR_DEP"
                    else:
                        self.read_set = server_CSN
                        get_content = song_name+":"+song_url
                    m_get_msg = Message(self.node_id, None, "MGetAck", get_content)
                    self.nt.send_to_master(m_get_msg)

                elif buf.mtype == "Break":
                    c_can_send_to_server.acquire()
                    self.can_send_to_server = False
                    c_can_send_to_server.release()

                elif buf.mtype == "Restore":
                    c_can_send_to_server.acquire()
                    self.can_send_to_server = True
                    c_can_send_to_server.notify()
                    c_can_send_to_server.release()


if __name__ == "__main__":
    cmd= sys.argv
    node_id = int(cmd[1])
    c = Client(node_id)
    if TERM_LOG:
        print(c.uid, "started")
    c.t_recv.join()
