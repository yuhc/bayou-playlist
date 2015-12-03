#!/usr/bin/python3

import string, sys, threading

from network   import Network
from threading import Thread, Lock, Condition
from message   import AntiEntropy, Write, Message
from config    import Config

TERM_LOG = Config.client_log

c_can_send_to_server = Condition()

class Client:

    def __init__(self, node_id):
        self.node_id    = node_id
        self.uid        = "Client#" + str(node_id)

        self.read_set = {} # session guarantees

        # create the network controller
        self.connected_server   = None # which server is connected
        self.nt                 = Network(self.uid)
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
                    w = Write(self.node_id, None, "Put", None, 0,
                              buf.content)
                    m_put = Message(self.node_id, None, "Write", w)
                    c_can_send_to_server.acquire()
                    while True:
                        if self.can_send_to_server:
                            break
                        c_can_send_to_server.wait()
                    self.nt.send_to_node(self.connected_server, m_put)
                    c_can_send_to_server.release()

                elif buf.mtype == "Get":
                    m_get = Message(self.node_id, None, "Get", buf.content)
                    if TERM_LOG:
                        print(self.uid, "tries to acquire c_can_send_to_server in receive.Get")
                    c_can_send_to_server.acquire()
                    if TERM_LOG:
                        print(self.uid, "acquires c_can_send_to_server in receive.Get")
                    while True:
                        if self.can_send_to_server:
                            break
                        c_can_send_to_server.wait()
                    self.nt.send_to_node(self.connected_server, m_get)
                    c_can_send_to_server.release()
                    if TERM_LOG:
                        print(self.uid, "releases c_can_send_to_server in receive.Get")

                elif buf.mtype == "Delete":
                    w = Write(self.node_id, None, "Delete", None, 0,
                              buf.content)
                    m_delete = Message(self.node_id, None, "Write", w)
                    c_can_send_to_server.acquire()
                    while True:
                        if self.can_send_to_server:
                            break
                        c_can_send_to_server.wait()
                    self.nt.send_to_node(self.connected_server, m_delete)
                    c_can_send_to_server.release()

                elif buf.mtype == "GetAck":
                    (song_name, song_url, server_vv) = buf.content
                    get_content = ""
                    # print("read set: " + str(self.read_set) +
                    #       "server_accept_time: " + str(server_vv))
                    err_dep = False
                    union_keys = set(self.read_set.keys())\
                    .union(server_vv.keys())
                    for i in union_keys:
                        if not i in server_vv:
                            get_content = song_name+":ERR_DEP"
                            err_dep = True
                            break
                        elif i in self.read_set and \
                             self.read_set[i] > server_vv[i]:
                            get_content = song_name+":ERR_DEP"
                            err_dep = True
                            break
                    if not err_dep:
                        get_content = song_name+":"+song_url
                    for i in union_keys:
                        if i in server_vv and i in self.read_set and\
                        self.read_set[i] < server_vv[i]:
                            self.read_set[i] = server_vv[i]
                        if not i in self.read_set:
                            self.read_set[i] = server_vv[i]
                    m_get_msg = Message(self.node_id, None, "MGetAck", get_content)
                    self.nt.send_to_master(m_get_msg)

                elif buf.mtype == "Done":
                    # print("read set: " + str(self.read_set) + "server_accept_time: " + str(buf.content))
                    union_keys = set(self.read_set.keys())\
                    .union(buf.content.keys())
                    for i in union_keys:
                        if i in buf.content and i in self.read_set and\
                        self.read_set[i] < buf.content[i]:
                            self.read_set[i] = buf.content[i]
                        if not i in self.read_set:
                            self.read_set[i] = buf.content[i]

                    done = Message(self.node_id, None, "Done", None)
                    self.nt.send_to_master(done)

                elif buf.mtype == "Join":
                    self.connected_server   = buf.content

                elif buf.mtype == "Break":
                    c_can_send_to_server.acquire()
                    self.can_send_to_server = False
                    c_can_send_to_server.release()

                elif buf.mtype == "Restore":
                    c_can_send_to_server.acquire()
                    self.can_send_to_server = True
                    self.connected_server   = buf.content[1]
                    c_can_send_to_server.notify()
                    c_can_send_to_server.release()


if __name__ == "__main__":
    cmd= sys.argv
    node_id = int(cmd[1])
    c = Client(node_id)
    if TERM_LOG:
        print(c.uid, "started")
    c.t_recv.join()
