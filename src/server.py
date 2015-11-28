#!/usr/bin/python3

import sys, string

from network   import Network
from threading import Thread, Lock

TERM_LOG = True

class Server:

    def __init__(self, node_id, is_primary):
        self.node_id    = node_id
        self.uid        = "Server#" + str(node_id)
        self.unique_id  = (None, node_id)  # replica_id in lecture note

        self.is_primary = is_primary # first created server is the primary
        self.is_retired = False
        self.is_paused  = False

        self.version_vector = {} # <server, clock>
        self.version_vector[self.unique_id] = 1

        self.playlist = {}

        self.CSN = 0            # commit sequence number
        self.accept_time = 0

        self.committed_log = []
        self.tentative_log = {} # dictionary of lists, index: unique_id

        # create the network controller
        self.connections = set() # whether could connect
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

    '''
    notify server @dest_id about its joining. '''
    def notify(self, dest_id):
        m_join_server = Message(server_id, None, "Creation", None)
        self.nt.send_to_server(dest_id, m_join_server)
        # TODO: while wait-for-ack

    def __str__(self):
        return "Server #" + self.node_id + "#" + self.unique_id


if __name__ == "__main__":
    cmd = sys.argv
    node_id = int(cmd[1])
    is_primary = (cmd[2] == "True")
    s = Server(node_id, is_primary)
    if TERM_LOG:
        print(s.uid, "started")
    s.t_recv.join()
