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

if __name__ == "__main__":
    cmd= sys.argv
    node_id = int(cmd[1])
    c = Client(node_id)
    if TERM_LOG:
        print(c.uid, "started")
    c.t_recv.join()
