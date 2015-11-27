#!/usr/bin/python3

import sys

class Server:

    def __init__(self, node_id, is_primary):
        self.node_id    = node_id
        self.unique_id  = (None, node_id)  # replica_id in lecture note

        self.is_primary = is_primary # first created server is the primary
        self.is_retired = False

        self.version_vector = {} # <server, clock>
        self.version_vector[self.unique_id] = 1
        self.CSN = 0            # commit sequence number
        self.accept_time = 0

        self.committed_log = []
        self.tentative_log = []

        self.nt = Network

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
    s = Server(node_id)
