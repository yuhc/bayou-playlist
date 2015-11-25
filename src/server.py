#!/usr/bin/python3

import sys

class Server:

    def __init__(self, node_id):
        self.node_id = node_id


if __name__ == "__main__":
    cmd = sys.argv
    node_id = int(cmd[1])
    s = Server(node_id)
