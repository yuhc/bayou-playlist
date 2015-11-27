#!/usr/bin/python3

import sys

class Client:

    def __init__(self, client_id):
        self.client_id = client_id

if __name__ == "__main__":
    cmd= sys.argv
    node_id = int(cmd[1])
    c = Client(node_id)

