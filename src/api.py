#!/usr/bin/python3

from threading import Thread, Lock
from server    import Server
from client    import Client

servers = {} # list of servers
clients = {} # list of clients

def joinServer(server_id):
    # the first server is also the primary
    p = subprocess.Popen(["./src/server.py",
                          str(server_id),
                          str(False) if servers else str(True)])
    servers[server_id] = p.pid
    if TERM_LOG:
        print("Server#", i, " pid:", p.pid, sep="")

    # connect to all other servers in the system
    for index in servers:
        if index != server_id:
            s.notify(index)




def retireServer(server_id):
    pass


def joinClient(client_id, server_id):
    pass


def breakConnection(id1, id2):
    pass


def restoreConnection(id1, id2):
    pass


def pause():
    pass


def start():
    pass


def stabilize():
    pass


def printLog(server_id):
    pass


def put(client_id, song_name, url):
    pass


def get(client_id, song_name):
    pass


def delete(client_id, song_name):
    pass
