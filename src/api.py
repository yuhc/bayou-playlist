#!/usr/bin/python3

from threading import Thread, Lock
from server    import Server
from client    import Client
from message   import Message

servers = {} # list of servers
clients = {} # list of clients

uid = "Master#0"
nt  = Network(uid)

def joinServer(server_id):
    # the first server is also the primary
    p = subprocess.Popen(["./src/server.py",
                          str(server_id),
                          str(False) if servers else str(True)])
    servers[server_id] = p.pid
    if TERM_LOG:
        print("Server#", i, " pid:", p.pid, sep="")

    # connect to a server in the system
    for index in servers:
        if index != server_id:
            m_create = Message(-1, None, "Creation", None)
            nt.send_to_server(server_id, m_create)
            break


def retireServer(server_id):
    if servers[server_id]:
        m_retire = Message(-1, None, "Retire", None)
        nt.send_to_server(server_id, m_retire)
        # TODO: block until it is able to tell another server of its retirement
        servers[server_id] = None


def joinClient(client_id, server_id):
    if cliens[client_id] and servers[server_id]:
        m_join = Message(-1, None, "Join", server_id)
        nt.send_to_client(client_id, m_join)


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
