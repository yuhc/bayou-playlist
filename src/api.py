#!/usr/bin/python3

import subprocess, sys, os, signal, time

from threading import Thread, Lock
from server    import Server
from client    import Client
from message   import Message
from network   import Network

TERM_LOG = True

nodes   = [] # list of nodes
servers = {} # list of servers
clients = {} # list of clients

uid = "Master#0"
nt  = Network(uid)

def joinServer(server_id):
    if not server_id in nodes:
        # the first server is also the primary
        p = subprocess.Popen(["./src/server.py",
                              str(server_id),
                              str(False) if servers else str(True)])
        servers[server_id] = p.pid
        nodes.append(server_id)
        #TODO: wait for ack
        if TERM_LOG:
            print("Server#", server_id, " pid:", p.pid, sep="")
        time.sleep(2)
        # connect to a server in the system
        for index in servers:
            if index != server_id:
                m_create = Message(-1, None, "Creation", None)
                nt.send_to_node(server_id, m_create)
                break


def retireServer(server_id):
    if servers[server_id]:
        m_retire = Message(-1, None, "Retire", None)
        nt.send_to_server(server_id, m_retire)
        # TODO: block until it is able to tell another server of its retirement
        nodes.remove(server_id)
        servers.pop(server_id)


def joinClient(client_id, server_id):
    if not client_id in nodes:
        if servers[server_id]:
            p = subprocess.Popen(["./src/client.py",
                                  str(client_id)])
            clients[client_id] = p.pid
            nodes.append(client_id)
            #TODO: wait for ack
            if TERM_LOG:
                print("Client#", client_id, " pid:", p.pid, sep="")

            m_join = Message(-1, None, "Join", server_id)
            nt.send_to_node(client_id, m_join)
        else:
            if TERM_LOG:
                print("Server#", server_id, " has not started", sep="")
    else:
        if TERM_LOG:
            print("ID#", client_id, " has been occuiped", sep="")


def breakConnection(id1, id2):
    if id1 in nodes and id2 in nodes:
        m_break = Message(-1, None, "Break", id1)
        nt.send_to_node(id2, m_break)
        m_break = Message(-1, None, "Break", id2)
        nt.send_to_node(id1, m_break)


def restoreConnection(id1, id2):
    if id1 in nodes and id2 in nodes:
        m_break = Message(-1, None, "Restore", id1)
        nt.send_to_node(id2, m_break)
        m_break = Message(-1, None, "Restore", id2)
        nt.send_to_node(id1, m_break)


def pause():
    for index in servers:
        m_pause = Message(-1, None, "Pause", None)
        nt.send_to_node(index, m_pause)


def start():
    for index in servers:
        m_start = Message(-1, None, "Start", None)
        nt.send_to_node(index, m_start)


def stabilize():
    pass


def printLog(server_id):
    if server_id in servers:
        m_print = Message(-1, None, "Print", None)
        nt.send_to_node(server_id, m_print)
        # TODO: block to wait


def put(client_id, song_name, url):
    if client_id in clients:
        m_put = Message(-1, None, "Put", song_name + ' ' + url)
        nt.send_to_node(client_id, m_put)
        # TODO: block


def get(client_id, song_name):
    if client_id in clients:
        m_get = Message(-1, None, "Get", song_name)
        nt.send_to_node(client_id, m_get)
        # TODO: block


def delete(client_id, song_name):
    if client_id in clients:
        m_delete = Message(-1, None, "Delete", song_name)
        nt.send_to_node(client_id, m_delete)
        # TODO: block

def exit():
    # kill the remained nodes and clients
    for i in servers:
        if servers[i] != None:
            os.kill(servers[i], signal.SIGKILL)
            if TERM_LOG:
                print("Servers#", i, " stopped", sep="")
    for i in clients:
        if clients[i] != None:
            os.kill(clients[i], signal.SIGKILL)
            if TERM_LOG:
                print("Clients#", i, " stopped", sep="")
    sys.exit()
