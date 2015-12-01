#!/usr/bin/python3

import subprocess, sys, os, signal, time

from threading import Thread, Lock, Condition
from server    import Server
from client    import Client
from message   import Message
from network   import Network
from config    import Config

TERM_LOG  = Config.master_log
CMD_DEBUG = Config.master_cmd

STABILIZE_TIME = 2

nodes   = [] # list of nodes
servers = {} # list of servers
clients = {} # list of clients

has_received_log = False
has_received_res = False
has_retired_res = False
c_has_received_res = Condition()

uid = "Master#0"
nt  = Network(uid)

def receive():
    global has_received_res
    global c_has_received_res
    global has_retired_res
    global has_received_log
    while 1:
        buf = nt.receive()
        if buf:
            if TERM_LOG:
                print(uid, "handles:", str(buf))

            if buf.mtype == "Playlist":
                print(uid, buf.content)
                has_received_log = True
            elif buf.mtype == "MGetAck":
                print(uid, buf.content)
                c_has_received_res.acquire()
                has_received_res = True
                c_has_received_res.notify()
                c_has_received_res.release()
            elif buf.mtype == "Done": # done processing put/delete
                c_has_received_res.acquire()
                print("XXXXXXXXXXXXXXXXXX Receives Done")
                has_retired_res = True
                c_has_received_res.notify()
                c_has_received_res.release()
            elif buf.mtype == "RetireAck":
                c_has_received_res.acquire()
                has_retired_res = True
                c_has_received_res.notify()
                c_has_received_res.release()

try:
    t_recv = Thread(target=receive)
    t_recv.daemon = True
    t_recv.start()
except:
    print(uid, "error: unable to start new thread")

def joinServer(server_id):
    if not server_id in nodes:
        # the first server is also the primary
        p = subprocess.Popen(["./src/server.py",
                              str(server_id),
                              str(False) if servers else str(True)])
        servers[server_id] = p.pid
        nodes.append(server_id)
        #TODO: wait for ack
        time.sleep(1)
        if TERM_LOG:
            print("Server#", server_id, " pid:", p.pid, sep="")

        # connect to a server in the system
        for index in servers:
            if index != server_id:
                m_create = Message(-1, None, "Creation", index)
                nt.send_to_node(server_id, m_create)
                break


def retireServer(server_id):
    if servers[server_id]:
        m_retire = Message(-1, None, "Retire", None)
        nt.send_to_server(server_id, m_retire)
        while not has_retired_res:
            pass
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
            time.sleep(1)
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
        if id1 in server_list:
            m_break = Message(-1, None, "Restore", ("Server", id1))
        else:
            m_break = Message(-1, None, "Restore", ("Client", id1))
        nt.send_to_node(id2, m_break)
        if id2 in server_list:
            m_break = Message(-1, None, "Restore", ("Server", id2))
        else:
            m_break = Message(-1, None, "Restore", ("Client", id2))
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
    time.sleep(STABILIZE_TIME*len(server_list))


def printLog(server_id):
    if server_id in servers:
        m_print = Message(-1, None, "Print", None)
        has_received_log = False
        nt.send_to_node(server_id, m_print)
        while not has_received_log:
            pass


def put(client_id, song_name, url):
    if client_id in clients:
        m_put = Message(-1, None, "Put", song_name + ' ' + url)
        has_received_res = False
        nt.send_to_node(client_id, m_put)
        # while not has_received_res:
        #     pass
        c_has_received_res.acquire()
        while True:
            if has_received_res:
                break
            c_has_received_res.wait()
        print("XXXXXXXXXXXXXX Done")
        c_has_received_res.release()


def get(client_id, song_name):
    if client_id in clients:
        m_get = Message(-1, None, "Get", song_name)
        has_received_res = False
        nt.send_to_node(client_id, m_get)
        # while not has_received_res:
        #     pass
        c_has_received_res.acquire()
        while True:
            if has_received_res:
                break
            c_has_received_res.wait()
        c_has_received_res.release()

def delete(client_id, song_name):
    if client_id in clients:
        m_delete = Message(-1, None, "Delete", song_name)
        has_received_res = False
        nt.send_to_node(client_id, m_delete)
        # while not has_received_res:
        #     pass
        c_has_received_res.acquire()
        while True:
            if has_received_res:
                break
            c_has_received_res.wait()
        c_has_received_res.release()


def exit():
    # kill the remained nodes and clients
    for i in servers:
        if servers[i] != None:
            os.kill(servers[i], signal.SIGKILL)
            if TERM_LOG:
                print("Server#", i, " stopped", sep="")
    for i in clients:
        if clients[i] != None:
            os.kill(clients[i], signal.SIGKILL)
            if TERM_LOG:
                print("Client#", i, " stopped", sep="")
    sys.exit()
