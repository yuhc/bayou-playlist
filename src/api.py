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

class API:

    STABILIZE_TIME = 2

    def __init__(self):
        self.nodes   = [] # list of nodes
        self.servers = {} # list of servers
        self.clients = {} # list of clients

        self.has_received_log   = False
        self.has_received_res   = False
        self.has_retired_res    = False
        self.c_has_received_res = Condition()

        self.uid = "Master#0"
        self.nt  = Network(self.uid)
        try:
            self.t_recv = Thread(target=self.receive)
            self.t_recv.daemon = True
            self.t_recv.start()
        except:
            print(uid, "error: unable to start new thread")

    def receive(self):
        while 1:
            buf = self.nt.receive()
            if buf:
                if TERM_LOG:
                    print(self.uid, "handles:", str(buf))

                if buf.mtype == "Playlist":
                    print(self.uid, buf.content)
                    self.has_received_log = True

                elif buf.mtype == "MGetAck":
                    print(self.uid, buf.content)
                    self.c_has_received_res.acquire()
                    self.has_received_res = True
                    self.c_has_received_res.notify()
                    self.c_has_received_res.release()

                elif buf.mtype == "Done": # done processing put/delete
                    self.c_has_received_res.acquire()
                    self.has_retired_res = True
                    print("in receive done", self.has_retired_res)
                    self.c_has_received_res.notify()
                    print("in receive done", self.has_retired_res)
                    self.c_has_received_res.release()

                elif buf.mtype == "RetireAck":
                    self.c_has_received_res.acquire()
                    self.has_retired_res = True
                    self.c_has_received_res.notify()
                    self.c_has_received_res.release()


    def joinServer(self, server_id):
        if not server_id in self.nodes:
            # the first server is also the primary
            p = subprocess.Popen(["./src/server.py",
                                  str(server_id),
                                  str(False) if self.servers else str(True)])
            self.servers[server_id] = p.pid
            self.nodes.append(server_id)
            #TODO: wait for ack
            time.sleep(1)
            if TERM_LOG:
                print("Server#", server_id, " pid:", p.pid, sep="")

            # connect to a server in the system
            for index in self.servers:
                if index != server_id:
                    m_create = Message(-1, None, "Creation", index)
                    self.nt.send_to_node(server_id, m_create)
                    break


    def retireServer(self, server_id):
        if self.servers[server_id]:
            m_retire = Message(-1, None, "Retire", None)
            self.nt.send_to_server(server_id, m_retire)
            while not self.has_retired_res:
                pass
            self.nodes.remove(server_id)
            self.servers.pop(server_id)


    def joinClient(self, client_id, server_id):
        if not client_id in self.nodes:
            if self.servers[server_id]:
                p = subprocess.Popen(["./src/client.py",
                                      str(client_id)])
                self.clients[client_id] = p.pid
                self.nodes.append(client_id)
                #TODO: wait for ack
                time.sleep(1)
                if TERM_LOG:
                    print("Client#", client_id, " pid:", p.pid, sep="")

                m_join = Message(-1, None, "Join", server_id)
                self.nt.send_to_node(client_id, m_join)
            else:
                if TERM_LOG:
                    print("Server#", server_id, " has not started", sep="")
        else:
            if TERM_LOG:
                print("ID#", client_id, " has been occuiped", sep="")


    def breakConnection(self, id1, id2):
        if id1 in self.nodes and id2 in self.nodes:
            m_break = Message(-1, None, "Break", id1)
            self.nt.send_to_node(id2, m_break)
            m_break = Message(-1, None, "Break", id2)
            self.nt.send_to_node(id1, m_break)


    def restoreConnection(self, id1, id2):
        if id1 in self.nodes and id2 in self.nodes:
            if id1 in self.server_list:
                m_break = Message(-1, None, "Restore", ("Server", id1))
            else:
                m_break = Message(-1, None, "Restore", ("Client", id1))
            self.nt.send_to_node(id2, m_break)
            if id2 in self.server_list:
                m_break = Message(-1, None, "Restore", ("Server", id2))
            else:
                m_break = Message(-1, None, "Restore", ("Client", id2))
            self.nt.send_to_node(id1, m_break)


    def pause(self):
        for index in self.servers:
            m_pause = Message(-1, None, "Pause", None)
            self.nt.send_to_node(index, m_pause)


    def start(self):
        for index in self.servers:
            m_start = Message(-1, None, "Start", None)
            self.nt.send_to_node(index, m_start)


    def stabilize(self):
        time.sleep(STABILIZE_TIME*len(server_list))


    def printLog(self, server_id):
        if server_id in self.servers:
            m_print = Message(-1, None, "Print", None)
            self.has_received_log = False
            self.nt.send_to_node(server_id, m_print)
            while not self.has_received_log:
                pass


    def put(self, client_id, song_name, url):
        if client_id in self.clients:
            m_put = Message(-1, None, "Put", song_name + ' ' + url)
            print("put before", self.has_received_res)
            self.has_received_res = False
            self.nt.send_to_node(client_id, m_put)
            print("after before", self.has_received_res)
            # while not self.has_received_res:
            #     pass
            self.c_has_received_res.acquire()
            print("acquires lock")
            while True:
                if self.has_received_res:
                    break
                self.c_has_received_res.wait()
                print("in loop", self.has_received_res)
            print("XXXXXXXXXXXXXX Done")
            self.c_has_received_res.release()


    def get(self, client_id, song_name):
        if client_id in self.clients:
            m_get = Message(-1, None, "Get", song_name)
            self.has_received_res = False
            self.nt.send_to_node(client_id, m_get)
            # while not self.has_received_res:
            #     pass
            self.c_has_received_res.acquire()
            while True:
                if self.has_received_res:
                    break
                self.c_has_received_res.wait()
            self.c_has_received_res.release()

    def delete(self, client_id, song_name):
        if client_id in self.clients:
            m_delete = Message(-1, None, "Delete", song_name)
            self.has_received_res = False
            self.nt.send_to_node(client_id, m_delete)
            # while not self.has_received_res:
            #     pass
            self.c_has_received_res.acquire()
            while True:
                if self.has_received_res:
                    break
                self.c_has_received_res.wait()
            self.c_has_received_res.release()


    def exit(self):
        # kill the remained nodes and self.clients
        for i in self.servers:
            if self.servers[i] != None:
                os.kill(self.servers[i], signal.SIGKILL)
                if TERM_LOG:
                    print("Server#", i, " stopped", sep="")
        for i in self.clients:
            if self.clients[i] != None:
                os.kill(self.clients[i], signal.SIGKILL)
                if TERM_LOG:
                    print("Client#", i, " stopped", sep="")
        sys.exit()
