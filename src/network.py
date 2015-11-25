#!/usr/bin/python3

import sys, socket, os, signal
from ast import literal_eval

TERM_LOG        = False
DEBUG_HEARTBEAT = False
DEBUG_SOCKET    = False

class Network:

    MASTER_BASE_PORT = 7000
    SERVER_BASE_PORT = 8000
    CLIENT_BASE_PORT = 9000

    '''
    @uid has three different kinds: Master#0, Server#i and Client#j
    '''
    def __init__(self, uid, num_nodes = 0, num_clients = 0):
        # get id
        self.uid = uid
        uid_list = uid.split('#')
        self.node_id = int(uid_list[1])

        # get number
        self.num_nodes = num_nodes
        self.num_clients = num_clients

        # timeBombLeader
        self.remain_message = -1 # -1 means infinity

        # create socket
        self.PRIVATE_TCP_IP = socket.gethostbyname(socket.gethostname())
        if uid[0] == 'M': # Master
            TCP_PORT = self.MASTER_BASE_PORT
        else:
            self.is_server = True if uid[0] == 'S' else False
            BASE_PORT = self.SERVER_BASE_PORT if self.is_server \
                   else self.CLIENT_BASE_PORT
            TCP_PORT = self.node_id + BASE_PORT
        self.BUFFER_SIZE = 1024
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server.bind((self.PRIVATE_TCP_IP, TCP_PORT))
        self.server.listen(128)
        if TERM_LOG:
            print(uid, " socket ", self.PRIVATE_TCP_IP, ":", TCP_PORT, " started",
              sep="")

    def set_num_nodes(self, num_nodes):
        self.num_nodes = num_nodes

    def set_num_clients(self, num_clients):
        self.num_clients = num_clients

    def set_remain_message(self, r_message=-1):
        self.remain_message = r_message

    def check_remain_message(self):
        if self.remain_message > 0:
            self.remain_message = self.remain_message - 1
            if self.remain_message <= 0:
                if TERM_LOG:
                    print(self.uid, "bombs")
                self.send_to_master(str(("leaderBombed", self.node_id)))
                os.kill(os.getpid(), signal.SIGKILL)

    def send_to_server(self, dest_id, message):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((self.PRIVATE_TCP_IP, self.SERVER_BASE_PORT+dest_id))
            s.send(message.encode('ascii'))
            # do not print heartbeat
            try:
                if DEBUG_HEARTBEAT or literal_eval(message)[0] != "heartbeat":
                    if TERM_LOG:
                        print(self.uid, " sends ", message, " to Server ", dest_id,
                          sep="")
            except:
                if TERM_LOG:
                    print(self.uid, " sends ", message, " to Server ", dest_id,
                      sep="")
        except:
            if DEBUG_SOCKET and TERM_LOG:
                print(self.uid, "connects to Server", dest_id, "failed")
                # print("Unexpected error:", sys.exc_info()[0])
        try:
            if literal_eval(message)[0] != "heartbeat" and \
               dest_id != self.node_id:
                self.check_remain_message()
        except:
            self.check_remain_message()

    def send_to_client(self, dest_id, message):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((self.PRIVATE_TCP_IP, self.CLIENT_BASE_PORT+dest_id))
            s.send(message.encode('ascii'))
            if TERM_LOG:
                print(self.uid, " sends ", message, " to Client ", dest_id, sep="")
        except:
            if DEBUG_SOCKET and TERM_LOG:
                print(self.uid, "connects to Client", dest_id, "failed")

    def send_to_master(self, message):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((self.PRIVATE_TCP_IP, self.MASTER_BASE_PORT))
            s.send(message.encode('ascii'))
            if TERM_LOG:
                print(self.uid, " sends ", message, " to Master ", sep="")
        except:
            if DEBUG_SOCKET and TERM_LOG:
                print(self.uid, "connects to Master", dest_id, "failed")

    def broadcast_to_server(self, message):
        for i in range(self.num_nodes):
            self.send_to_server(i, message)

    def broadcast_to_client(self, message):
        for i in range(self.num_clients):
            self.send_to_client(i, message)

    def receive(self):
        connection, address = self.server.accept()
        buf = connection.recv(self.BUFFER_SIZE)
        if len(buf) > 0:
            decode_buf = buf.decode('ascii')
            # do not print heartbeat
            try:
                if DEBUG_HEARTBEAT or \
                   literal_eval(decode_buf)[0] != "heartbeat":
                    if TERM_LOG:
                        print(self.uid, " receives ", decode_buf, " from ", address,
                          sep="")
            except:
                if TERM_LOG:
                    print(self.uid, " receives ", decode_buf, " from ", address,
                      sep="")
        else:
            decode_buf = ""
        return decode_buf

    def shutdown(self):
        self.server.close()
        if TERM_LOG:
            print(self.uid, "socket closed")
