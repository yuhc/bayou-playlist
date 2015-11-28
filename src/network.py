#!/usr/bin/python3

import sys, socket, os, signal

from message import Message

TERM_LOG        = True
DEBUG_SOCKET    = True

class Network:

    MASTER_BASE_PORT = 7000
    NODE_BASE_PORT   = 8000

    '''
    @uid has three different kinds: Master#0, Server#i and Client#j
    '''
    def __init__(self, uid):
        # get id
        self.uid = uid
        uid_list = uid.split('#')
        self.node_id = int(uid_list[1])

        # create socket
        self.PRIVATE_TCP_IP = socket.gethostbyname(socket.gethostname())
        if uid[0] == 'M': # Master
            TCP_PORT = self.MASTER_BASE_PORT
        else:
            self.is_server = True if uid[0] == 'S' else False
            BASE_PORT = self.NODE_BASE_PORT
            TCP_PORT = self.node_id + BASE_PORT
        self.BUFFER_SIZE = 1024
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server.bind((self.PRIVATE_TCP_IP, TCP_PORT))
        self.server.listen(128)
        if TERM_LOG:
            print(uid, " socket ", self.PRIVATE_TCP_IP, ":", TCP_PORT, " started",
              sep="")

    def send_to_node(self, dest_id, message):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((self.PRIVATE_TCP_IP, self.NODE_BASE_PORT+dest_id))
            s.send(str(message).encode('ascii'))
            if TERM_LOG:
                print(self.uid, " sends (", str(message), ") to Node ", dest_id,
                      sep="")
        except:
            if DEBUG_SOCKET and TERM_LOG:
                print(self.uid, "connects to Node", dest_id, "failed")

    def send_to_master(self, message):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((self.PRIVATE_TCP_IP, self.MASTER_BASE_PORT))
            s.send(message.encode('ascii'))
            if TERM_LOG:
                print(self.uid, " sends (", str(message), ") to Master ",
                      sep="")
        except:
            if DEBUG_SOCKET and TERM_LOG:
                print(self.uid, "connects to Master", dest_id, "failed")

    def receive(self):
        connection, address = self.server.accept()
        buf = connection.recv(self.BUFFER_SIZE)
        if len(buf) > 0:
            decode_buf = buf.decode('ascii').split(' ')
            sender_id  = decode_buf[0]
            sender_uid = decode_buf[1] if decode_buf != "None" else None
            mtype      = decode_buf[2]
            content    = ' '.join(decode_buf[3:])
            if content == "None":
                content = None
            message = Message(sender_id, sender_uid, mtype, content)
            if TERM_LOG:
                print(self.uid, " receives (", str(message), ") from ", address,
                  sep="")
        else:
            message = None
        return message

    def shutdown(self):
        self.server.close()
        if TERM_LOG:
            print(self.uid, "socket closed")
