#!/usr/bin/python3

import sys, string, random

from network   import Network
from threading import Thread, Lock, Conditon

TERM_LOG = True

class Server:

    ANTI_ENTROPY_TIME = 2

    def __init__(self, node_id, is_primary):
        self.node_id    = node_id
        self.uid        = "Server#" + str(node_id)
        self.unique_id  = (1, 0)  # replica_id in lecture note, <clock, id>

        self.is_primary = is_primary # first created server is the primary
        self.is_retired = False
        self.is_paused  = False

        self.version_vector = {} # unique_id: accept_time
        if is_primary:
            self.version_vector[self.unique_id] = 1

        self.playlist = {}      # song_name: url
        self.history  = []      # list of history playlists

        self.CSN = 0            # commit sequence number
        self.accept_time = 1

        self.committed_log = []
        self.tentative_log = [] # list of writes

        # create the network controller
        self.server_list = []    # created by Creation Write
        self.client_list = []    # modified by master
        self.nt = Network(self.uid)
        try:
            self.t_recv = Thread(target=self.receive)
            self.t_recv.daemon = True
            self.t_recv.start()
        except:
            print(self.uid, "error: unable to start new thread")

        # start Anti-Entropy
        random.seed()
        threading.Timer(self.ANTI_ENTROPY_TIME, self.timer_anti_entropy).start()

    def receive(self):
        while 1:
            buf = self.nt.receive()
            if buf:
                if TERM_LOG:
                    print(self.uid, "handles:", str(buf))
                # TODO: parse buf
                # update logical clock
                if isinstance(buf.content, Write):
                    self.accept_time = max(self.accept_time,
                                           buf.content.accept_time) + 1

                if buf.mtype == "Creation":
                    tentative_log.append(buf.content)
                    server_list.append(buf.content.sender_id)
                    history.append((playlist, server_list, client_list))
                    m_create_ack = Message(self.node_id, self.unique_id,
                                           "Creation_Ack", self.accept_time)
                    self.nt.send_to_node(buf.sender_id, m_create_ack)

                if buf.mtype == "Creation_Ack":
                    # buf.content contains sender's accept_time
                    self.unique_id   = (buf.content, buf.unique_id)
                    self.accept_time = buf.content + 1
                    c_create.notify()

                if buf.mtype == "RequestAntiEn":
                    a_ack = AntiEntropy(self.node_id, self.version_vector,
                                        self.CSN)
                    m_anti_entropy_ack = Message(self.node_id, self.unique_id,
                                                 "AntiEn_Ack", a_ack)
                    self.nt.send_to_node(buf.sender_id, m_anti_entropy_ack)

                if buf.mtype == "AntiEn_Ack":
                    self.m_anti_entropy = buf.content
                    c_request_antientropy.notify()

    '''
    Notify server @dest_id about its joining. '''
    def notify(self, dest_id):
        w_write = Write(self.node_id, "Creation", 0, 1, None)
        m_join_server = Message(self.node_id, None, "Creation", w_write)
        self.nt.send_to_server(dest_id, m_join_server)
        global c_create = Condition()
        c_create.wait()

    '''
    Process Anti-Entropy periodically. '''
    def timer_anti_entropy(self):
        rand_index = random.randint(0, len(self.server_list)-1)
        self.anti_entropy(self.server_list[rand_index])
        threading.Timer(self.ANTI_ENTROPY_TIME, self.timer_anti_entropy).start()

    '''
    Process a received message of type of AntiEntropy. Stated in the paper
    Flexible Update Propagation '''
    def anti_entropy(receiver_id):
        m_request_anti = Message(self.node_id, self.unique_id, "RequestAntiEn",
                                 None)
        self.block_request_anti = True
        self.nt.send_to_node(receiver_id, m_request_anti)
        global c_request_antientropy = Condition()
        c_request_antientropy.wait()

        m_anti_entropy = self.m_anti_entropy

        # information of receiver
        R_version_vector = m_anti_entropy.version_vector
        R_CSN            = m_anti_entropy.CSN
        R_committed_log  = m_anti_entropy.committed_log
        R_tentative_log  = m_anti_entropy.tentative_log

        # information of sender (self)
        S_version_vector = self.version_vector
        S_CSN            = self.CSN
        S_committed_log  = self.committed_log
        S_tentative_log  = self.tentative_log

        # TODO: truncation of stable log

        # anti-entropy with support for committed writes
        if R_CSN < S_CSN:
            # committed log
            for w in S_committed_log:
                if w in R_committed_log or w in R_tentative_log:
                    continue
                if w.accept_time <= R_version_vector[w.sender_id]:
                    m_commit = Message(self.node_id, self.unique_id, "Commit",
                                       w)
                    self.nt.send_to_node(receiver_id, m_commit)
                else:
                    m_write = Message(self.node_id, self.unique_id, "Write", w)
                    self.nt.send_to_node(receiver_id, m_write)
        # tentative log
        for w in S_tentative_log:
            if R_version_vector[w.sender_id] < w.accept_time:
                m_write = Message(self.node_id, self.unique_id, "Write", w)
                self.nt.send_to_node(receiver_id, m_write)

    '''
    Receive_Writes in paper Bayou, client part. '''
    def receive_client_writes(w):
        self.accept_time = max(self.accept_time+1, w.accept_time) # BUG?
        w.sender = self.node_id
        w.accept_time = self.accept_time
        w.wid = (self.accept_time, self.node_id)
        self.tentative_log.append(w)
        # TODO: bayou_write()

    def receive_server_writes(w):
        if w.state == "COMMITTED":
            insert_point = len(self.committed_log)
            for i in range(len(self.committed_log)):
                if committed_log[i].wid > w.wid:
                    insert_point = i
                    break
            # rollback
            suffix_committed_log = self.committed_log[insert_point:]
            if insert_point == 0:
                self.playlist = set()
                self.history  = []
                self.committed_log = []
            else:
                self.playlist = self.history[insert_point-1]
                self.history  = self.history[:insert_point-1]
                self.committed_log = self.committed_log[:insert_point-1]
            # insert w
            # TODO: bayou_write()
            # roll forward
            for wx in suffix_committed_log:
                # TODO: bayou_write()

        else:
            # TODO: tentative rollback

    '''
    Bayou_Write in paper Bayou. '''
    def bayou_write(self):
        pass

    def __str__(self):
        return "Server #" + self.node_id + "#" + self.unique_id


if __name__ == "__main__":
    cmd = sys.argv
    node_id = int(cmd[1])
    is_primary = (cmd[2] == "True")
    s = Server(node_id, is_primary)
    if TERM_LOG:
        print(s.uid, "started")
    s.t_recv.join()
