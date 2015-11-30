#!/usr/bin/python3

import sys, string, random

from network   import Network
from threading import Thread, Lock, Conditon

TERM_LOG = True

class Server:

    ANTI_ENTROPY_TIME   = 2
    PRIMARY_COMMIT_TIME = 2
    c_create = Condition()
    c_request_antientropy = Condition()
    c_antientropy = Lock()

    def __init__(self, node_id, is_primary):
        self.node_id    = node_id
        self.uid        = "Server#" + str(node_id)
        self.unique_id  = (1, 0)  # replica_id in lecture note, <clock, id>

        self.is_primary = is_primary # first created server is the primary
        self.is_retired = False
        self.is_paused  = False

        self.version_vector = {} # unique_id: accept_time
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

        if is_primary:
            threading.Timer(self.PRIMARY_COMMIT_TIME,
                            self.timer_primary_commit).start()

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
                    self.version_vector[self.unique_id] = self.accept_time

                if buf.mtype == "Creation":
                    tentative_log.append(buf.content)
                    server_list.append(buf.content.sender_id)
                    history.append((playlist, server_list, client_list))
                    m_create_ack = Message(self.node_id, self.unique_id,
                                           "Creation_Ack", self.accept_time)
                    self.nt.send_to_node(buf.sender_id, m_create_ack)

                elif buf.mtype == "Creation_Ack":
                    # buf.content contains sender's accept_time
                    c_create.acquire()
                    self.unique_id   = (buf.content, buf.unique_id)
                    self.accept_time = buf.content + 1
                    self.version_vector[self.unique_id] = self.accept_time
                    c_create.notify()
                    c_create.release()

                elif buf.mtype == "Retire":
                    self.is_retired = True
                    self.accept_time = self.accept_time + 1
                    self.version_vector[self.unique_id] = self.accept_time
                    w_retire = Write(self.sender_id, self.unique_id,
                                     "Retirement", None
                                     self.accept_time, self.unique_id)
                    self.c_antientropy.acquire()
                    self.receive_server_writes(w_retire)
                    self.c_antientropy.release()

                elif buf.mtype == "RequestAntiEn":
                    self.c_antientropy.acquire()
                    a_ack = AntiEntropy(self.node_id, self.version_vector,
                                        self.CSN, self.committed_log,
                                        self.tentative_log)
                    m_anti_entropy_ack = Message(self.node_id, self.unique_id,
                                                 "AntiEn_Ack", a_ack)
                    # dest_id may retire
                    if not
                       self.nt.send_to_node(buf.sender_id, m_anti_entropy_ack):
                       self.c_antientropy.release()

                elif buf.mtype == "AntiEn_Ack":
                    self.c_request_antientropy.acquire()
                    self.m_anti_entropy = buf.content
                    self.c_request_antientropy.notify()
                    self.c_request_antientropy.release()

                elif buf.mtype == "AntiEn_Finsh":
                    self.c_antientropy.release()

                # from master
                elif buf.mtype == "Break":
                    self.server_list.remove(buf.content)
                    self.client_list.remove(buf.content)

                elif buf.mtype == "Restore":
                    if buf.content[0] == "Server":
                        self.server_list.append(buf.content[1])
                    else:
                        self.client_list.append(buf.content[2])

                elif buf.mtype == "Pause":
                    self.is_paused = True

                elif buf.mtype == "Start":
                    self.is_paused = False

                elif buf.mtype == "Print":
                    self.printLog()

                elif buf.mtype == "Get":
                    song_name = buf.content
                    try:
                        song_url  = self.playlist[song_name]
                    except:
                        song_url  = "ERR_KEY"
                    m_get = Message(self.node_id, self.unique_id, "GetAck",
                                    (song_name, song_url, self.CSN))
                    self.nt.send_to_node(buf.sender_id, m_get)

                elif buf.mtype == "Write":
                    c_antientropy.acquire()
                    if buf.sender_id in server_list:
                        self.receive_server_writes(buf.content)
                    else:
                        self.receive_client_writes(buf.content)
                    c_antientropy.release()
                    done = Message(self.node_id, None, "Done", None)
                    self.nt.send_to_master(done)

    '''
    Notify server @dest_id about its joining. '''
    def notify(self, dest_id):
        w_write = Write(self.node_id, self.unique_id, "Creation", 0, 1, None)
        m_join_server = Message(self.node_id, None, "Creation", w_write)
        self.nt.send_to_node(dest_id, m_join_server)
        self.c_create.acquire()
        while True:
            if self.accept_time != 1:
                break
            self.c_create.wait()
        self.c_create.release()

    '''
    Process Anti-Entropy periodically. '''
    def timer_anti_entropy(self):
        c_antientropy.acquire()
        rand_index = random.randint(0, len(self.server_list)-1)
        while self.server_list[rand_index] == self.node_id:
            rand_index = random.randint(0, len(self.server_list)-1)
        self.anti_entropy(self.server_list[rand_index])

        m_finish = Message(self.node_id, self.unique_id, "AntiEn_Finsh", None)
        self.nt.send_to_node(self.server_list[rand_index], m_finish)

        if self.is_retired:
            m_retire = Message(self.node_id, None, "Retire", None)
            self.nt.send_to_master(m_retire)

        threading.Timer(self.ANTI_ENTROPY_TIME, self.timer_anti_entropy).start()
        c_antientropy.release()

    '''
    Primary commits periodically. '''
    def timer_primary_commit(self):
        c_antientropy.acquire()
        for wx in self.tentative_log:
            wx.state = "COMMITTED"
            self.receive_server_writes(wx)
        self.tentative_log = []
        c_antientropy.release()

        threading.Timer(self.PRIMARY_COMMIT_TIME,
                        self.timer_primary_commit).start()

    '''
    Process a received message of type of AntiEntropy. Stated in the paper
    Flexible Update Propagation '''
    def anti_entropy(receiver_id):
        m_request_anti = Message(self.node_id, self.unique_id, "RequestAntiEn",
                                 None)
        self.block_request_anti = True
        self.nt.send_to_node(receiver_id, m_request_anti)
        self.c_request_antientropy.acquire()
        while True:
            if self.m_anti_entropy:
                break
            self.c_request_antientropy.wait()

        m_anti_entropy = self.m_anti_entropy

        # information of receiver
        R_version_vector = m_anti_entropy.version_vector
        R_CSN            = m_anti_entropy.CSN
        R_committed_log  = m_anti_entropy.committed_log
        R_tentative_log  = m_anti_entropy.tentative_log

        # information of sender (self)
        S_version_vector = self.version_vector
        S_CSN            = self.CSN

        # update version_vector
        for v in S_version_vector:
            # TODO

        # anti-entropy with support for committed writes
        if R_CSN < S_CSN:
            # committed log
            for index in [R_CSN+1:len(S_committed_log)]:
            # for w in S_committed_log:
                # if w in R_committed_log or w in R_tentative_log:
                #    continue
                w = S_committed_log[index]
                if w.accept_time <= R_version_vector[w.sender_id]:
                    m_commit = Message(self.node_id, self.unique_id,
                                       "Write", w)
                    self.nt.send_to_node(receiver_id, m_commit)
                else:
                    m_write = Message(self.node_id, self.unique_id, "Write", w)
                    self.nt.send_to_node(receiver_id, m_write)
        # tentative log
        for w in S_tentative_log:
            if R_version_vector[w.sender_id] < w.accept_time:
                m_write = Message(self.node_id, self.unique_id, "Write", w)
                self.nt.send_to_node(receiver_id, m_write)

        self.c_request_antientropy.release()

    '''
    Receive_Writes in paper Bayou, client part. '''
    def receive_client_writes(w):
        #self.accept_time = max(self.accept_time+1, w.accept_time) # BUG?
        w.sender         = self.node_id
        w.accept_time    = self.accept_time
        w.wid            = (self.accept_time, self.unique_id)
        self.tentative_log.append(w)
        self.bayou_write(w)

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
            self.bayou_write(w)
            # roll forward
            for wx in suffix_committed_log:
                self.bayou_write(wx)
            # tentative roll forward
            self.tentative_log.remove(w)
            for wx in self.tentative_log:
                self.bayou_write(wx)

        else:
            insert_point = len(self.tentative_log)
            for i in range(len(self.tentative_log)):
                if tentative_log[i].wid > w.wid:
                    insert_point = i
                    break
            # rollback
            total_point = insert_point + len(self.committed_log)
            suffix_tentative_log = self.tentative_log[insert_point:]
            if total_point == 0:
                self.playlist = set()
                self.history = []
            else:
                self.playlist = self.history[total_point-1]
                self.history  = self.history[:total_point-1]
            if insert_point == 0:
                self.tentative_log = []
            else:
                self.tentative_log = self.tentative_log[:insert_point-1]
            # insert w
            self.bayou_write(w)
            # roll forward
            self.tentative_log.remove(w)
            for wx in self.tentative_log:
                self.bayou_write(wx)

    '''
    Bayou_Write in paper Bayou. '''
    def bayou_write(self, w):
        if w.mtype == "Put":
            cmd = w.content.split(' ')
            self.playlist[cmd[0]] = cmd[1]
        elif w.mtype == "Delete":
            self.playlist.pop(cmd)
        elif w.mtype == "Retirement":
            self.version_vector.pop(w.content)
        if w.state == "COMMITTED":
            self.committed_log.append(w)
        else:
            self.tentative_log.append(w)
        self.history.append(playlist)

    '''
    Print playlist and send it to master. '''
    def printLog():
        plog = ""
        for wx in self.committed_log:
            if wx.mtype == "Put":
                plog = plog + "PUT:(" + wx.content + "):TRUE\n"
            elif wx.mtype == "Delete":
                plog = plog + "DELETE:(" + wx.content _ "):TRUE\n"
        for wx in self.tentative_log:
            if wx.mtype == "Put":
                plog = plog + "PUT:(" + wx.content + "):FALSE\n"
            elif wx.mtype == "Delete":
                plog = plog + "DELETE:(" + wx.content _ "):FALSE\n"

        m_log = Message(self.node_id, None, "Playlist", plog)
        self.nt.send_to_master(m_log)

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
