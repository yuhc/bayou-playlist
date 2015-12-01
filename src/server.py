#!/usr/bin/python3

import sys, string, random, threading, copy

from network   import Network
from threading import Thread, Lock, Condition
from message   import AntiEntropy, Write, Message
from config    import Config

TERM_LOG = Config.server_log

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

        self.CSN = -1           # commit sequence number
        self.accept_time = 1

        self.committed_log = []
        self.tentative_log = [] # list of writes

        # create the network controller
        self.server_list = set() # created by Creation Write
        self.client_list = set() # modified by master
        self.nt = Network(self.uid)
        try:
            self.t_recv = Thread(target=self.receive)
            self.t_recv.daemon = True
            self.t_recv.start()
        except:
            print(self.uid, "error: unable to start new thread")

        if is_primary:
            self.accept_time = self.accept_time + 1
            w_create = Write(self.node_id, self.unique_id, "Creation", None,
                             self.accept_time, self.unique_id)
            self.bayou_write(w_create)
        threading.Timer(self.PRIMARY_COMMIT_TIME,
                        self.timer_primary_commit).start()

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
                    self.version_vector[self.unique_id] = self.accept_time

                if buf.mtype == "Creation":
                    if buf.sender_id < 0:
                        try:
                            Thread(target=self.notify,args=(buf.content,)).start()
                        except:
                            print(self.uid, "error: unable to start new thread")
                    else:
                        self.tentative_log.append(buf.content)
                        self.server_list.add(buf.content.sender_id)
                        self.history.append((self.playlist, self.server_list,
                                             self.client_list))
                        m_create_ack = Message(self.node_id, self.unique_id,
                                               "Creation_Ack", self.accept_time)
                        self.nt.send_to_node(buf.sender_id, m_create_ack)

                elif buf.mtype == "Creation_Ack":
                    # buf.content contains sender's accept_time
                    if TERM_LOG:
                        print(self.uid, "tries to acquire a c_antientropy lock in receive.Creation_Ack")
                    self.c_create.acquire()
                    if TERM_LOG:
                        print(self.uid, "acquires a c_create lock in receive.Creation_Ack")
                    self.unique_id   = (buf.content, buf.sender_uid)
                    self.accept_time = buf.content + 1
                    self.version_vector[self.unique_id] = self.accept_time
                    self.c_create.notify()
                    self.c_create.release()
                    if TERM_LOG:
                        print(self.uid, "releases a c_create lock in receive.Creation_Ack")

                elif buf.mtype == "Retire":
                    self.is_retired = True
                    self.accept_time = self.accept_time + 1
                    self.version_vector[self.unique_id] = self.accept_time
                    w_retire = Write(self.sender_id, self.unique_id,
                                     "Retirement", None,
                                     self.accept_time, self.unique_id)
                    self.c_antientropy.acquire()
                    self.receive_server_writes(w_retire)
                    self.c_antientropy.release()

                elif buf.mtype == "RequestAntiEn":
                    # unknown server or itself retires
                    if not buf.sender_id in self.server_list or self.is_retired:
                        m_reject_anti = Message(self.node_id, self.unique_id,
                                                "AntiEn_Reject", None)
                        self.nt.send_to_node(buf.sender_id, m_reject_anti)
                        continue

                    if TERM_LOG:
                        print(self.uid, "tries to acquire a c_antientropy lock in receive.RequestAntiEn")
                    self.c_antientropy.acquire()
                    if TERM_LOG:
                        print(self.uid, "acquires a c_antientropy lock in receive.RequestAntiEn")
                    a_ack = AntiEntropy(self.node_id, self.version_vector,
                                        self.CSN, self.committed_log,
                                        self.tentative_log)
                    m_anti_entropy_ack = Message(self.node_id, self.unique_id,
                                                 "AntiEn_Ack", a_ack)
                    # dest_id may retire
                    if not \
                       self.nt.send_to_node(buf.sender_id, m_anti_entropy_ack):
                       self.c_antientropy.release()
                       if TERM_LOG:
                           print(self.uid, "releases a c_antientropy lock in receive.RequestAntiEn.Retire")

                elif buf.mtype == "AntiEn_Reject":
                    self.m_anti_entropy = Message(None, None, None, "Reject")
                    self.c_request_antientropy.release()
                    if TERM_LOG:
                        print(self.uid, "releases a c_request_antientropy lock in receive.AntiEn_Reject")

                elif buf.mtype == "AntiEn_Ack":
                    if TERM_LOG:
                        print(self.uid, "tries to acquire a c_antientropy lock in receive.AntiEn_Ack")
                    self.c_request_antientropy.acquire()
                    if TERM_LOG:
                        print(self.uid, "acquires a c_antientropy lock in receive.AntiEn_Ack")
                    self.m_anti_entropy = buf.content
                    self.c_request_antientropy.notify()
                    self.c_request_antientropy.release()
                    if TERM_LOG:
                        print(self.uid, "releases a c_antientropy lock in receive.AntiEn_Ack")

                elif buf.mtype == "AntiEn_Finsh":
                    self.c_antientropy.release()
                    if TERM_LOG:
                        print(self.uid, "releases a c_antientropy lock in receive.AntiEn_Finsh")

                elif buf.mtype == "Elect":
                    self.is_primary = True

                # from master
                elif buf.mtype == "Break":
                    self.server_list.remove(buf.content)
                    self.client_list.remove(buf.content)

                elif buf.mtype == "Restore":
                    if buf.content[0] == "Server":
                        self.server_list.add(buf.content[1])
                    else:
                        self.client_list.add(buf.content[2])

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
                    if self.is_retired:
                        continue

                    if buf.sender_id in self.server_list:
                        if TERM_LOG:
                            print(self.uid, " receives a write from Server#",
                                  buf.sender_id, sep="")
                        self.receive_server_writes(buf.content)
                    else:
                        if TERM_LOG:
                            print(self.uid, "tries to acquire a c_antientropy lock in receive.Write.Client")
                        self.c_antientropy.acquire()
                        if TERM_LOG:
                            print(self.uid, "acquires a c_antientropy lock in receive.Write.Client")
                        if TERM_LOG:
                            print(self.uid, " receives a write from Client#",
                                  buf.sender_id, sep="")
                        self.receive_client_writes(buf.content)
                        self.c_antientropy.release()
                        if TERM_LOG:
                            print(self.uid, "releases a c_antientropy lock in receive.Write.Client")
                        done = Message(self.node_id, None, "Done", None)
                        self.nt.send_to_master(done)

    '''
    Notify server @dest_id about its joining. '''
    def notify(self, dest_id):
        w_write = Write(self.node_id, self.unique_id, "Creation", 0, 1, None)
        m_join_server = Message(self.node_id, None, "Creation", w_write)
        self.nt.send_to_node(dest_id, m_join_server)

        self.c_create.acquire()
        if TERM_LOG:
            print(self.uid, "acquires a c_create lock in notify")
        while True:
            if self.accept_time != 1:
                break
            self.c_create.wait()
        self.server_list.add(dest_id)
        self.c_create.release()
        if TERM_LOG:
            print(self.uid, "releases a c_create lock in notify")

    '''
    Process Anti-Entropy periodically. '''
    def timer_anti_entropy(self):
        if not self.server_list:
            threading.Timer(self.ANTI_ENTROPY_TIME,
                            self.timer_anti_entropy).start()
            return

        self.c_antientropy.acquire()
        if TERM_LOG:
            print(self.uid, "acquires a c_antientropy lock in timer_anti_entropy")

        rand_dest = random.sample(self.server_list, 1)[0]
        while rand_dest == self.node_id:
            rand_dest = random.sample(self.server_list, 1)[0]
        succeed_anti = self.anti_entropy(rand_dest)

        # select a new primary
        if self.is_retired and succeed_anti:
            m_elect = Message(self.sender_id, self.unique_id, "Elect",
                              None)
            self.nt.send_to_node(rand_dest, m_elect)

        # finish the anti-entropy
        m_finish = Message(self.node_id, self.unique_id, "AntiEn_Finsh", None)
        self.nt.send_to_node(rand_dest, m_finish)

        if self.is_retired and succeed_anti:
            m_retire = Message(self.node_id, None, "Retire", None)
            self.nt.send_to_master(m_retire)


        threading.Timer(self.ANTI_ENTROPY_TIME, self.timer_anti_entropy).start()

        self.c_antientropy.release()
        if TERM_LOG:
            print(self.uid, "releases a c_antientropy lock in timer_anti_entropy")

    '''
    Primary commits periodically. '''
    def timer_primary_commit(self):
        if self.is_primary:
            if TERM_LOG:
                print(self.uid, "tries to acquire a c_antientropy lock in timer_primary_commit")
            self.c_antientropy.acquire()
            if TERM_LOG:
                print(self.uid, "acquires a c_antientropy lock in timer_primary_commit")

            for wx in self.tentative_log:
                wx.state = "COMMITTED"
                wx.CSN   = self.CSN + 1
                self.receive_server_writes(wx)
            self.tentative_log = []

            if self.is_retired:
                self.is_primary = False

            self.c_antientropy.release()
            if TERM_LOG:
                print(self.uid, "releases a c_antientropy lock in timer_primary_commit")

        threading.Timer(self.PRIMARY_COMMIT_TIME,
                        self.timer_primary_commit).start()

    '''
    Process a received message of type of AntiEntropy. Stated in the paper
    Flexible Update Propagation '''
    def anti_entropy(self, receiver_id):
        m_request_anti = Message(self.node_id, self.unique_id, "RequestAntiEn",
                                 None)
        self.m_anti_entropy = None
        if not self.nt.send_to_node(receiver_id, m_request_anti):
            return False
        self.c_request_antientropy.acquire()
        while True:
            if self.m_anti_entropy:
                break
            self.c_request_antientropy.wait()
        if isinstance(self.m_anti_entropy, Message): # was rejected
            return False

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
            try:
                self.version_vector[v] = max(self.version_vector[v],
                                             R_version_vector[v])
            except:
                pass

        # anti-entropy with support for committed writes
        if R_CSN < S_CSN:
            # committed log
            for index in range(R_CSN+1, len(self.committed_log)):
                w = self.committed_log[index]
                if w.accept_time <= R_version_vector[w.sender_uid]:
                    m_commit = Message(self.node_id, self.unique_id,
                                       "Write", w)
                    self.nt.send_to_node(receiver_id, m_commit)
                else:
                    m_write = Message(self.node_id, self.unique_id, "Write", w)
                    self.nt.send_to_node(receiver_id, m_write)
        # tentative log
        for w in self.tentative_log:
            if R_version_vector[w.sender_uid] < w.accept_time:
                m_write = Message(self.node_id, self.unique_id, "Write", w)
                self.nt.send_to_node(receiver_id, m_write)

        self.c_request_antientropy.release()
        return True

    '''
    Receive_Writes in paper Bayou, client part. '''
    def receive_client_writes(self, w):
        w.sender         = self.node_id
        w.sender_uid     = self.unique_id
        w.accept_time    = self.accept_time
        w.wid            = (self.accept_time, self.unique_id)
        self.tentative_log.append(w)
        self.bayou_write(w)

    def receive_server_writes(self, w):
        if w.state == "COMMITTED":
            insert_point = len(self.committed_log)
            for i in range(len(self.committed_log)):
                if self.committed_log[i].wid == w.wid:
                    return
                if self.committed_log[i].wid > w.wid:
                    insert_point = i
                    break

            # rollback
            suffix_committed_log = self.committed_log[insert_point:]
            if insert_point == 0:
                self.playlist = {}
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
            tmp_tentative_log = copy.deepcopy(self.tentative_log)
            self.tentative_log = []
            for wx in tmp_tentative_log:
                self.bayou_write(wx)
            print(self.uid, "commit", self.committed_log, "tentative", self.tentative_log)

        else:
            insert_point = len(self.tentative_log)
            for i in range(len(self.tentative_log)):
                if tentative_log[i].wid == w.wid:
                    return
                if tentative_log[i].wid > w.wid:
                    insert_point = i
                    break
            # rollback
            total_point = insert_point + len(self.committed_log)
            suffix_tentative_log = self.tentative_log[insert_point:]
            if total_point == 0:
                self.playlist = {}
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
        elif w.mtype == "Creation":
            self.server_list.add(w.sender_id)
        elif w.mtype == "Retirement":
            self.server_list.remove(w.sender_id)
            self.version_vector.pop(w.content)
        if w.state == "COMMITTED":
            self.committed_log.append(w)
            self.CSN = w.CSN
        else:
            self.tentative_log.append(w)
        self.history.append(self.playlist)

    '''
    Print playlist and send it to master. '''
    def printLog():
        plog = ""
        for wx in self.committed_log:
            if wx.mtype == "Put":
                plog = plog + "PUT:(" + wx.content + "):TRUE\n"
            elif wx.mtype == "Delete":
                plog = plog + "DELETE:(" + wx.content + "):TRUE\n"
        for wx in self.tentative_log:
            if wx.mtype == "Put":
                plog = plog + "PUT:(" + wx.content + "):FALSE\n"
            elif wx.mtype == "Delete":
                plog = plog + "DELETE:(" + wx.content + "):FALSE\n"

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
