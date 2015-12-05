#!/usr/bin/python3

import sys, string, random, threading, copy, signal, os

from network   import Network
from threading import Thread, Lock, Condition
from message   import AntiEntropy, Write, Message
from config    import Config

TERM_LOG   = Config.server_log
DETAIL_LOG = Config.server_detail_log
LOCK_LOG   = Config.server_lock_log

class Server:

    ANTI_ENTROPY_LOWER = Config.anti_entropy_lower
    ANTI_ENTROPY_UPPER = Config.anti_entropy_upper

    c_create = Condition()
    c_request_antientropy = Condition()
    c_antientropy = Lock()

    def __init__(self, node_id, is_primary):
        self.node_id    = node_id
        self.uid        = "Server#" + str(node_id)
        self.unique_id  = (1, node_id)  # replica_id in lecture note, <clock,id>

        self.is_primary = is_primary    # first created server is the primary
        self.is_retired = False
        self.is_paused  = False
        self.first_creation = True      # whether it is the first Creation

        self.version_vector = {} # unique_id: accept_time
        if is_primary:
            self.version_vector[self.unique_id] = 1

        self.playlist = {}      # song_name: url
        self.history  = []      # list of history playlists

        self.CSN = -1           # commit sequence number
        self.accept_time = 1

        self.committed_log = []
        self.tentative_log = [] # list of writes

        # create the network controller
        self.server_list    = set() # created by Creation Write
        self.client_list    = set() # modified by master
        self.block_list     = set() # list of blocked connections
        self.available_list = set() # last available
        self.sent_list      = set() # has sent to theses servers
        self.nt = Network(self.uid)
        try:
            self.t_recv = Thread(target=self.receive)
            self.t_recv.daemon = True
            self.t_recv.start()
        except:
            print(self.uid, "error: unable to start new thread")

        random.seed(self.node_id)
        if is_primary:
            self.accept_time = self.accept_time + 1
            w_create = Write(self.node_id, self.unique_id, "Creation", None,
                             self.accept_time, self.unique_id)
            w_create.state = "COMMITTED"
            self.bayou_write(w_create)

        # start Anti-Entropy
        threading.Timer(random.randint(4, 10)/10,
                        self.timer_anti_entropy).start()


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
                    if buf.sender_id < 0:
                        try:
                            Thread(target=self.notify,args=(buf.content,)
                            ).start()
                        except:
                            print(self.uid, "error: unable to start new thread")
                    else:
                        w = buf.content
                        if w.content: # first creation
                            # rewrite Creation's wid
                            w.sender_uid = (self.accept_time, self.unique_id)
                            w.wid = (w.accept_time, w.sender_uid)

                        self.receive_server_writes(w)

                        m_create_ack = Message(self.node_id, self.unique_id,
                                               "Creation_Ack", self.accept_time)
                        if not w.content:
                            m_create_ack.content = None
                        self.nt.send_to_node(buf.sender_id, m_create_ack)

                elif buf.mtype == "Creation_Ack":
                    # buf.content contains sender's accept_time
                    if LOCK_LOG:
                        print(self.uid, "tries to acquire a c_antientropy lock",
                                        "in receive.Creation_Ack")
                    self.c_create.acquire()
                    if LOCK_LOG:
                        print(self.uid, "acquires a c_create lock in",
                              "receive.Creation_Ack")
                    if not self.unique_id in self.version_vector and \
                       buf.content:
                        self.unique_id   = (buf.content, buf.sender_uid)
                        self.accept_time = buf.content + 1
                    self.c_create.notify()
                    self.c_create.release()
                    if LOCK_LOG:
                        print(self.uid, "releases a c_create lock in",
                              "receive.Creation_Ack")

                elif buf.mtype == "Retire":
                    self.is_retired = True
                    self.accept_time = self.accept_time + 1
                    w_retire = Write(self.node_id, self.unique_id,
                                     "Retirement", None,
                                     self.accept_time, self.unique_id)
                    self.c_antientropy.acquire()
                    self.receive_server_writes(w_retire)
                    self.c_antientropy.release()

                elif buf.mtype == "RequestAntiEn":
                    # unknown server or itself retires
                    # comment "not buf.sender_id in self.server_list or"
                    if buf.sender_id in self.block_list or self.is_retired or \
                       self.is_paused:
                        m_reject_anti = Message(self.node_id, self.unique_id,
                                                "AntiEn_Reject", None)
                        self.nt.send_to_node(buf.sender_id, m_reject_anti)
                        continue

                    if LOCK_LOG:
                        print(self.uid, "tries to acquire a c_antientropy lock",
                              "in receive.RequestAntiEn")
                    lock_result = self.c_antientropy.acquire(blocking=False)
                    if LOCK_LOG:
                        print(self.uid, "acquires a c_antientropy lock in",
                              "receive.RequestAntiEn:", lock_result)
                    self.server_list.add(buf.sender_id)

                    # if currently anti-entropy, then reject
                    if not lock_result:
                        m_reject_anti = Message(self.node_id, self.unique_id,
                                                "AntiEn_Reject", None)
                        self.nt.send_to_node(buf.sender_id, m_reject_anti)
                        continue

                    a_ack = AntiEntropy(self.node_id, self.version_vector,
                                        self.CSN, self.committed_log,
                                        self.tentative_log)
                    m_anti_entropy_ack = Message(self.node_id, self.unique_id,
                                                 "AntiEn_Ack", a_ack)
                    self.nt.send_to_node(buf.sender_id, m_anti_entropy_ack)

                elif buf.mtype == "AntiEn_Reject":
                    try:
                        Thread(target=self.handle_antien_reject).start()
                    except:
                        print(self.uid, "error: unable to start new thread")

                elif buf.mtype == "AntiEn_Ack":
                    if LOCK_LOG:
                        print(self.uid, "tries to acquire a",
                              "c_request_antientropy lock in",
                              "receive.AntiEn_Ack")
                    self.c_request_antientropy.acquire()
                    if LOCK_LOG:
                        print(self.uid, "acquires a c_request_antientropy lock",
                              "in receive.AntiEn_Ack")
                    self.m_anti_entropy = copy.deepcopy(buf.content)
                    self.c_request_antientropy.notify()
                    self.c_request_antientropy.release()
                    if LOCK_LOG:
                        print(self.uid, "releases a c_request_antientropy lock",
                              "in receive.AntiEn_Ack")

                elif buf.mtype == "AntiEn_Finsh":
                    self.c_antientropy.release()
                    if LOCK_LOG:
                        print(self.uid, "releases a c_antientropy lock in",
                              "receive.AntiEn_Finsh")

                elif buf.mtype == "Elect":
                    self.is_primary = True

                # from master
                elif buf.mtype == "Break":
                    self.block_list.add(buf.content)

                elif buf.mtype == "Restore":
                    if buf.content[0] == "Server":
                        self.server_list.add(buf.content[1])
                    else:
                        self.client_list.add(buf.content[1])
                    try:
                        self.block_list.remove(buf.content[1])
                    except:
                        pass

                elif buf.mtype == "Pause":
                    self.is_paused = True

                elif buf.mtype == "Start":
                    self.is_paused = False

                elif buf.mtype == "Print":
                    # print(self.uid, self.committed_log, self.tentative_log)
                    self.printLog()

                elif buf.mtype == "Get":
                    song_name = buf.content
                    # print(self.uid, song_name, self.playlist, self.committed_log, self.tentative_log)
                    try:
                        song_url  = self.playlist[song_name]
                    except:
                        song_url  = "ERR_KEY"
                    m_get = Message(self.node_id, self.unique_id, "GetAck",
                                    (song_name, song_url, self.version_vector))
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
                            print(self.uid, " receives a write from Client#",
                                  buf.sender_id, sep="")
                        try:
                            Thread(target=self.handle_client_write,
                                   args=(buf,)).start()
                        except:
                            print(self.uid, "error: unable to start new thread")

    '''
    Notify server @dest_id about its joining. '''
    def notify(self, dest_id):
        self.c_create.acquire()
        if LOCK_LOG:
            print(self.uid, "acquires a c_create lock in notify")

        if self.first_creation:
            w_write = Write(self.node_id, self.unique_id, "Creation", None, 1,
                            self.first_creation)
            m_join_server = Message(self.node_id, None, "Creation", w_write)
            self.first_creation = False # a creation has been sent
            self.nt.send_to_node(dest_id, m_join_server)
            while True:
                if self.accept_time != 1:
                    break
                self.c_create.wait()
        else:
            while True:
                if self.accept_time != 1:
                    break
                self.c_create.wait()
            w_write = Write(self.node_id, self.unique_id, "Creation", None, 1,
                            self.first_creation)
            m_join_server = Message(self.node_id, None, "Creation", w_write)
            self.first_creation = False # a creation has been sent
            self.nt.send_to_node(dest_id, m_join_server)

        self.server_list.add(dest_id)
        self.c_create.release()
        if LOCK_LOG:
            print(self.uid, "releases a c_create lock in notify")

    '''
    Create a thread to handle PUT/DELETE from client. '''
    def handle_client_write(self, buff):
        buf = copy.deepcopy(buff)
        client_version_vector = buf.content.content[1]
        union_keys = set(self.version_vector.keys())\
                     .union(client_version_vector.keys())
        for i in union_keys:
            if not i in self.version_vector:
                done = Message(self.node_id, None, "Done", self.version_vector)
                self.nt.send_to_node(buf.sender_id, done)
                return
            elif i in client_version_vector and \
                 client_version_vector[i] > self.version_vector[i]:
                done = Message(self.node_id, None, "Done", self.version_vector)
                self.nt.send_to_node(buf.sender_id, done)
                return

        if LOCK_LOG:
            print(self.uid, "tries to acquire a c_antientropy",
                  "lock in receive.Write.Client")
        self.c_antientropy.acquire()
        if LOCK_LOG:
            print(self.uid, "acquires a c_antientropy lock in",
                  "receive.Write.Client")
        self.receive_client_writes(buf.content)
        self.c_antientropy.release()
        if LOCK_LOG:
            print(self.uid, "releases a c_antientropy lock in",
                  "receive.Write.Client")
        done = Message(self.node_id, None, "Done", self.version_vector)
        self.nt.send_to_node(buf.sender_id, done)

    '''
    Create a thread handle AntiEn_Reject. '''
    def handle_antien_reject(self):
        if LOCK_LOG:
            print(self.uid, "tries to acquire a c_request_antientropy lock in",
                  "receive.AntiEn_Reject")
        self.c_request_antientropy.acquire()
        if LOCK_LOG:
            print(self.uid, "acquires a c_request_antientropy lock in",
                  "receive.AntiEn_Reject")
        self.m_anti_entropy = Message(None, None, None, "Reject")
        self.c_request_antientropy.notify()
        self.c_request_antientropy.release()
        if LOCK_LOG:
            print(self.uid, "releases a c_request_antientropy lock in",
                  "receive.AntiEn_Reject")

    '''
    Process Anti-Entropy periodically. '''
    def timer_anti_entropy(self):
        if TERM_LOG and DETAIL_LOG:
            print(self.uid, "current lists: server_list", self.server_list,
                  "block_list", self.block_list)
        self.available_list = copy.deepcopy(self.server_list)
        self.available_list = self.available_list.difference(self.block_list)
        self.available_list = self.available_list.difference(self.sent_list)
        if self.node_id in self.available_list:
            self.available_list.remove(self.node_id)
        if not self.available_list or self.is_paused:
            threading.Timer(
                random.randint(self.ANTI_ENTROPY_LOWER,
                               self.ANTI_ENTROPY_UPPER)/100.0,
                self.timer_anti_entropy
            ).start()
            return

        if LOCK_LOG:
            print(self.uid, "tries to acquire a c_antientropy lock in",
                  "timer_anti_entropy")
        self.c_antientropy.acquire()
        if LOCK_LOG:
            print(self.uid, "acquires a c_antientropy lock in",
                  "timer_anti_entropy")

        rand_dest = list(self.available_list)[0]

        if TERM_LOG:
            print(self.uid, " selects to anti-entropy with Server#",
                  rand_dest, sep="")
        succeed_anti = self.anti_entropy(rand_dest)

        # select a new primary
        if self.is_retired and succeed_anti:
            if self.is_primary:
                m_elect = Message(self.node_id, self.unique_id, "Elect",
                                  None)
                self.nt.send_to_node(rand_dest, m_elect)
            m_retire = Message(self.node_id, self.unique_id,
                               "RetireAck", None)
            self.nt.send_to_master(m_retire)

        # finish the anti-entropy
        if succeed_anti:
            self.available_list.remove(rand_dest)
            self.sent_list.add(rand_dest)
            m_finish = Message(self.node_id, self.unique_id, "AntiEn_Finsh",
                               None)
            self.nt.send_to_node(rand_dest, m_finish)

        if self.is_retired and succeed_anti:
            if TERM_LOG:
                print(self.uid, "kills itself")
            os.kill(os.getpid(), signal.SIGKILL)


        threading.Timer(
            random.randint(self.ANTI_ENTROPY_LOWER,
                           self.ANTI_ENTROPY_UPPER)/100.0,
            self.timer_anti_entropy
        ).start()

        self.c_antientropy.release()
        if LOCK_LOG:
            print(self.uid, "releases a c_antientropy lock in",
                  "timer_anti_entropy")

    '''
    Process a received message of type of AntiEntropy. Stated in the paper
    Flexible Update Propagation '''
    def anti_entropy(self, receiver_id):
        if TERM_LOG and DETAIL_LOG:
            print(self.uid, " requests anti-entropy to Server#",
                  receiver_id, sep="")
        m_request_anti = Message(self.node_id, self.unique_id, "RequestAntiEn",
                                 None)
        self.m_anti_entropy = None

        if LOCK_LOG:
            print(self.uid, "tries to acquire a c_request_antientropy lock in",
                  "anti_entropy")
        self.c_request_antientropy.acquire()
        if LOCK_LOG:
            print(self.uid, "acquires a c_request_antientropy lock in",
                  "anti_entropy")

        if not self.nt.send_to_node(receiver_id, m_request_anti):
            self.c_request_antientropy.release()
            try:
                self.server_list.remove(receiver_id) # receiver has retired
            except:
                pass
            return False
        while True:
            if self.m_anti_entropy:
                break
            if LOCK_LOG:
                print(self.uid, "is waiting for c_request_antientropy in",
                      "anti_entropy")
            self.c_request_antientropy.wait()
        if isinstance(self.m_anti_entropy, Message): # was rejected
            if TERM_LOG and DETAIL_LOG:
                print(self.uid, " was rejected anti-entropy by Server#",
                      receiver_id, sep="")
            self.c_request_antientropy.release()
            if LOCK_LOG:
                print(self.uid, "releases a c_request_antientropy lock in",
                      "anti_entropy")
            return False

        if TERM_LOG and DETAIL_LOG:
            print(self.uid, " starts anti-entropy to Server#", receiver_id,
                  " >>> committed log: ", self.committed_log,
                  " >>> tentative log: ", self.tentative_log, sep="")

        m_anti_entropy = self.m_anti_entropy

        # information of receiver
        R_version_vector = m_anti_entropy.version_vector
        R_CSN            = m_anti_entropy.CSN
        R_committed_log  = m_anti_entropy.committed_log
        R_tentative_log  = m_anti_entropy.tentative_log

        # information of sender (self)
        S_version_vector = self.version_vector
        S_CSN            = self.CSN

        # anti-entropy with support for committed writes
        if R_CSN < S_CSN:
            # committed log
            for index in range(R_CSN+1, len(self.committed_log)):
                w = self.committed_log[index]
                # if w.accept_time <= R_version_vector[w.sender_uid]:
                m_commit = Message(self.node_id, self.unique_id,
                                   "Write", w)
                self.nt.send_to_node(receiver_id, m_commit)

        # print(self.uid, self.unique_id, "antientropy with", receiver_id,
        #       "R_version_vector", R_version_vector, "available_list",
        #       self.available_list, "server_list", self.server_list, "++++++++",
        #       self.tentative_log, ">>>>>>>>", self.committed_log)
        # print(self.uid, "r_vv", R_version_vector, " ~~~~ commit_log", self.committed_log, "**** tentative_log", self.tentative_log)

        # tentative log
        for w in self.tentative_log:
            if not w.sender_uid in R_version_vector or \
               R_version_vector[w.sender_uid] < w.accept_time:
                m_write = Message(self.node_id, self.unique_id, "Write", w)
                self.nt.send_to_node(receiver_id, m_write)

        self.c_request_antientropy.release()
        if LOCK_LOG:
            print(self.uid, "releases a c_request_antientropy lock in",
                  "anti_entropy")
        return True

    '''
    Receive_Writes in paper Bayou, client part. '''
    def receive_client_writes(self, wx):
        w = copy.deepcopy(wx)
        # primary commits immediately
        if self.is_primary:
            w.state = "COMMITTED"
        w.sender         = self.node_id
        w.sender_uid     = self.unique_id
        w.accept_time    = self.accept_time
        w.wid            = (self.accept_time, self.unique_id)
        w.content        = w.content[0]
        self.receive_server_writes(w)

        # update available_list
        self.available_list = self.available_list.union(self.sent_list)
        self.sent_list      = set()

    def receive_server_writes(self, wt):
        new_write = False
        w = copy.deepcopy(wt)
        # primary commits immediately
        if self.is_primary:
            w.state = "COMMITTED"

        len_committed_log = len(self.committed_log)
        len_tentative_log = len(self.tentative_log)

        if w.state == "COMMITTED":
            insert_point = len(self.committed_log)
            for i in range(len(self.committed_log)):
                if self.committed_log[i].wid == w.wid:
                    return

            # rollback
            suffix_committed_log = self.committed_log[insert_point:]
            if insert_point == 0:
                self.playlist = {}
                self.history  = []
                self.committed_log = []
            else:
                self.playlist = self.history[insert_point-1]
                self.history  = self.history[:insert_point]
                self.committed_log = self.committed_log[:insert_point]

            # insert w
            ww = copy.deepcopy(w)
            self.bayou_write(w)

            # roll forward
            for wx in suffix_committed_log:
                self.bayou_write(wx)

            # tentative roll forward
            ww.state = "TENTATIVE"
            ww.CSN = None
            try:
                self.tentative_log.remove(ww)
                whether_new = True
            except:
                whether_new = False
            tmp_tentative_log = copy.deepcopy(self.tentative_log)
            self.tentative_log = []
            for wx in tmp_tentative_log:
                self.bayou_write(wx)

            # new item added in committed log
            if (len(self.committed_log) > len_committed_log):
                new_write = True
            if TERM_LOG:
                print(self.uid, "<FINAL COMMIT>", "commit", self.committed_log,
                      "tentative", self.tentative_log)

        else:
            for i in range(len(self.committed_log)):
                if self.committed_log[i].wid == w.wid:
                    return

            insert_point = len(self.tentative_log)
            for i in range(len(self.tentative_log)):
                if self.tentative_log[i].wid == w.wid:
                    return
                if self.tentative_log[i].wid > w.wid:
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
                self.history  = self.history[:total_point]
            if insert_point == 0:
                self.tentative_log = []
            else:
                self.tentative_log = self.tentative_log[:insert_point]

            # insert w
            self.bayou_write(w)
            # roll forward
            for wx in suffix_tentative_log:
                self.bayou_write(wx)
            if (len(self.tentative_log) > len_tentative_log):
                new_write = True
            if TERM_LOG:
                print(self.uid, "<FINAL TENTATIVE>", "commit",
                      self.committed_log, "tentative", self.tentative_log)


        # update available_list
        if new_write:
            self.available_list = self.available_list.union(self.sent_list)
            self.sent_list      = set()

    '''
    Bayou_Write in paper Bayou. '''
    def bayou_write(self, w):
        if w.mtype == "Put":
            cmd = w.content.split(' ')
            self.playlist[cmd[0]] = cmd[1]
        elif w.mtype == "Delete":
            cmd = w.content
            try:
                self.playlist.pop(cmd)
            except:
                pass
        elif w.mtype == "Creation":
            self.server_list.add(w.sender_id)
        elif w.mtype == "Retirement":
            try:
                self.version_vector.pop(w.content[0])
            except:
                pass
            try:
                self.server_list.remove(w.sender_id)
            except:
                pass
            self.sent_list = set()
        if w.state == "COMMITTED":
            w.CSN = self.CSN + 1
            self.committed_log.append(w)
            self.CSN = w.CSN
        else:
            self.tentative_log.append(w)
        self.history.append(self.playlist)

        # update version_vector
        if not w.sender_uid in self.version_vector:
            self.version_vector[w.sender_uid] = w.accept_time
        else:
            self.version_vector[w.sender_uid] = \
                max(self.version_vector[w.sender_uid], w.accept_time)

    '''
    Print playlist and send it to master. '''
    def printLog(self):
        plog = ""
        contents = []
        for wx in self.committed_log:
            if wx.mtype == "Put" or wx.mtype == "Delete":
                contents = wx.content.split(' ')
            if wx.mtype == "Put":
                plog = plog + "PUT:(" + contents[0] + ", " + contents[1] + \
                       "):TRUE\n"
            elif wx.mtype == "Delete":
                plog = plog + "DELETE:(" + contents[0] + "):TRUE\n"
        for wx in self.tentative_log:
            if wx.mtype == "Put" or wx.mtype == "Delete":
                contents = wx.content.split(' ')
            if wx.mtype == "Put":
                plog = plog + "PUT:(" + contents[0] + ", " + contents[1] + \
                       "):FALSE\n"
            elif wx.mtype == "Delete":
                plog = plog + "DELETE:(" + contents[0] + "):FALSE\n"

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
