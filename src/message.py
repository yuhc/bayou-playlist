#!/usr/bin/python3

import string

class Message:

    '''
    @mtype is the type of the message. It can be either
           TODO: rewrite this comment
           from client: Get, Put, Delete, GetAck
           from server: Write, RequestAntiEn, AntiEntropy, AntiEn_Ack,
                        AntiEn_Finsh, Creation, Creation_Ack
           from master: Retire, Join, Break, Restore, Pause, Start, Print, Get,
                        Write
           to master: MGetAck, Playlist
    '''
    def __init__(self, sender, sender_uid, mtype, content):
        self.sender_id  = sender
        self.sender_uid = sender_uid # unique_id
        self.mtype      = mtype
        self.content    = content

    '''
    Message format:
        (Message, sender_id, sender_unique_id, message_type, message_content)
    '''
    def __str__(self):
        return str(("Message", self.sender_id, self.sender_uid, self.mtype,
                    self.content))

    def __repr__(self):
        return str(("Message", self.sender_id, self.sender_uid, self.mtype,
                    self.content))

class AntiEntropy:
    def __init__(self, sender, version_vector, CSN, commit_log=[], tent_log=[]):
        self.sender_id      = sender
        self.version_vector = version_vector
        self.CSN            = CSN        # commit sequence number
        self.committed_log  = commit_log # may be not necessary
        self.tentative_log  = tent_log

    '''
    Message format:
        (AntiEntropy, sender_id, version_vector, CSN, committed_log,
         tentative_log) '''
    def __str__(self):
        return str(("AntiEntropy", self.sender_id, self.version_vector,
                    self.CSN, self.committed_log, self.tentative_log))

    def __repr__(self):
        return str(("AntiEntropy", self.sender_id, self.version_vector,
                    self.CSN, self.committed_log, self.tentative_log))


class Write:
    '''
    A Write is created when a server receives any message from a client.
    @mtype is either Creation, Retirement, Put or Delete. '''
    def __init__(self, sender, sender_uid, mtype, CSN, accept_time, content):
        self.sender_id   = sender
        self.sender_uid  = sender_uid  # unique_id
        self.mtype       = mtype
        self.CSN         = CSN
        self.accept_time = accept_time
        self.wid         = (accept_time, sender_uid)
        self.state       = "TENTATIVE" # or "COMMITTED"
        self.content     = content     # content is a string

    def is_tentative(self):
        return not self.is_notice() and self.CSN < 0

    def is_committed(self):
        return not self.is_notice() and self.CSN >= 0

    def is_notice(self):
        return self.mtype in ["Creation", "Retirement"]

    def __eq__(self, other):
        return str(self) == str(other) and self.wid == other.wid and self.state == other.state

    '''
    Message format:
        (Write, sender_id, message_type, CSN, accept_time, message_content) '''
    def __str__(self):
        return str(("Write", self.sender_id, self.sender_uid, self.mtype, self.CSN,
                    self.accept_time, self.content))

    def __repr__(self):
        return str(("Write", self.sender_id, self.sender_uid, self.mtype, self.CSN,
                    self.accept_time, self.content))
