#!/usr/bin/python3

import string

class Message:

    '''
    @mtype is the type of the message. It can be either
           Get, Put, Delete, Write, AntiEntropy;
           Creation, Creation_Ack
           If @mtype is Get, Put or Delete, then the message is
           from a client; otherwise, it is from other severs
    '''
	def __init__(self, sender, sender_uid, mtype, content):
		self.sender_id  = sender
        self.sender_uid = sender_uid
		self.mtype      = mtype
		self.content    = content

	def __str__(self):
		return str(self.sender_id) + " " + str(self.sender_uid) + " " +
               str(self.mtype) + " " + str(self.content)

	def __repr__(self):
		return str(self.sender_id) + " " + str(self.sender_uid) + " " +
               str(self.mtype) + " " + str(self.content)
