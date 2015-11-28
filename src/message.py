#!/usr/bin/python3

import string

class Message:

    '''
    @mtype is the type of the message. It can be either
           from client: Get, Put, Delete,
           from server: Write, AntiEntropy, Creation, Creation_Ack,
           from master: Retire, Join, Break, Restore, Pause, Start, Print, Put,
                        Get, Delete
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
