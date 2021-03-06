#! /usr/bin/python3

import fileinput
import string
import subprocess, sys, os, signal, time

from threading import Thread, Lock
from network   import Network
from api       import API, CMD_DEBUG


if __name__ == "__main__":
    api = API()

    for line in fileinput.input():
        if CMD_DEBUG:
            print("#", line.strip())

        line = line.split()
        if line[0] ==  "joinServer":
            serverId = int(line[1])
            """
            Start up a new server with this id and connect it to all servers
            """
            api.joinServer(serverId)

        if line[0] ==  "retireServer":
            serverId = int(line[1])
            """
            Retire the server with the id specified. This should block until
            the server can tell another server of its retirement
            """
            api.retireServer(serverId)

        if line[0] ==  "joinClient":
            clientId = int(line[1])
            serverId = int(line[2])
            """
            Start a new client with the id specified and connect it to
            the server
            """
            api.joinClient(clientId, serverId)

        if line[0] ==  "breakConnection":
            id1 = int(line[1])
            id2 = int(line[2])
            """
            Break the connection between a client and a server or between
            two servers
            """
            api.breakConnection(id1, id2)

        if line[0] ==  "restoreConnection":
            id1 = int(line[1])
            id2 = int(line[2])
            """
            Restore the connection between a client and a server or between
            two servers
            """
            api.restoreConnection(id1, id2)

        if line[0] ==  "pause":
            """
            Pause the system and don't allow any Anti-Entropy messages to
            propagate through the system
            """
            api.pause()

        if line[0] ==  "start":
            """
            Resume the system and allow any Anti-Entropy messages to
            propagate through the system
            """
            api.start()

        if line[0] ==  "stabilize":
            """
            Block until there are enough Anti-Entropy messages for all values to
            propagate through the currently connected servers. In general, the
            time that this function blocks for should increase linearly with the
            number of servers in the system.
            """
            api.stabilize()

        if line[0] ==  "printLog":
            serverId = int(line[1])
            """
            Print out a server's operation log in the format specified in the
            handout.
            """
            api.printLog(serverId)

        if line[0] ==  "put":
            clientId = int(line[1])
            songName = line[2]
            URL = line[3]
            """
            Instruct the client specified to associate the given URL with the given
            songName. This command should block until the client communicates with
            one server.
            """
            api.put(clientId, songName, URL)

        if line[0] ==  "get":
            clientId = int(line[1])
            songName = line[2]
            """
            Instruct the client specified to attempt to get the URL associated with
            the given songName. The value should then be printed to standard out of
            the master script in the format specified in the handout. This command
            should block until the client communicates with one server.
            """
            api.get(clientId, songName)

        if line[0] ==  "delete":
            clientId = int(line[1])
            songName = line[2]
            """
            Instruct the client to delete the given songName from the playlist.
            This command should block until the client communicates with one server.
            """
            api.delete(clientId, songName)

    api.exit()
