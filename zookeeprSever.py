# Sample code for CS6381
# Vanderbilt University
# Instructor: Aniruddha Gokhale
#
# Code taken from ZeroMQ examples with additional
# comments or extra statements added to make the code
# more self-explanatory or tweak it for our purposes
#
# We are executing these samples on a Mininet-emulated environment
from kazoo.client import KazooClient
from kazoo.client import KazooState
from ansible.module_utils._text import to_bytes
import logging

from kazoo.exceptions import (
    ConnectionClosedError,
    NoNodeError,
    KazooException
)
from pip._vendor.distlib.compat import raw_input

logging.basicConfig()  # set up logginga

import os
import sys
import time
import threading
import zmq
from random import randrange


class Proxy:
    """Implementation of the proxy"""

    def __init__(self):
        # Use XPUB/XSUB to get multiple contents from different publishers
        self.context = zmq.Context()
        self.xsubsocket = self.context.socket(zmq.XSUB)
        self.xpubsocket = self.context.socket(zmq.XPUB)
        self.xpubsocket.setsockopt(zmq.XPUB_VERBOSE, 1)
        self.xpubsocket.send_multipart([b'\x01', b'10001'])
        # Now we are going to create a poller
        self.poller = zmq.Poller()
        self.poller.register(self.xsubsocket, zmq.POLLIN)
        self.poller.register(self.xpubsocket, zmq.POLLIN)

        self.global_url = 0
        self.global_port = 0
        self.newSub = False

        self.topic_info_queue = []  # the content queue for different topic (zipcode)
        self.topicInd = 0
        self.zip_list = []  # the ziplist to keep track with the zipcodes received

        self.zk_object = KazooClient(hosts='127.0.0.1:2181')
        self.zk_object.start()

        self.path = '/home/'

        znode1 = self.path + "broker1"
        if self.zk_object.exists(znode1):
            pass
        else:
            # Ensure a path, create if necessary
            self.zk_object.ensure_path(self.path)
            # Create a node with data
            self.zk_object.create(znode1, to_bytes("5555,5556"))
            # Print the version of a node and its data

        znode2 = self.path + "broker2"
        if self.zk_object.exists(znode2):
            pass
        else:
            # Ensure a path, create if necessary
            self.zk_object.ensure_path(self.path)
            # Create a node with data
            self.zk_object.create(znode2, to_bytes("5557,5558"))

        znode3 = self.path + "broker3"
        if self.zk_object.exists(znode3):
            pass
        else:
            # Ensure a path, create if necessaryto_bytes
            self.zk_object.ensure_path(self.path)
            # Create a node with data
            self.zk_object.create(znode3, to_bytes("5553,5554"))

        '''
        elect a leader and put that node under the path of /leader/ and send the port number to the publisher and subscriber
        '''
        self.election = self.zk_object.Election(self.path, "leader")
        leader_list = self.election.contenders()
        self.leader = leader_list[-1].encode('latin-1')  # the leader here is a pair of address
        self.leader = str(self.leader)
        address = self.leader.split(",")
        tem = address[0]
        tem = tem.split("'")[1]
        address[0] = tem
        tem = address[1]
        tem = tem.split("'")[0]
        address[1] = tem
        pub_addr = "tcp://*:" + address[0]
        sub_addr = "tcp://*:" + address[1]
        print("Current elected broker: ", pub_addr + "," + sub_addr)
        self.xsubsocket.bind(pub_addr)
        self.xpubsocket.bind(sub_addr)

        self.watch_dir = self.path + self.leader  # use  Datawatch

        self.leader_path = "/leader/"
        self.leader_node = self.leader_path + "node"
        if self.zk_object.exists(self.leader_node):
            pass
        else:
            # Ensure a path, create if necessary
            self.zk_object.ensure_path(self.leader_path)
            # Create a node with data
            self.zk_object.create(self.leader_node, ephemeral=True)
        self.zk_object.set(self.leader_node, to_bytes(self.leader))


    def sendToSubscriber(self):

        events = dict(self.poller.poll(10000))
        if self.xsubsocket in events:
            msg = self.xsubsocket.recv_multipart()
            content = msg[0]
            content = str(content)
            content = content.split("'")[1]
            #print("content here is {}".format(content))
            zipcode, temperature, relhumidity, pub_time = content.split(" ")[:4]
            cur_msg = [zipcode,temperature, relhumidity ,pub_time]
            if zipcode not in self.zip_list:  # a new topic just come from a new publisher
                self.zip_list.append(zipcode)

                topic_info = cur_msg
                self.topic_info_queue.append(topic_info)
            else:
                zipInd = self.zip_list.index(zipcode)
                self.topic_info_queue[zipInd] = cur_msg

            self.xpubsocket.send_string("%i %i %i %i "% (int(cur_msg[0]), int(cur_msg[1]), int(cur_msg[2]), int(cur_msg[3])))

        if self.xpubsocket in events:  # a subscriber comes here
            msg = self.xpubsocket.recv_multipart()
            self.xsubsocket.send_multipart(msg)


    def schedule(self):
        while True:
            @self.zk_object.DataWatch(self.watch_dir)
            def watch_node(data, stat, event):
                if event != None:
                    print(event.type)
                    if event.type == "DELETED":  # redo a election
                        self.election = self.zk_object.Election(self.path, "leader")
                        leader_list = self.election.contenders()
                        self.leader = leader_list[-1].encode('latin-1')

                        self.zk_object.set(self.leader_node, self.leader)

                        self.xsubsocket.unbind(self.current_pub)
                        self.xpubsocket.unbind(self.current_sub)

                        address = self.leader.split(",")
                        pub_addr = "tcp://*:" + address[0]
                        sub_addr = "tcp://*:" + address[1]

                        self.xsubsocket.bind(pub_addr)
                        self.xpubsocket.bind(sub_addr)

            self.election.run(self.sendToSubscriber)

    def close(self):
        """ This method closes the PyZMQ socket. """
        self.xsubsocket.close(0)
        self.xpubsocket.close(0)

if __name__ == '__main__':
    proxy = Proxy()
    proxy.schedule()