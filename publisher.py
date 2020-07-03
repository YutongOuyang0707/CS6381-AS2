# Sample code for CS6381
# Vanderbilt University
# Instructor: Aniruddha Gokhale
# Code based on xsub and xpub
# We are executing these samples on a Mininet-emulated environment

from kazoo.client import KazooClient
import logging

logging.basicConfig() # set up logginga

import sys
import zmq
from random import randrange
import time

zoo_ok = False

class Publisher:
    """Implementation of a publisher with ownership_strength"""
    def __init__(self, broker_addr):
        self.broker = broker_addr
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PUB)
        # Connet to the broker
        self.path = '/leader/node'

        self.zk_object = KazooClient(hosts='127.0.0.1:2181') #or to make it multiple zookeepers.....
        self.zk_object.start()

        #initializtion, what for zookeeper_server to be steady
        for i in range(5): # multiple test
            @self.zk_object.DataWatch(self.path)
            def watch_node(data, stat, event):
                if event == None: #wait for event to be alive and None(stable)
                    data, stat = self.zk_object.get(self.path)
                    global zoo_ok
                    zoo_ok = True
        if zoo_ok:   # make sure zoo keeper is ok,
            data, stat = self.zk_object.get(self.path)
            print("Data changed for znode: data = {} state = {}".format(data, stat))
            data = str(data)
            address = data.split(",")
            tem = address[0]
            tem = tem.split("'")[1]
            address[0] = tem
            #address = data.split(",")
            self.connect_str = "tcp://" + self.broker + ":"+ address[0]
            print(self.connect_str)
            self.socket.connect(self.connect_str)
        else:
            print("Zookeeper is not ready yet, please restart the publisher later")

    def publish(self,zipcode):
        zipcode = int(zipcode)
        while True:
            @self.zk_object.DataWatch(self.path)
            def watch_node(data, stat, event):
                if event != None:
                        print(event.type)
                        if event.type == "CHANGED":
                            self.socket.close()
                            self.context.term()
                            time.sleep(1)
                            self.context = zmq.Context()
                            self.socket = self.context.socket(zmq.PUB)

                            # Connet to the broker
                            data, stat = self.zk_object.get(self.path)
                            address = data.split(",")
                            self.connect_str = "tcp://" + self.broker + ":"+ address[0]
                            print(self.connect_str)
                            #print ("Publisher connecting to proxy at: {}".format(connect_str))
                            self.socket.connect(self.connect_str)

            temperature = randrange(-80, 135)
            relhumidity = randrange(10, 60)
            #print ("Sending: %i %i %i" % (zipcode, temperature, relhumidity))
            pub_timestamp = time.time()
            self.socket.send_string("%i %i %i %i" % (zipcode, temperature, relhumidity, pub_timestamp))
            # print("publisher time is: ", pub_timestamp)
            time.sleep(1)

    def close(self):
        """ This method closes the PyZMQ socket. """
        self.socket.close(0)

if __name__ == '__main__':
    zipcode = sys.argv[1] if len(sys.argv) > 1 else '10001'
    broker = sys.argv[3] if len(sys.argv) > 3 else "127.0.0.1"
    print ('Topic:',zipcode)
    pub = Publisher(broker)
    if zoo_ok:
        print("--Start publishing--")
        pub.publish(zipcode)