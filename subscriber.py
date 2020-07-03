# Sample code for CS6381
# Vanderbilt University
# Instructor: Aniruddha Gokhale
#
# Code taken from ZeroMQ examples with additional
# comments or extra statements added to make the code
# more self-explanatory  or tweak it for our purposes
#
# We are executing these samples on a Mininet-emulated environment
#
#   Weather update client
#   Connects SUB socket to tcp://localhost:5556
#   Collects weather updates and finds avg temp in zipcode
from kazoo.client import KazooClient

import logging

logging.basicConfig() # set up logginga

import sys
import zmq
import time

zoo_ok = False
class Subscriber:
    """Implementation of the subscriber"""
    def __init__(self, broker_addr, zipcode):
        self.broker = broker_addr
        self.zipcode = zipcode
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.SUB)
        # Connect to broker
        self.path = '/leader/node'
        self.zk_object = KazooClient(hosts='127.0.0.1:2181')
        self.zk_object.start()

        #initializtion, what for zookeeper_server to be steady
        for i in range(5): # multiple test
            @self.zk_object.DataWatch(self.path)
            def watch_node(data, stat, event):
                if event == None: #wait for event to be alive and None(stable)
                    data, stat = self.zk_object.get(self.path)
                    global zoo_ok
                    zoo_ok = True
        if zoo_ok:   # make sure zookeeper is steady
            data, stat = self.zk_object.get(self.path)
            data = str(data)
            address = data.split(",")
            tem = address[1]
            tem = tem.split("'")[0]
            address[1] = tem
            self.connect_str = "tcp://" + self.broker + ":"+ address[1]
            print(self.connect_str)
            self.socket.connect(self.connect_str)
            # any subscriber must use the SUBSCRIBE to set a subscription, i.e., tell the
            # system what it is interested in
            self.socket.setsockopt_string(zmq.SUBSCRIBE, self.zipcode)
        else:
            print ("Zookeeper is not ready yet, please restart the subscriber later")

    def subscribe(self):
        # Keep subscribing
        while True:
            @self.zk_object.DataWatch(self.path)
            def watch_node(data, stat, event):
                if event != None:
                        print(event.type)
                        if event.type == "CHANGED": #reconnect once the election happend, change to the new leader
                            self.socket.close()
                            self.context.term()
                            time.sleep(1)
                            self.context = zmq.Context()
                            self.socket = self.context.socket(zmq.PUB)
                            # Connet to the broker
                            data, stat = self.zk_object.get(self.path)
                            address = data.split(",")
                            self.connect_str = "tcp://" + self.broker + ":"+ address[1]
                            print(self.connect_str)
                            #print ("Publisher connecting to proxy at: {}".format(connect_str))
                            self.socket.connect(self.connect_str)

            string = self.socket.recv_string()
            zipcode, temperature, relhumidity, pub_time = string.split()
            # total_temp += int(temperature)
            pub_time = float(pub_time)
            time_diff = time.time() - pub_time
            logFile = '/home/baozi/Desktop/timeLog.txt'
            f = open(logFile, "a")
            f.write("My topic is {}, The time difference is {}\n".format(zipcode, time_diff))
            f.close()
            print('ok, I got it')


    def close(self):
        """ This method closes the PyZMQ socket. """
        self.socket.close(0)

if __name__ == '__main__':
    zipcode = sys.argv[1] if len(sys.argv) > 1 else "10001"
    broker = sys.argv[2] if len(sys.argv) > 2 else "127.0.0.1"
    print('Topic: {}'.format(zipcode))
    # Python 2 - ascii bytes to unicode str
    if isinstance(zipcode, bytes):
        zipcode = zipcode.decode('ascii')
    sub = Subscriber(broker, zipcode)
    if zoo_ok:
        sub.subscribe()