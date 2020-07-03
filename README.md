# Github for cs6381 Assignment 2

Code files including three files: publisher.py, subscriber.py and zookeeperServer.py

How to set up and run the codes: 
python zookeeperServer.py python publisher.py topicIndex python subscriber topicIndex

The publisher.py and subscriber.py implemente publisher and subscriber

The Zookeeper in zookeeperServer.py use XPUB/XSUB to get multiple contents from different publishers. And we hold a schemanism
to elect a leader and put that node under the path of /leader/ and send the port number to the publisher and subscriber.

You can type a zipcode as topic. For publisher, you can pub any topic you want
for subscriber, you can get the topic if the topic has been published by publisher.

Eg: python zookeeperServer.py
    python publisher.py 100001
    python subscriber.py 100001

For the Unit Test, we test different number of publishers and subscriber in the networok, record the time of publishing and
receiving to calculate the latency in network. Subscriber send a requestion of what topic it is interested to Zookeeper, and
wait for its response until it get the publishing news.
The result of time-log and comprasion could be found in Measurement.xlsx.

