To run this code, first edit config.conf to reflect the target node ids, ips, and ports of your system; ours
runs on 5 cloudlab nodes that's information is currenlty stored in the conf file. It should look like this, 
where n represents the id by which the client will call
that node, and y represents the port for clients/replicas to connect to.

config.conf:
______________________________

n xxx.xxx.xxx.xxx yyyyy
n xxx.xxx.xxx.xxx yyyyy
...
______________________________


Copy this repo onto all nodes in the system, and execute the server code, passing in the node's ID as
the only argument. This way, a node can easily identify which of conf file entries is its own.
ex:
>> python3 server.py 0

Execute this command on the other nodes. Hurry for ideal execution in which a leader is elected upon initialization in term 0.
Otherwise, nodes will attempt to become leader and fail until a quorum of the nodes defined in conf join the system and one
times out waiting to hear from the (not yet selected) leader, thus triggering re-election.

To interface with the database, run the client code locally or on a node in the raft cluster.

>> python3 client.py

Client can invoke commands such as...

>> put [key] [val]
 - if replica invoked is leader, stores key, val pair in db, else returns ID of leader node.
>> get [key]
 - returns val associated with key from prior put
>> connect [n]
 - connect to server n to start issuing commands
>> suspend
 - suspends the server client has connected to. If this server is acting as leader, re-election will be triggered.
 
REFERENCES:

Referenced this yt video for general grpc tutorial (very helpful btw)
https://www.youtube.com/watch?v=WB37L7PjI5k&t=855s

Referenced this repo for an idea of how to use gRPC for raft; looked at at their invocation of setVal
and getVal in raft.proto and client.py only
https://github.com/KuronoSangatsu7/Raft_Consensus/blob/master/client.py

