##Referenced this yt video for general grpc tutorial (very helpful btw)
#https://www.youtube.com/watch?v=WB37L7PjI5k&t=855s

##Referenced this repo for an idea of how to use gRPC for raft; looked at at their invocation of setVal
#and getVal in raft.proto and client.py
#https://github.com/KuronoSangatsu7/Raft_Consensus/blob/master/client.py

import grpc
import raft_pb2
import raft_pb2_grpc
import sys

    
def parse( msg):
    command =  msg.split(" ")[0]
    parsed_msg =  msg.split(" ")

    if command == "connect" or command == "Connect":
        return ["Connect", parsed_msg[1]]
    
    elif command == "suspend" or command == "Suspend":
        return ["Suspend"]
    
    elif command == "put" or command == "Put":
        return ["setVal", parsed_msg[1], parsed_msg[2]]
    
    elif command == "get" or command == "Get":
        return ["getVal", parsed_msg[1]]
    
    elif command == "quit" or command == "Quit" or command == "Q":
        return ["Quit"]
    
    else:
        return ("Invalid")

def suspend():
    global SERVER
    channel = grpc.insecure_channel(SERVER)
    stub = raft_pb2_grpc.RaftStub(channel)

    try:
        params = raft_pb2.suspend_request(temp = 0) ##temp is meaningless... idk how to make an empty rpc msg
        response = stub.suspend(params)
    except grpc.RpcError as e:
            print("gRPC ERR: ", e)
            fail()
    return

def set_val(key, value):
    global SERVER
    channel = grpc.insecure_channel(SERVER)
    stub = raft_pb2_grpc.RaftStub(channel)

    try:
        params = raft_pb2.setVal_request(key=key, value=value)
        response = stub.setVal(params)
        if not response.outcome:
            print("connect to ", response.value, " to set values...")
    except grpc.RpcError as e:
            print("gRPC ERR: ", e)
            fail()
    return

def get_val(key):
    global SERVER

    channel = grpc.insecure_channel(SERVER)
    stub = raft_pb2_grpc.RaftStub(channel)

    try:
        params = raft_pb2.getVal_request(key=key)
        response = stub.getVal(params)

        if response.outcome == False:
            print("ERROR: No value associated with key")
        else:
            print(response.value)
                    
    except grpc.RpcError  as e:
            print("gRPC ERR: ", e)
            fail()
    return

# Terminate the client
def terminate():
    print("Terminating client.")
    sys.exit(0)
    
def fail():
    global SERVER
    print("The server %s is unavailable" % SERVER)
    return

def connect(ip_addr_port_num):
    global SERVER
    SERVER = ip_addr_port_num

# Initialize the client
def init():
    print("Initializing...")
    
    global SERVERS
    SERVERS = {}
    with open("config.conf", "r") as f:
        for line in f:
            SERVERS[int(line.split(" ")[0])] = f'{line.split(" ")[1]}:{line.split(" ")[2].rstrip()}'

    print("SERVERS accepting connections:")
    for node, ip_port in SERVERS.items():
        print(node, ip_port)

    while True:
        try:
            user_input = input(">")
            parsed_input = parse(user_input)
            msg_type = parsed_input[0]

            if  msg_type == "Connect" and len(parsed_input) >= 2 : ##make sure enough input to handle request
                connect(parsed_input[1])
                
            elif  msg_type == "Suspend":
                suspend()
                
            elif  msg_type == "setVal" and len(parsed_input) >= 3: ##make sure enough input to handle request
                set_val(parsed_input[1], parsed_input[2])
                
            elif  msg_type == "getVal" and len(parsed_input) >= 2: ##make sure enough input to handle request
                get_val(parsed_input[1])
                
            elif msg_type == "Quit":
                terminate()
                
            else:
                print("Invalid command! Please try again.")

        except KeyboardInterrupt:
            terminate()

if __name__ == "__main__":
    init()
