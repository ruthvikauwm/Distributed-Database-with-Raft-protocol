#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr  6 20:52:45 2023

@author: miaweaver
"""
from concurrent import futures
import raft_pb2
import raft_pb2_grpc
import grpc
import sys
from random import choice
import time
import signal
import math

import pickle



def set_timer(func, reset):
    global LOG, ALARM
    
    #LOG[TERM].append("Timer started...")
    if reset:
        signal.alarm(0) #stops current alarm
        
    ALARM = True
    heartbeat_timeout = choice(range(10, 20)) ##EDIT THIS..
    signal.signal(signal.SIGALRM, func)
    signal.alarm(heartbeat_timeout)
    return

def stop_timer():
    #LOG[TERM].append("Timer stopped...")
    signal.alarm(0)
    return

################################## INTERFACE WITH DB #####################################
#
#
#
##########################################################################################
def set_val(key, val):
    leader_id = None
    global LOCAL_PENDING, REMOTE_PENDING, ID, ELECTIONS, TERM
    
    leader_id = ELECTIONS[max(ELECTIONS.keys())]
    if leader_id == ID:
        if TERM not in LOCAL_PENDING.keys():
            LOCAL_PENDING[TERM] = {}
        LOCAL_PENDING[TERM][key] = val
    print("LOCAL_PENDING:", LOCAL_PENDING)
    
    if TERM in LOCAL_PENDING.keys():
        return (key in LOCAL_PENDING[TERM].keys(), str(leader_id))
    else:
        return (False, str(leader_id))

def get_val(key):  
    global LOCAL_PENDING, REMOTE_PENDING, ID, ELECTIONS, TERM
    value, outcome = "", False

    try: ##get value...
        with open('raft_db.pickle','rb') as handle:
                COMMITTED_DB = pickle.load( handle ) ##load DB
        value, outcome = COMMITTED_DB[str(key)], True
    except:
        value, outcome = "ERR", False

    return value, outcome

def update_db(key, value):
    global LOCAL_PENDING, TERM, DB_ENTRIES
    
    try:
        with open('raft_db.pickle','rb') as handle:
            COMMITTED_DB = pickle.load( handle ) ##load DB
    except:
        COMMITTED_DB = {}
    
    if key in COMMITTED_DB.keys():
        if value == COMMITTED_DB[str(key)]:
            return True
        
    if DB_ENTRIES == {}:
        DB_ENTRIES = {"0" : "root"}
    
    db_ents_as_ints = [ int(i) for i in DB_ENTRIES.keys()]
    index = max(db_ents_as_ints) + 1
    DB_ENTRIES[str(index)] = key
    COMMITTED_DB[str(key)] = str(value) #LOCAL_PENDING[TERM][key]
    
    print("DB ENTRIES...", DB_ENTRIES)
    print("COMMITED_DB...", COMMITTED_DB)
    with open('raft_db.pickle','wb+') as handle:
        pickle.dump( COMMITTED_DB, handle, protocol=pickle.HIGHEST_PROTOCOL ) ##load DB
    print("saves written...")
    return key in COMMITTED_DB


def delete_DB_entry(key, value):
    try:
        with open('raft_db.pickle','rb') as handle:
            COMMITTED_DB = pickle.load( handle ) ##load DB
    except:
        COMMITTED_DB = {}
        
    new_db = {}
    
    if key in COMMITTED_DB.keys():
        for db_key, db_value in COMMITTED_DB.keys():
            if db_key == key:
                continue
            else:
                new_db[db_key] = db_value
    
    print("COMMITED_DB...", new_db)
    with open('raft_db.pickle','wb+') as handle:
        pickle.dump( new_db, handle, protocol=pickle.HIGHEST_PROTOCOL ) ##load DB
    return

    

######################## FCNS TO HANDLE RPC INVOKING STATE CHANGE ########################
#
#
#
##########################################################################################
def handle_vote_request(request_term):
    print("handlng vote_request...")
    global PENDING_ELECTION, VOTED, TERM
    
    if request_term > TERM:
        VOTED[request_term] = False ##have not yet voted in this term...
        TERM = request_term
        
    if request_term in VOTED:
        if not VOTED[request_term]:
            VOTED[request_term] = True
            PENDING_ELECTION = True
            print("voted...")
            return True
    else:
        VOTED[request_term] = True
        PENDING_ELECTION = True
        print("voted...")
        return True

    print("not voting...")
    return False

def handle_appendEntry(key, value):
    LOCAL_PENDING[key] = value
    return LOCAL_PENDING

## ELECTION HANDLER FUNCTIONS:
#  election_lost():
#       called in case of heartbeat timeout, or upon rep db initialization, if
#       db initialized with no leader
def election_lost(leader_identified = True): ##invoked when dispatcher in candidate state and receives heartbeat
    print("ELECTION LOST...")
    global STATE, PENDING_ELECTION, VOTES, VOTED, RESET_TIMER, SET_TIMER

    VOTED[TERM + 1] = False
    PENDING_ELECTION = False #stop the election
    STATE = "F" #update state of dispatcher back to follower
    VOTES = [] ##re-initialize votes for next election
    RESET_TIMER, SET_TIMER = False, True ##flag to set timer...
    print("STATE:", STATE)
    return

#  election_timeout():
#       called when election times out; simply re-invokes election process    
def election_timeout(signum, frame):
    global ID, TERM
    LOG[TERM].append("Election timeout... invoking re-election on replica %d" % ID )
    election( timeout = True) ##commented out for now so we do not enter infinite elections

#  election():
#       prints message if initializing DB, otherwise called from timeout
#       updates dispatcher state to candidate, increments term, sets election timer
#       and sends "vote_4_me" requests to all clients
def election(timeout):
    print("invoking election...")
    global PENDING_ELECTION, TERM, STATE, LOG, VOTES, ID
    if not timeout:
        LOG[TERM].append("FOLLOWER: Initializing first election... Switching status to candidate...")
    else:
        TERM += 1
        LOG[TERM] = []
        LOG[TERM].append("FOLLOWER: Timeout occurred... Switching status to candidate for term %d" % TERM)

    PENDING_ELECTION = True
    STATE = "C"
    set_timer(election_timeout, reset = True) ##set timer...
    VOTES = []
    return
    
#  invoke_election():
#       heartbeat timeout occurred; print notification and start election process
def invoke_election(signum, frame): ##invoke election from heartbeat timeout (invoked from dispatcher)
    print("TIMEOUT: re-election invocation...")
    global STATE, LOG, ID, TERM
    if not STATE == "L": #if heartbeat timeout on non-leader replica...
        #LOG[TERM].append( "Heartbeat timed out... invoking election on replica %d" %  ID )
        election(timeout = True)
    return

'''

def handle_election_outcome(source_node, source_node_term): ##called by follower after election ends
    print("handling election outcome...")
    global PENDING_ELECTION, TERM, ELECTIONS, VOTED
    if TERM not in LOG.keys():
        LOG[TERM] = []
    return

'''

def handle_heartbeats(source_node, source_node_term):
    global STATE, LOG, VOTED, RESET_TIMER, SET_TIMER, RECEIVED_FIRST_HEARTBEAT, ALARM, ELECTIONS, TERM, PENDING_ELECTION
    RECEIVED_FIRST_HEARTBEAT = True

    TERM = source_node_term
    
    if TERM not in ELECTIONS.keys():
        ELECTIONS[TERM] = source_node
        print("ELECTIONS:", ELECTIONS)

    if PENDING_ELECTION:
        PENDING_ELECTION, VOTED[TERM+1] = False, False
        TERM = source_node_term

    if STATE == "C": ##if candidate, this means election failed...
        election_lost(leader_identified = True)
        
    if STATE == "L":    
        STATE = "F"
            
    print("checking alarms...")
    if ALARM:
        RESET_TIMER, SET_TIMER = True, False
    else:
        RESET_TIMER, SET_TIMER = False, True
        
    return
    
def compare_logs(leader_lastDB_ID, leader_lastDB_key):
    global DB_ENTRIES
    if DB_ENTRIES == {}:
        DB_ENTRIES = {"0" : "root"}
        
    lastDB_ID = max( [ int(i) for i in DB_ENTRIES.keys() ] )
    lastDB_key = DB_ENTRIES[lastDB_ID]
    
    if str(lastDB_key) == str(leader_lastDB_key) and str(lastDB_ID) == str(leader_lastDB_ID):
        print("logs match...")
        return True
    print("logs don't match... deleting entry...")
    delete_DB_entry(lastDB_ID, lastDB_key)
    return False
    
######################## RPC REQUEST STATE & SIMPLE UPDATE STATE  ##################
#
#
#
####################################################################################
def get_server_id():
    return ID

def get_leader_id():
    global ELECTIONS
    return ELECTIONS[ max(ELECTIONS.keys())]

def get_term():
    global TERM
    return TERM

def get_state():
    global STATE
    return STATE

def update_log(log_str):
    global LOG
    LOG[TERM].append(log_str)
    return

def update_state(new_state):
    global STATE
    STATE = new_state
    return

def set_suspended():
    global SUSPENDED
    SUSPENDED = True
    return

def update_term(new_term):
    global TERM
    TERM = new_term
    return

def update_elections(new_term, new_leader):
    global TERM, ELECTIONS
    TERM = new_term
    ELECTIONS[TERM] = new_leader
    return

def pending_value_lookup(key):
    global LOCAL_PENDING, TERM
    return LOCAL_PENDING[TERM][key]
######################## EVENT LOOP HELPERS, SEND RPC ##############################
#
#
#
####################################################################################
##bring replica up to date after partition...
def resend_DB_entries_from_key(server_key, last_common_key):
    global DB_ENTRIES, TERM, ID
    term_ = TERM
    commit = False
    print("Resending old entires...")
    for entryID, entryKey in DB_ENTRIES.items():
        if commit:
            try:
                params = raft_pb2.commitVal_request(source_node = ID, dst_node = server_key, term = term_,
                                                key = str(entryID), value = str(entryKey))
                response = stub.commitVal(params)
            except:
                print("The server %s is unavailable" % SERVERS[ID])
        if entryKey == last_common_key:
            commit = True
    return
    
def update_replica_log(server_key):
    print("Discrepancy found... updating replica log...")
    global DB_ENTRIES, TERM, SERVERS
    term_ = TERM
    entries_to_try = list(DB_ENTRIES.items())[:-1]
    entries_to_try.reverse()

    for DB_ID, DB_key in entries_to_try:
        print("Checking for match on...", DB_ID, DB_key)
        try:
            params = raft_pb2.heartbeat(source_node = ID, dst_node = server_key, term = term_,
                                        lastDB_ID = int(DB_ID), lastDB_key = str(DB_key))
            response = stub.heartbeatUpdate(params) ##response can be ignored...
            
            if not response.outcome:
                print("not on replica log... removing...")
                continue
            else:
                print("last common key is", DB_key)
                resend_DB_entries_from_key(server_key, DB_key, server_key)
            
        except grpc.RpcError as e:
            print("The server %s is unavailable" % SERVERS[ID])

    return    

##handled when leader...
def send_heartbeats():

    global SERVERS, TERM, ID, RECEIVED_FIRST_HEARTBEAT, DB_ENTRIES, STATE
    if STATE != "L":
        return
    
    print("sending heartbeats...")

    
    if DB_ENTRIES != {}:
        local_lastDB_ID =  max( [ int(i) for i in DB_ENTRIES.keys() ])
        local_lastDB_key = str( DB_ENTRIES[ str(local_lastDB_ID) ] )
    else:
        local_lastDB_ID = 0
        local_lastDB_key = "root"

    for server_key in SERVERS.keys():
        if server_key == ID:
            continue
        
        addr = SERVERS[server_key]
        channel = grpc.insecure_channel(addr)
        stub = raft_pb2_grpc.RaftStub(channel)
        term_ = TERM
        
        
        try:
            params = raft_pb2.heartbeat(source_node = ID, dst_node = server_key, term = term_,
                                        lastDB_ID = int(local_lastDB_ID), lastDB_key = str(local_lastDB_key) )
            response = stub.heartbeatUpdate(params) ##response can be ignored...
            
            if response.term > TERM:
                print("stale term... waiting to hear from new later...")
                STATE = "F"
                TERM = response.term
                print("Now following...")
                
            if not response.outcome:
                print("updating replica log...")
                update_replica_log(server_key)
            
        except grpc.RpcError as e:
            print("The server %s is unavailable" % SERVERS[ID])
    return    

##handled when leader...
def send_appendEntry_requests():
    global SERVERS, REMOTE_PENDING, LOCAL_PENDING, TERM, ID, TRACK_COMMITS
    if STATE != "L":
        return
    print("sending appendEntry requests...")

    if TERM not in LOCAL_PENDING.keys():
        LOCAL_PENDING[TERM] = {}
    
    for commit_key, commit_val in LOCAL_PENDING[TERM].items():                
        if TERM not in REMOTE_PENDING.keys():
            REMOTE_PENDING[TERM] = {}
            
        if commit_key not in REMOTE_PENDING[TERM].keys():
            REMOTE_PENDING[TERM][commit_key] = []

        
        for server_key in SERVERS.keys():
            if TERM in TRACK_COMMITS.keys():
                if commit_key in TRACK_COMMITS[TERM].keys():
                    if server_key in TRACK_COMMITS[TERM][commit_key]: ##already committed val...
                        continue        

            if server_key in REMOTE_PENDING[TERM][commit_key]: ##if server in list of servers that has already accepted key,val continue...
                continue
            
            if server_key == ID:
                REMOTE_PENDING[TERM][commit_key].append(ID)
                continue

            try:
                addr = SERVERS[server_key]
                channel = grpc.insecure_channel(addr)
                stub = raft_pb2_grpc.RaftStub(channel)
                
                term_ = TERM
                params = raft_pb2.appendEntry(source_node = ID, dst_node = server_key, term = term_,
                                              key = str(commit_key), value = str(commit_val))
                response = stub.AppendEntryRequest(params)
                print("VALUE PENDING ON REPLICA?", response.outcome)
                if response.outcome:
                    REMOTE_PENDING[TERM][commit_key].append(server_key)
            except grpc.RpcError as e:
                print("The server %s is unavailable" % SERVERS[ID])
    return

def update_votes(voter):
    global VOTES, PENDING_ELECTION, STATE, ELECTIONS
    quorum_reached = False
    if STATE == "C": ##if candidate and receives votes, update list of voters for candidate...
        if voter not in VOTES:
            VOTES.append(voter)
    
    print("CURRENT VOTES:", VOTES)

    if len(VOTES) >= QUORUM: ##if received a quorum of votes
        print("quorum reached. replica now leader.")
        PENDING_ELECTION = False #stop the election
        STATE = "L" #update state of dispatcher
        VOTES = [] ##re-initialize votes
        ELECTIONS[TERM] = ID
        print("ELECTIONS", ELECTIONS)
        try:
            stop_timer()
        except:
            pass
        quorum_reached = True
    return quorum_reached


def send_vote_4_mes():    
    print("sending vote_4_mes")
    global SERVERS, VOTES, ID, TERM, QUORUM, STATE
    quorum_reached = False

    for server_key in SERVERS.keys():
        
        if quorum_reached:
            break
        
        if STATE != "C":
            return
        
        if server_key == ID:
            VOTES.append(ID)
            continue
        
        if server_key in VOTES:
            continue

        addr = SERVERS[server_key]
        channel = grpc.insecure_channel(addr)
        stub = raft_pb2_grpc.RaftStub(channel)

        term_ = TERM
        try:
            params = raft_pb2.vote_4_me(source_node = ID, dst_node = server_key, term = term_)
            response = stub.VoteRequest(params)
            if response.outcome:
                quorum_reached = update_votes(server_key)
        except:
            print("The server %s is unavailable" % SERVERS[ID])


                    
    if not quorum_reached:
        print("Did not receive enough votes & quorum not reached...")
        election_lost(leader_identified = False) #update state of dispatcher
        set_timer(invoke_election, reset = False) #set timer till election invocation
        
    return

def send_commit_requests(commit_key):
    global TERM, LOCAL_PENDING, REMOTE_PENDING, ID, SERVERS, TRACK_COMMITS
    
    for server_key in SERVERS.keys():
        if TERM not in LOCAL_PENDING.keys():
            continue
        if commit_key not in LOCAL_PENDING[TERM].keys():
            continue
        
        if TERM in TRACK_COMMITS.keys():
            if commit_key in TRACK_COMMITS[TERM].keys():
                if server_key in TRACK_COMMITS[TERM][commit_key]:
                    continue
            else:
                TRACK_COMMITS[TERM][commit_key] = []
        else:
            TRACK_COMMITS[TERM] = {}
                
        print("sending commit request to...", server_key)
        
        if server_key == ID:
            continue
            
        try:
            addr = SERVERS[server_key]
            channel = grpc.insecure_channel(addr)
            stub = raft_pb2_grpc.RaftStub(channel)
            term_ = TERM        
            
            commit_value = LOCAL_PENDING[TERM][commit_key]
            params = raft_pb2.commitVal_request(source_node = ID, dst_node = server_key, term = term_,
                                                key = str(commit_key), value = str(commit_value))
            response = stub.commitVal(params)
            if response.outcome:
                if TERM not in TRACK_COMMITS.keys():
                    TRACK_COMMITS[TERM] = {}
                if commit_key not in TRACK_COMMITS[TERM].keys():
                    TRACK_COMMITS[TERM][commit_key] = []
                TRACK_COMMITS[TERM][commit_key].append(server_key)
        except:
            print("The server %s is unavailable" % SERVERS[ID])
        return

def check_remote_pending():
    ##check REMOTE_PENDING... if any term has quorum of replicas pending on committing it,
    ##commit and send out commit requests.
    #remove it from LOCAL_PENDING & REMOTE_PENDING
    if STATE != "L":
        return

    print("Checking remote pending...")
    global TERM, LOCAL_PENDING, REMOTE_PENDING

    if TERM not in LOCAL_PENDING.keys():
        LOCAL_PENDING[TERM] = {}
    
    for key, value in LOCAL_PENDING[TERM].items():
        if TERM in REMOTE_PENDING.keys():
            if key in REMOTE_PENDING[TERM].keys():
                if len( REMOTE_PENDING[TERM][key] ) >= QUORUM:                        
                    send_commit_requests(key)
                    outcome = update_db(key, value)
    return

def update_local_pending():
    global LOCAL_PENDING, TERM

    try:
        with open('raft_db.pickle','rb') as handle:
            COMMITTED_DB = pickle.load( handle ) ##load DB
    except:
        COMMITTED_DB = {}
        
    new_local_pending = {}
    
    for key, val in LOCAL_PENDING[TERM].items():
        if key in COMMITTED_DB.keys():
            if COMMITTED_DB[key] == val:
                continue
            else:
                new_local_pending[key] = val
        else:
            new_local_pending[key] = val
        
    return

            
            
    
    

######################## SERVER INIT & EVENT LOOP ##################################
#
#
#
####################################################################################
def init(server_id):
    ###FETCH DB FROM PICKLE FILE OR INITIALIZE NEW DB...
    global SERVERS
    global STATE, ID, REPLICAS, QUORUM, TERM, SUSPENDED
    global LOG, ELECTIONS, SUSPENDED
    global PENDING_ELECTION, VOTES, VOTED
    global LOCAL_PENDING, REMOTE_PENDING, DB_ENTRIES ##handling pre-commit data
    global RECEIVED_FIRST_HEARTBEAT
    global SET_TIMER, RESET_TIMER, ALARM
    global TRACK_COMMITS

    
    LOCAL_PENDING = {} ##store data until committed
    REMOTE_PENDING = {} ##leader track what is stored on other replicas

    STATE  = "F"
    ID = server_id
    REPLICAS = [i for i in SERVERS.keys() if i != ID ]
    QUORUM = math.floor( (len(REPLICAS)) / 2) + 1
    
    TERM = 0
    
    try:##define if not already defined by rpc invocations...
        type(LOG)
    except:
        LOG = { 0 : []} #TERM : LOG ENTRIES
    
    try:##define if not already defined by rpc invocations...
        type(LOG)
    except:
        ELECTIONS = {} ##no leaders yet... fill out as leaders get elected

    ELECTIONS = {} ##no leaders yet... fill out as leaders get elected
    
    try:##define if not already defined by rpc invocations...
        type(DB_ENTRIES)
    except:
        DB_ENTRIES = { "0" : "root" } 

    try:##define if not already defined by rpc invocations...
        type(SET_TIMER)
    except:
        SET_TIMER = False

    try:##define if not already defined by rpc invocations...
        type(RESET_TIMER)
    except:
        RESET_TIMER = False

    
    try:##define if not already defined by rpc invocations...
        type(VOTED)
    except:
        VOTED = { i : False for i in range(100) }

    try:##define if not already defined by rpc invocations...
        type(VOTES)
    except:
        VOTES = []

    try:##define if not already defined by rpc invocations...
        type(PENDING_ELECTION)
    except:
        PENDING_ELECTION = False

    try:##define if not already defined by rpc invocations...
        type(RECEIVED_FIRST_HEARTBEAT)
    except:
        RECEIVED_FIRST_HEARTBEAT = False
        
    try:
        type(ALARM)
    except:
        ALARM = False
        
    try:
        type(TRACK_COMMITS)
    except:
        TRACK_COMMITS = { 0 : {} }


    SUSPENDED = False ##cant suspend until first state established...
    time.sleep(choice(range(0,10)))
    
    if not VOTED[TERM] and not RECEIVED_FIRST_HEARTBEAT and not ALARM and TERM == 0:
        print("starting election...")
        set_timer(invoke_election, reset = False)
        election(timeout = False)
    print(STATE, TERM, PENDING_ELECTION, VOTED[TERM])    
    return
    

def event_loop(server):      
    global DB_ENTRIES, SUSPENDED, TERM, STATE, SET_TIMER, RESET_TIMER
    print("INIT STATE:", STATE)
    i = 0
    while True:
        i+=1
        print("DB_ENTRIES:", DB_ENTRIES)        
        if SUSPENDED: ##suspending to invoke leader re-election
            print("suspending til re-election...")
            server.stop(0)
            time.sleep(20)
            server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
            raft_pb2_grpc.add_RaftServicer_to_server(RAFTServices(), server)
            server.add_insecure_port(SERVERS[ID])
            server.start()
            time.sleep(2) ##sleep for a second & wait for new leader to send heartbeat...
            #STATE = "F"
            SUSPENDED = False
            STATE = "F" ##returned to follower state...

        if STATE == "F": ###if follower do nothing
        
            print("setting timers...")
            if not ALARM and SET_TIMER:
                print("set timer...")
                try:
                    set_timer( invoke_election, reset = True )
                except:
                    set_timer( invoke_election, reset = False )
            elif ALARM and RESET_TIMER:
                print("timer reset...")
                set_timer( invoke_election, reset = False )
            SET_TIMER, RESET_TIMER = False, False
                
        elif STATE == "C": ###if candidate send vote_4_mes to replicas that haven't voted for client yet
            send_vote_4_mes()
        elif STATE == "L": ##sending appendetnries and heartbeats
            send_heartbeats() #sends heartbeats
            check_remote_pending() #checks if quorum of replicas have accepted appendEntry, if yes, send commit requests
            send_appendEntry_requests() #sends appendEntries to replicas not in REMOTE_PENDING, if any exist for this term
            #update_local_pending()
        time.sleep(1)
    return

########################################################################

class RAFTServices(raft_pb2_grpc.RaftServicer):
    ##HANDLE RPC CALLS....
    ##handles incoming RPC and passes to leader, follower, or candidate based on status...
    #def VoteRequest(source_node, dst_node, term):
    def VoteRequest(self, request, conext):
        print("receiving vote requests...")        
        replica_term = get_term()
        replica_state = get_state()
        
        if (request.term >= replica_term) and replica_state == "F":
            outcome = handle_vote_request(request.term)
            vote_4_me_reply = raft_pb2.voted_4_u(source_node = request.dst_node, dst_node = request.source_node,
                                                 term = replica_term, outcome = outcome)
        else:
            vote_4_me_reply = raft_pb2.voted_4_u(source_node = request.dst_node, dst_node = request.source_node,
                                                 term = replica_term, outcome = False)
        print(vote_4_me_reply)
        return vote_4_me_reply

    def AppendEntryRequest(self, request, context): #source_node, dst_node, term, key, value):
        print("receiving appendEntry request...")
        replica_term = get_term()
        replica_state = get_state()

        if request.term == replica_term and replica_state == "F":
            LOCAL_PENDING = handle_appendEntry(request.key, request.value)
            appendEntry_reply = raft_pb2.appendEntry_pending(source_node = request.dst_node, dst_node =  request.source_node,
                                                             term = replica_term, outcome = request.key in LOCAL_PENDING.keys())
        else:
            appendEntry_reply = raft_pb2.appendEntry_pending(source_node = request.dst_node, dst_node =  request.source_node,
                                                             term = replica_term, outcome = False)
        return appendEntry_reply
    
    def heartbeatUpdate(self, request, context): #source_node, dst_node, term):
        print("Receiving heartbeat...")
        print("Leader:", request.source_node, "TERM:", request.term)
        replica_term = get_term()
        replica_state = get_state()
        print("STATE:", replica_state)
        
        logs_check = True
        if request.term >= replica_term:
            if request.term > replica_term:
                update_term(request.term)
                update_elections(request.term, request.source_node)
                logs_check = compare_logs(str(request.lastDB_ID), request.lastDB_key)

            if replica_state == "C": ##if candidate, this means election failed...
                log_str = str("CANDIDATE: Election for term %d lost... Returning to follower status" % replica_term )
                update_log(log_str)
                election_lost()

            if replica_state == "L": ##notified of new leader
                print(  str("LEADER: New leader detected... Updating to term %d and switching to follower status..." % replica_term) )
                log_str = str("LEADER: New leader detected... Updating to term %d and switching to follower status..." % replica_term)
                update_state("F")
                
            print("handling heartbeat...")
            handle_heartbeats(request.source_node, request.term)
            
        print("putting together reply...")
        ###COMPARE LOGS HERE...
        heartbeat_reply = raft_pb2.heartbeat_response(source_node = request.dst_node, dst_node = request.source_node,
                                                      term = replica_term, outcome = logs_check)
        print(heartbeat_reply)
        return heartbeat_reply
    
    def getVal(self, request, context):#key):
        print("Receiving get...")
        value, outcome = get_val(request.key)
        print("got:", outcome)
        response = raft_pb2.getVal_response(value = value, outcome = outcome)
        return response
    
    def setVal(self, request, context):
        print("Receiving put...")
        set_bool, leader_id = set_val(request.key, request.value)
        print("set:", set_bool)
        response = raft_pb2.setVal_response(value = leader_id, outcome = set_bool)
        return response
    
    def commitVal(self, request, context): #source_node, dst_node, term, key):
        print("Receiving commitVal...")
        replica_term = get_term()
        outcome = update_db(request.key, request.value)
        response = raft_pb2.commitVal_response(source_node = request.dst_node, dst_node = request.source_node,
                                               term = replica_term, outcome = outcome)
        return response

    def suspend(self, request, context):
        set_suspended() 
        response = raft_pb2.suspend_response(temp = 0)
        return response
    ##END HANDLE RPC CALLS....

def init_servers():
    global SERVERS
    SERVERS = {}

    with open("config.conf", "r") as f:
        for line in f:
            SERVERS[int(line.split(" ")[0])] = f'{line.split(" ")[1]}:{line.split(" ")[2].rstrip()}'

if __name__ == "__main__":
    global SERVERS, LOG, TERM, ELECTIONS
    
    #try:
    init_servers()
    print("local:", SERVERS[int(sys.argv[1])])
    
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    raft_pb2_grpc.add_RaftServicer_to_server(RAFTServices(), server)
    server.add_insecure_port(SERVERS[int(sys.argv[1])])
    server.start()
    
    print('server started; sleeping before election... ')
    time.sleep(5)
    
    init(int(sys.argv[1]))
    event_loop(server)
    
    #except Exception as e:
        ##KEYBOARD INTERRUPT... PRINT LOG & SAVE TO OUTPUT...
    #    print('Exception occured')
    #    print(e)
    '''
    try:
        with open('raft_db.pickle','rb') as handle:
            COMMITTED_DB = pickle.load( handle ) ##load DB
    except:
        COMMITTED_DB = {}
        
    if TERM not in LOG.keys():
        LOG[TERM] = []
    if ELECTIONS != {}:
        LOG[TERM].append("[END OF SESSION] leader: %d\t term: %d" % (ELECTIONS[ max(ELECTIONS.keys())], TERM) )
    LOG[TERM].append( str("[END OF SESSION] elections: " + str(list(ELECTIONS.items()))) )
    LOG[TERM].append( str("[END OF SESSION] local pendingDB:" + str(list(LOCAL_PENDING.items()))) )
    LOG[TERM].append( str("[END OF SESSION] remote pendingDB:" + str(list(REMOTE_PENDING.items()))) )
    LOG[TERM].append( str("[END OF SESSION] commited_db:" + str(list(COMMITTED_DB.items()))) )
    
    ##turn off alarm...
    stop_timer()
    for item in LOG:
        print(item)
    '''

