import random
import time
import logging


class Message:
    def __init__(self, sender):
        self.sender = sender
    
    def exec(self, node):
        raise NotImplementedError


class AckMessage(Message):
    def __init__(self, sender_id):
        self.sender_id = sender_id
    
    def exec(self, node):
        pass
        # node.ack_wait.remove(self.sender_id)


class Money(Message):
    def __init__(self, sender_id, money):
        self.sender_id = sender_id
        self.money = money
    
    def exec(self, node):
        node.money += self.money
        if node.taking_snapshot == True and node.in_q_ended[self.sender_id] == False:
            node.in_q_states[self.sender_id] += self.money
        # print for debugging purpose
        # print(node.node_id, node.money)


class VoteReq(Message):
    def __init__(self, vote_id):
        self.vote_id = vote_id

    def exec(self, node):
        node.vote_status[self.vote_id] = 'requested'


class Vote(Message):
    def __init__(self, sender_id, vote_id, vote):
        self.sender_id = sender_id
        self.vote_id = vote_id
        self.vote = vote

    def exec(self, node):
        if self.vote_id in node.votes:
            node.votes[self.vote_id][self.sender_id] = self.vote
        else:
            logging.info('Received vote for {} from {} after timeout'.format(self.vote_id, self.sender_id))


class Commit(Message):
    def __init__(self, vote_id):
        self.vote_id = vote_id

    def exec(self, node):
        if self.vote_id in node.vote_status:
            node.vote_status[self.vote_id] = 'commit'


class Abort(Message):
    def __init__(self, vote_id):
        self.vote_id = vote_id

    def exec(self, node):
        if self.vote_id in node.vote_status:
            node.vote_status[self.vote_id] = 'abort'


class Send(Message):
    def __init__(self, receiver_id, money):
        self.receiver_id = receiver_id
        self.money = money
    
    def exec(self, node):
        if node.money >= self.money:
            node.money -= self.money
            out_queue = node.out_q.get(self.receiver_id)
            out_queue.put(Money(node.node_id, self.money))
        
        else:
            print ('ERR_SEND')
        
        # Send Ack to Master
        master_queue = node.out_q.get(0)
        master_queue.put(AckMessage(node.node_id))


class Receive(Message):
    def __init__(self, sender_id, receiver_id):
        self.sender_id = sender_id
        self.receiver_id = receiver_id
    
    def exec(self, node):
        # Assume the message is sent
        if self.sender_id is None:
            # randomly pick 1 incoming channel to receive
            queues = list(filter(lambda q: q.qsize()>0, node.in_q.values()))
            queue = random.choice(queues)
        else:
            queue = node.in_q.get(self.sender_id)
        msg = queue.get()
        msg.exec(node)
        
        if isinstance(msg, Money):
            print (f'{msg.sender_id} Transfer {msg.money}')
        elif isinstance(msg, TakeSnapshot):
            print (f'{msg.sender_id} SnapshotToken -1')
        
        # Send Ack to Master
        master_queue = node.out_q.get(0)
        master_queue.put(AckMessage(node.node_id))


class ReceiveOneFromAll(Message):
    def __init__(self, channel):
        self.channel = channel
    
    def exec(self, node):
        # Assume the message is sent
        msg = self.channel.get()
        msg.exec(node)
        
        # Send Ack to Master
        master_queue = node.out_q.get(0)
        master_queue.put(AckMessage(node.node_id))


class ReceiveAll(Message):
    def __init__(self, channel):
        self.channel = channel

    def exec(self, node):
        # Assume the message is sent
        msg = self.channel.get()
        msg.exec(node)

        # Send Ack to Master
        master_queue = node.out_q.get(0)
        master_queue.put(AckMessage(node.node_id))


class BeginSnapshot(Message):
    """
    The message master sends to observer to begin the snapshot
    """
    def __init__(self, start_node_id):
        self.start_node_id = start_node_id
    
    def exec(self, node):
        # enqueue the message to the channel to start node
        out_queue = node.out_q.get(self.start_node_id)
        msg = TakeSnapshot(node.node_id)
        out_queue.put(msg)
        print(f'Started by Node {self.start_node_id}')

        while not msg.received:
            time.sleep(0.05)
        
        # Send Ack to Master
        master_queue = node.out_q.get(0)
        master_queue.put(AckMessage(node.node_id))


class TakeSnapshot(Message):
    """
    The message observer sends to the first node to begin the snapshot
    or the message each node broadcasts to others when received for the first time
    """
    def __init__(self, sender_id):
        self.sender_id = sender_id
        self.received = False
    
    def exec(self, node):
        self.received = True

        # if the node has seen the message
        if node.taking_snapshot == True:
            # stop to record the incoming channel state
            node.in_q_ended[self.sender_id] = True
        
        # if the nodes see this message for the first time
        else:
            node.taking_snapshot = True
            
            # record the node's own state
            node.node_state = node.money
            
            # start to record the incoming channel state
            for sender_id, queue in node.in_q.items():
                node.in_q_states[sender_id] = 0
                node.in_q_ended[sender_id] = False
            node.in_q_ended[self.sender_id] = True
            
            # broadcast the token to all outgoing channels
            for receiver_id, queue in node.out_q.items():
                if receiver_id > 0:
                    queue.put(TakeSnapshot(node.node_id))


class State(Message):
    def __init__(self, sender_id, node_state, channel_states):
        self.sender_id = sender_id
        self.node_state = node_state
        self.channel_states = channel_states
    
    def exec(self, node):
        # add the node state to observer
        node.node_states.append((self.sender_id, self.node_state))
        
        # add the channel states to observer
        for sender_id, channel_state in self.channel_states.items():
            if sender_id > 0:
                node.channel_states.append((sender_id ,self.sender_id, channel_state))
                
        # remove the node id from the waiting set
        node.state_wait.remove(self.sender_id)


class ObserverCollectState(Message):
    """
    The message master sends to observer to collect state
    """
    def __init__(self):
        pass
    
    def exec(self, node):
        # empty lists before collecting
        node.state_wait = set([])
        node.node_states = list()
        node.channel_states = list()

        # send CollectState message to every node and wait for responses
        for receiver_id, queue in node.out_q.items():
            if receiver_id > 0:
                queue.put(CollectState(node.node_id))
                node.state_wait.add(receiver_id)


class CollectState(Message):
    """
    The message observer sends to each node to collect state
    """
    def __init__(self, sender_id):
        self.sender_id = sender_id
    
    def exec(self, node):
        # print for debugging
        # print(node.node_id, node.node_state)
        # for sender_id, queue in node.in_q.items():
        #    print(sender_id, node.node_id, node.in_q_states[sender_id])
        
        # send node and channel states to the observer
        observer_queue = node.out_q.get(self.sender_id)
        observer_queue.put(State(node.node_id, node.node_state, node.in_q_states))
        
        # reset node and channel states
        node.taking_snapshot = False
        node.node_state = 0
        node.in_q_states = dict()
        node.in_q_ended = dict()


class PrintSnapshot(Message):
    """
    The message master sends to observer to output states
    """
    def __init__(self):
        pass
    
    def exec(self, node):
        # output node states
        print("---Node states")
        for node_id, node_state in node.node_states:
            print(f'node {node_id} = {node_state}')
            
        # output channel states
        print("---Channel states")
        for sender_id, receiver_id, channel_state in node.channel_states:
            print(f'channel ({sender_id} -> {receiver_id}) = {channel_state}')
            
        # Send Ack to Master
        master_queue = node.out_q.get(0)
        master_queue.put(AckMessage(node.node_id))