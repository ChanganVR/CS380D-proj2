import weakref
import time
from threading import Thread, Lock
from queue import Queue

from .messages import *

class Node(Thread):
    def __init__(self, node_id, init_money, freq=20):
        super().__init__()
        self.node_id = node_id
        self.money = init_money
                
        self.in_q = dict()
        self.out_q = dict()

        self.node_state = 0
        self.in_q_states = dict()
        self.in_q_ended = dict()
        self.taking_snapshot = False

        self.sleep = 1./freq
        self.stop = False
    
    def add_in_channel(self, node_id, queue):
        self.in_q[node_id] = queue
    
    def add_out_channel(self, node_id, queue):
        self.out_q[node_id] = queue
    
    def terminate(self):
        self.stop = True
    
    def _receive_queue(self, queue):
        if queue.qsize() > 0:
            msg = queue.get()
            msg.exec(self)
    
    def _receive_master(self):
        queue = self.in_q.get(0)
        self._receive_queue(queue)
        
    def _receive_observer(self):
        queue = self.in_q.get(-1)
        self._receive_queue(queue)
    
    def run(self):
        while True:
            if self.stop == False:
                self._receive_master()
                self._receive_observer()
                time.sleep(self.sleep)
            else:
                return


class ObserverNode(Node):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.state_wait = set([])
        self.node_states = list()
        self.channel_states = list()
        
    def _receive_states(self):
        for sender_id in self.state_wait.copy():
            queue = self.in_q.get(sender_id)
            if queue.qsize() > 0:
                message = queue.get()
                message.exec(self)
        
        # block until all responses (states) are received and processed
        if len(self.state_wait) == 0:
            # sort the collected states
            self.node_states.sort()
            self.channel_states.sort()
            
            # Send Ack to Master
            master_queue = self.out_q.get(0)
            master_queue.put(AckMessage(self.node_id))
        
    
    def run(self):
        while True:
            if self.stop == False:
                # block until all responses (states) are received and processed if collecting
                if len(self.state_wait) > 0:
                    self._receive_states()
                elif len(self.state_wait) == 0:
                    self._receive_master()
                time.sleep(self.sleep)
            else:
                return


class MasterNode(Node):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.tasks = Queue()
        self.ack_wait = list()
    
    def _receive_ack(self):
        for ack_id in self.ack_wait.copy():
            queue = self.in_q.get(ack_id)
            if queue.qsize() > 0:
                message = queue.get()
                message.exec(self)

    def _wait_for(self, ack_id):
        queue = self.in_q.get(ack_id)
        while queue.qsize() == 0:
            time.sleep(self.sleep)

        message = queue.get()
        message.exec(self)
    
    def send_money(self, sender_id, receiver_id, money, event):
        queue = self.out_q.get(sender_id)
        msg = Send
        args = (receiver_id, money)
        ack_id = sender_id
        queue.put(msg(*args))
        self._wait_for(ack_id)
        event.set()
        # self.tasks.put((queue, msg, args, ack_id, event))
        # print('send money')
        # print(self.tasks.qsize(), len(self.ack_wait))
    
    def receive_money(self, sender_id, receiver_id, event):
        queue = self.out_q.get(receiver_id)
        msg = Receive
        args = (sender_id, receiver_id)
        ack_id = receiver_id
        queue.put(msg(*args))
        self._wait_for(ack_id)
        event.set()
        # self.tasks.put((queue, msg, args, ack_id, event))
    
    def receive_all(self, nodes, event):
        """
        This method has the same effect has calling receive to each node repeatedly,
        except that the output message won't be printed
        """
        # prepare the list for all incoming channels with message(s)
        # each node_channel_tuple will be of the form 
        # (node id, incoming channel, the number of messages in the channel)
        # print('Nodes:', nodes)
        node_channel_tuples = list()
        for node_id, node in nodes.items():
            # ignore the master and observer node
            if node_id > 0:
                num_msg = sum([q.qsize() for q in node.in_q.values()])
                # print('in channel of {} is {}'.format(node.node_id, num_msg))
                channels = list(filter(lambda q: q.qsize()>0, node.in_q.values()))
                channel_sizes = [channel.qsize() for channel in channels]
                current_node_id = [node_id] * len(channels)
                node_channel_tuples += list(zip(current_node_id, channels, channel_sizes))
        
        # enqueue all receive tasks
        while len(node_channel_tuples) > 0:
            # randomly pick 1 channel with message and remove it from the list
            rand_idx = random.choice(range(len(node_channel_tuples)))
            node_channel_tuple = node_channel_tuples.pop(rand_idx)
            receiver_id = node_channel_tuple[0]
            channel = node_channel_tuple[1]
            size = node_channel_tuple[2]
            
            # enqueue the receive task to the tasks queue
            queue = self.out_q.get(receiver_id)
            msg = ReceiveOneFromAll
            args = (channel, )
            ack_id = receiver_id

            queue.put(msg(*args))
            self._wait_for(ack_id)
            # self.tasks.put((queue, msg, args, ack_id, event))
            # print('Receive all: ', queue, msg, args, ack_id)
            
            size -= 1
            # append the channel to the list if there are still message(s)
            if size > 0:
                node_channel_tuples.append((receiver_id, channel, size))

        event.set()
                
    def begin_snapshot(self, node_id, event):
        observer_id = -1
        queue = self.out_q.get(observer_id)
        msg = BeginSnapshot
        args = (node_id, )
        ack_id = observer_id
        queue.put(msg(*args))
        self._wait_for(ack_id)
        event.set()
        # self.tasks.put((queue, msg, args, ack_id, event))
        
    def collect_state(self, event):
        observer_id = -1
        queue = self.out_q.get(observer_id)
        msg = ObserverCollectState
        args = ()
        ack_id = observer_id
        queue.put(msg(*args))
        self._wait_for(ack_id)
        event.set()
        # self.tasks.put((queue, msg, args, ack_id, event))
        
    def print_snapshot(self, event):
        observer_id = -1
        queue = self.out_q.get(observer_id)
        msg = PrintSnapshot
        args = ()
        ack_id = observer_id
        queue.put(msg(*args))
        self._wait_for(ack_id)
        event.set()
        # self.tasks.put((queue, msg, args, ack_id, event))
    
    def run(self):
        pass
    #     while True:
    #         if self.stop == False:
    #             # check if we are waiting for any ack from the nodes
    #             self._receive_ack()
    #
    #             if len(self.ack_wait) == 0 and self.tasks.qsize() > 0:
    #                 # Dispatch message
    #                 queue, msg, args, ack_id, event = self.tasks.get()
    #                 print(msg, args, ack_id)
    #                 queue.put(msg(*args))
    #
    #                 # add the ack id to the set if we need to wait for some node to finish the process
    #                 if ack_id:
    #                     self.ack_wait.append(ack_id)
    #                 event.set()
    #
    #             time.sleep(self.sleep)
    #         else:
    #             return