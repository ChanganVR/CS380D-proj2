import os.path as osp
import weakref
import time
import logging
from threading import Thread, Lock
from queue import Queue

from .messages import *
from .tasks import *


class Node(Thread):
    def __init__(self, node_id, vote_responses, tasks, freq=1000):
        super().__init__()
        self.node_id = node_id
        # self.money = init_money
                
        self.in_q = dict()
        self.out_q = dict()

        self.sleep = 1./freq
        self.stop = False
        self.start_time = time.time()
        self.vote_responses = vote_responses
        self.tasks = tasks

        # key: vote_id, value: status
        self.vote_status = dict()
        # key: vote_id, value: (msg, time-to-send)
        self.msg_to_send = dict()

        self.killed = False
    
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

    def log_vote(self, time, vote_id, msg):
        write_path = f'logs/{self.node_id}'
        write_mode = 'a' if osp.exists(write_path) else 'w'
        with open(write_path, write_mode) as file:
            file.write(f'{time}:{vote_id}:{msg}\n')

    def recover(self):
        write_path = f'logs/{self.node_id}'
        with open(write_path, 'r') as file:
            lines = file.readlines()
            for line in lines:
                _, vote_id, msg = line.rstrip().split(':')
                if msg == 'start':
                    self.recover_as_master(lines)
                    return

            # Recover as participant
            votes = dict()
            statuses = dict()

            for line in lines:
                _, vote_id, msg = line.rstrip().split(':')
                if msg in ['yes', 'no']:
                    votes[vote_id] = msg
                elif msg in ['requested', 'commit', 'abort']:
                    statuses[vote_id] = msg

            for vote_id, status in statuses.items():
                _, vote_id, msg = line.rstrip().split(':')
                if msg in ['commit', 'abort']:
                    continue
                if vote_id in votes:
                    # TODO: Termination protocal
                    raise NotImplementedError
                else:
                    cur_time = time.time()
                    self.log_vote(cur_time, vote_id, 'abort')
                    vote_id = int(vote_id)
                    self.msg_to_send[vote_id] = (Vote(self.node_id, vote_id, 0), time_to_send) # Notify coordinator, not sure if correct



    def recover_as_master(self):
        raise NotImplementedError

    def run(self):
        while True:
            if not self.killed:
                self._receive_master()

                # check vote queue
                for vote_id, status in list(self.vote_status.items()):
                    if status == 'requested':
                        response = self.vote_responses[vote_id]
                        time_to_send = time.time() + response.delay
                        self.msg_to_send[vote_id] = (Vote(self.node_id, vote_id, response.vote), time_to_send)
                        if response.vote:
                            self.vote_status[vote_id] = 'pending'
                            logging.info('Node {} votes YES for {}, changing status to pending'.format(self.node_id, vote_id))
                            cur_time = time.time()
                            self.log_vote(cur_time, vote_id, 'yes')
                        else:
                            logging.info('Node {} votes NO for {}'.format(self.node_id, vote_id))
                            cur_time = time.time()
                            # self.log_vote(cur_time, vote_id, 'no')
                            self.log_vote(cur_time, vote_id, 'abort')
                            logging.info('Node {} aborts vote {}'.format(self.node_id, vote_id))
                            del self.vote_status[vote_id]
                    elif status == 'commit':
                        cur_time = time.time()
                        self.log_vote(cur_time, vote_id, 'commit')
                        logging.info('Node {} commits vote {}'.format(self.node_id, vote_id))
                        del self.vote_status[vote_id]
                    elif status == 'abort':
                        cur_time = time.time()
                        self.log_vote(cur_time, vote_id, 'abort')
                        logging.info('Node {} aborts vote {}'.format(self.node_id, vote_id))
                        del self.vote_status[vote_id]

                # check msg_to_send list
                for vote_id, (msg, time_to_send) in list(self.msg_to_send.items()):
                    cur_time = time.time()
                    if cur_time >= time_to_send:
                        self.out_q.get(0).put(msg)
                        self.log_vote(cur_time, vote_id, 'requested')
                        del self.msg_to_send[vote_id]

            # check tasks
            for i, task in enumerate(self.tasks):
                cur_time = time.time()
                if cur_time - self.start_time > task.time_to_execute:
                    task.exec(self)
                    self.tasks.remove(task)

            time.sleep(self.sleep)
            if self.stop:
                break




class MasterNode(Node):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # self.tasks = Queue()
        self.ack_wait = list()
        self.votes = dict()
        self.vote_req_times = dict()
        self.timeout = 2
        self.jobs = list()


    def log_status(self, time, vote_id, status):
        write_path = f'logs/{self.node_id}'
        write_mode = 'a' if osp.exists(write_path) else 'w'
        with open(write_path, write_mode) as file:
            file.write(f'{time}:{vote_id}:{status}\n')


    def recover_as_master(self, lines):
        decisions = dict()
        for line in lines:
            _, vote_id, msg = line.rstrip().split(':')
            decisions[vote_id] = msg

        for vote_id, msg in decisions.items():
            if msg == 'start':
                cur_time = time.time()
                self.log_status(cur_time, vote_id, 'abort')



    def run(self):
        while True:

            if not self.killed:
                # check response from participant nodes
                for node, queue in self.in_q.items():
                    while not queue.empty():
                        msg = queue.get()
                        msg.exec(self)

                # collect messages from other nodes
                for vote_id, votes in list(self.votes.items()):
                    abort = False
                    if not (-1 in votes):
                        # received all votes
                        if sum(votes) == len(votes):
                            cur_time = time.time()
                            self.log_status(cur_time, vote_id, 'commit')
                            # all vote YES
                            for node, queue in self.out_q.items():
                                queue.put(Commit(vote_id))
                            logging.info('Master decides to commit vote {}'.format(vote_id))
                            del self.votes[vote_id]
                        else:
                            abort = True
                            logging.info('Master decides to abort vote {} because of partial agreement'.format(vote_id))


                    elif time.time() > self.vote_req_times[vote_id] + self.timeout:
                        abort = True
                        logging.info('Master decides to abort vote {} because of timeout'.format(vote_id))

                    if abort:
                        cur_time = time.time()
                        self.log_status(cur_time, vote_id, 'abort')
                        for node, queue in self.out_q.items():
                            queue.put(Abort(vote_id))
                        del self.votes[vote_id]

            # send votes
            for i, task in enumerate(self.tasks):
                cur_time = time.time()
                if cur_time - self.start_time > task.time_to_execute:
                    if not self.killed and isinstance(task, VoteResponse):
                        self.log_status(cur_time, task.vote_id, 'start')
                        task.exec(self)
                        logging.info('Execute {} at time {:.1f}s'.format(task, time.time() - self.start_time))
                        self.tasks.remove(task)
                    else:
                        # Kill or resume
                        task.exec(self)
                        self.tasks.remove(task)

            time.sleep(self.sleep)
            if self.stop:
                break

