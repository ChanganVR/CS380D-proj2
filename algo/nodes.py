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
        # key: vote_id, value: vote time
        self.pending_times = dict()
        # key: node_id for in channel, value: a list of (start, end) time interval for link failure
        self.in_q_failure = dict()
        # key: node_id for out channel, value: a list of (start, end) time interval for link failure
        self.out_q_failure = dict()

        self.timeout = 2
        self.killed = False

    def terminate(self):
        self.stop = True

    def add_in_channel(self, node_id, queue):
        self.in_q[node_id] = queue
        self.in_q_failure[node_id] = list()
    
    def add_out_channel(self, node_id, queue):
        self.out_q[node_id] = queue
        self.out_q_failure[node_id] = list()

    def enqueue_with_failure(self, node_id, msg):
        # discard the message if the channel currently fails
        cur_time = time.time() - self.start_time
        for period in self.out_q_failure[node_id]:
            # period[0] is start time and period[1] is end time for the link failure
            if period[0] <= cur_time <= period[1]:
                logging.info(f'Message {msg} from node {self.node_id} to {node_id} not sent due to link failure')
                return

        self.out_q.get(node_id).put(msg)
    
    def receive_queue_with_failure(self, node_id):
        # discard all messages in the channel if the channel currently fails
        cur_time = time.time() - self.start_time
        for period in self.in_q_failure[node_id]:
            # period[0] is start time and period[1] is end time for the link failure
            if period[0] <= cur_time <= period[1]:
                if self.in_q.get(node_id).qsize() > 0:
                    self.in_q.get(node_id).queue.clear()
                    logging.info(f'All messages from node {node_id} to {self.node_id} lost due to link failure')
                return

        if self.in_q.get(node_id).qsize() > 0:
            msg = self.in_q.get(node_id).get()
            msg.exec(self)

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
                    # Termination protocal
                    self.decision_request(int(vote_id))
                # else:
                    # cur_time = time.time()
                    # self.log_vote(cur_time, vote_id, 'abort')
                    # vote_id = int(vote_id)
                    # self.msg_to_send[vote_id] = (Vote(self.node_id, vote_id, 0), time_to_send) # Notify coordinator, not sure if correct

    def recover_as_master(self, lines):
        raise NotImplementedError

    def prepare_vote(self, vote_id):
        response = self.vote_responses[vote_id]
        time_to_send = time.time() + response.delay
        self.msg_to_send[vote_id] = (Vote(self.node_id, vote_id, response.vote), time_to_send)
        if response.vote:
            self.vote_yes(vote_id)
        else:
            self.vote_no(vote_id)

    def vote_yes(self, vote_id):
        self.vote_status[vote_id] = 'pending'
        logging.info(f'Node {self.node_id} votes YES for vote {vote_id}, changing status to pending')
        cur_time = time.time()
        self.log_vote(cur_time, vote_id, 'yes')
        self.pending_times[vote_id] = cur_time

    def vote_no(self, vote_id):
        self.vote_status[vote_id] = 'abort'
        logging.info(f'Node {self.node_id} votes NO for vote {vote_id}')
        self.abort(vote_id)

    def commit(self, vote_id):
        cur_time = time.time()
        self.log_vote(cur_time, vote_id, 'commit')
        logging.info(f'Node {self.node_id} commits vote {vote_id}')
        del self.vote_status[vote_id]

    def abort(self, vote_id):
        cur_time = time.time()
        self.log_vote(cur_time, vote_id, 'abort')
        logging.info(f'Node {self.node_id} aborts vote {vote_id}')
        del self.vote_status[vote_id]

    def decision_request(self, vote_id):
        # check if the master hasn't sent the decision for too long using termination protocol
        logging.info(f'Node {self.node_id} timeout, initiate decision requests for vote {vote_id}')
        for node_id, out_q in self.out_q.items():
            self.enqueue_with_failure(node_id, DecisionReq(vote_id, self.node_id))
        # reset timer for next decision request
        self.pending_times[vote_id] = time.time()

    def run(self):
        while True:
            if not self.killed:
                # receive from every node for the need of termination protocol
                for node_id, in_q in self.in_q.items():
                    self.receive_queue_with_failure(node_id)

                # check vote queue
                for vote_id, status in list(self.vote_status.items()):
                    if status == 'requested':
                        self.prepare_vote(vote_id)
                    elif status == 'commit':
                        self.commit(vote_id)
                    elif status == 'abort':
                        self.abort(vote_id)
                    elif status == 'pending' and time.time() > self.pending_times[vote_id] + self.timeout:
                        self.decision_request(vote_id)

                # check msg_to_send list
                for vote_id, (msg, time_to_send) in list(self.msg_to_send.items()):
                    cur_time = time.time()
                    if cur_time >= time_to_send:
                        self.enqueue_with_failure(0, msg)
                        self.log_vote(cur_time, vote_id, 'requested')
                        del self.msg_to_send[vote_id]

            # check tasks
            for i, task in enumerate(self.tasks):
                cur_time = time.time()
                # execute task after at least (start time + time to execute)
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
        self.jobs = list()
        self.timeout = 2

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
                    self.receive_queue_with_failure(node)

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
                                self.enqueue_with_failure(node, Commit(vote_id))
                            logging.info(f'Master decides to commit vote {vote_id}')
                            del self.votes[vote_id]
                        else:
                            abort = True
                            logging.info(f'Master decides to abort vote {vote_id} because of partial agreement')
                    elif 0 in votes:
                        abort = True
                        logging.info(f'Master decides to abort vote {vote_id} because of partial agreement')
                    elif time.time() > self.vote_req_times[vote_id] + self.timeout:
                        abort = True
                        logging.info(f'Master decides to abort vote {vote_id} because of timeout')

                    if abort:
                        cur_time = time.time()
                        self.log_status(cur_time, vote_id, 'abort')
                        for node, queue in self.out_q.items():
                            self.enqueue_with_failure(node, Abort(vote_id))
                        del self.votes[vote_id]

            # send votes
            for i, task in enumerate(self.tasks):
                cur_time = time.time()
                if cur_time - self.start_time > task.time_to_execute:
                    if not self.killed and isinstance(task, VoteResponse):
                        self.log_status(cur_time, task.vote_id, 'start')
                        task.exec(self)
                        logging.info(f'Execute {task} at time {(time.time() - self.start_time) :.1f}s')
                        self.tasks.remove(task)
                    else:
                        # Kill or resume
                        task.exec(self)
                        self.tasks.remove(task)

            time.sleep(self.sleep)
            if self.stop:
                break
