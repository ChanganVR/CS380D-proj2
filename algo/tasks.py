import logging
from .messages import *


class SendVoteRequest:
    def __init__(self, vote_id, time_to_execute):
        self.vote_id = vote_id
        self.time_to_execute = time_to_execute

    def exec(self, node):
        for node_id, queue in node.out_q.items():
            queue.put(VoteReq(self.vote_id))

        node.vote_req_times[self.vote_id] = time.time()
        node.votes[self.vote_id] = [node.vote_responses[self.vote_id].vote] + [-1 for _ in range(len(node.out_q))]

    def __str__(self):
        return 'send vote request {}'.format(self.vote_id)


class VoteResponse:
    def __init__(self, vote, delay):
        # vote: 0 means NO and 1 means YES
        self.vote = vote
        self.delay = delay


class KillSelf:
    def __init__(self, time_to_execute):
        self.time_to_execute = time_to_execute

    def exec(self, node):
        node.killed = True
        logging.info(f'Node {node.node_id} killed')

class ResumeSelf:
    def __init__(self, time_to_execute):
        self.time_to_execute = time_to_execute

    def exec(self, node):
        node.killed = False
        logging.info(f'Node {node.node_id} resumed')
        node.recover()
