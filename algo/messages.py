import time
import logging


class Message:
    def __init__(self, sender):
        self.sender = sender
    
    def exec(self, node):
        raise NotImplementedError

    def __str__(self):
        raise NotImplementedError


class VoteReq(Message):
    def __init__(self, vote_id):
        self.vote_id = vote_id

    def exec(self, node):
        node.vote_status[self.vote_id] = 'requested'
        node.log_vote(time.time(), self.vote_id, 'requested')

    def __str__(self):
        return f'Vote Request for transaction {self.vote_id}'


class Vote(Message):
    def __init__(self, sender_id, vote_id, vote):
        self.sender_id = sender_id
        self.vote_id = vote_id
        self.vote = vote

    def exec(self, node):
        if self.vote_id in node.votes:
            node.votes[self.vote_id][self.sender_id] = self.vote
        else:
            logging.info(f'Received vote for transaction {self.vote_id} from {self.sender_id} after timeout')

    def __str__(self):
        return f'Vote for transaction {self.vote_id}'


class Commit(Message):
    def __init__(self, vote_id):
        self.vote_id = vote_id

    def exec(self, node):
        if self.vote_id in node.vote_status:
            node.vote_status[self.vote_id] = 'commit'

    def __str__(self):
        return f'Commit for transaction {self.vote_id}'


class Abort(Message):
    def __init__(self, vote_id):
        self.vote_id = vote_id

    def exec(self, node):
        if self.vote_id in node.vote_status:
            node.vote_status[self.vote_id] = 'abort'

    def __str__(self):
        return f'Abort for transaction {self.vote_id}'


class DecisionReq(Message):
    def __init__(self, vote_id, sender_id):
        self.vote_id = vote_id
        self.sender_id = sender_id

    def exec(self, node):
        statuses = list()
        read_path = f'logs/{node.node_id}'
        with open(read_path, 'r') as file:
            lines = file.readlines()
            for line in lines:
                _, vote_id, msg = line.rstrip().split(':')
                if int(vote_id) == self.vote_id:
                    if msg == 'abort':
                        # reply ABORT
                        node.enqueue_with_failure(self.sender_id, Abort(self.vote_id))
                        return
                    elif msg == 'commit':
                        # reply COMMIT
                        node.enqueue_with_failure(self.sender_id, Commit(self.vote_id))
                        return
                    else:
                        # append all other possible message to the statuses list
                        statuses.append(msg)
            if 'yes' not in statuses:
                # abort locally and notify master if the node has not voted yet
                node.vote_responses[self.vote_id].vote = 0
                node.prepare_vote(self.vote_id)
                # reply ABORT
                node.enqueue_with_failure(self.sender_id, Abort(self.vote_id))

    def __str__(self):
        return f'Decision Request for transaction {self.vote_id}'
