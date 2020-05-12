import random
from queue import Queue
import sys
import time
import logging
from threading import Event

from algo.nodes import Node, MasterNode
from algo.tasks import *

observer = None
master = None
nodes = dict()
event = Event()


def create_node(node_id: int, vote_responses: dict, tasks: list, node_cls=Node):
    """
    Create a node with id `node_id`, and initial money `init_money`
    """
    # initialize the new node (node1)
    node1 = node_cls(node_id, vote_responses, tasks)

    # loop through all current nodes (node2) and connect them to this new node (node1)
    for node_id2, node2 in nodes.items():
        # create 2 new queues as the channels between node1 and node2
        # node1's in-channel would be node2's out-channel and vice versa
        queue1 = Queue()    # node1 -> node2
        queue2 = Queue()    # node2 -> node1
        node1.add_in_channel(node_id2, queue2)
        node1.add_out_channel(node_id2, queue1)
        node2.add_in_channel(node_id, queue1)
        node2.add_out_channel(node_id, queue2)
    
    # add the new node to the node dictionary
    nodes[node_id] = node1
    
    # start the new node thread
    node1.start()
    
    return node1


def main():

    import os
    if os.path.exists('logs'):
        import shutil
        shutil.rmtree('logs')

    os.mkdir('logs')

    logging.basicConfig(level=logging.INFO, format='%(asctime)s, %(levelname)s: %(message)s',
                        datefmt="%Y-%m-%d %H:%M:%S")

    master = create_node(node_id=0,
                         vote_responses={0: VoteResponse(vote=1, delay=0), 1: VoteResponse(vote=1, delay=0)},
                         tasks=[SendVoteRequest(vote_id=0, time_to_execute=1),
                                KillSelf(time_to_execute=2),
                                ResumeSelf(time_to_execute=2.5),
                                SendVoteRequest(vote_id=1, time_to_execute=1.5)],
                         node_cls=MasterNode)

    participant1 = create_node(node_id=1,
                               vote_responses={0: VoteResponse(vote=0, delay=0.5), 1: VoteResponse(vote=1, delay=0.5)},
                               tasks=[KillSelf(time_to_execute=2), ResumeSelf(time_to_execute=5)],
                               node_cls=Node)
    participant2 = create_node(node_id=2,
                               vote_responses={0: VoteResponse(vote=1, delay=0.5), 1: VoteResponse(vote=1, delay=0.5)},
                               tasks=[],
                               node_cls=Node)


if __name__ == '__main__':
    main()
