import os
import shutil

from queue import Queue

from algo.nodes import Node, MasterNode
from algo.tasks import *


nodes = dict()


def create_node(node_id: int, vote_responses: dict, tasks: list, node_cls=Node):
    """
    Create a node with id `node_id`,  vote response for each vote request, tasks to be executed by that node, of type node_cls
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


def link_failure(from_node, to_node, start_time, end_time):
    """
    Simulate link failures for channels, all messages in the channel would be discard during this time
    :param from_node:  sender of the channel
    :param to_node:  receiver of the channel
    :param start_time:  the time when the link failure occurs
    :param end_time:  the time when the link failure is repaired
    :return:
    """
    period = (start_time, end_time)
    from_node.out_q_failure[to_node.node_id].append(period)
    to_node.in_q_failure[from_node.node_id].append(period)


def network_partition(node_group1, node_group2, start_time, end_time):
    """
    Simulate network partition failure
    Partition the nodes into 2 groups, where nodes in group1 can not communicate with nodes in group2 during the specified time
    :param node_group1:  a list of nodes in group1
    :param node_group2:  a list of nodes in group2
    :param start_time:  the time when the network partition failure occurs
    :param end_time:  the time when the network partition is repaired
    :return:
    """
    for n1 in node_group1:
        for n2 in node_group2:
            link_failure(n1, n2, start_time, end_time)
            link_failure(n2, n1, start_time, end_time)


def stop():
    global nodes
    for node_id, node in nodes.items():
        node.terminate()
        node.join()

    nodes = dict()


def main():
    if os.path.exists('logs'):
        shutil.rmtree('logs')
    os.mkdir('logs')

    logging.basicConfig(level=logging.INFO, format='%(asctime)s, %(levelname)s: %(message)s',
                        datefmt="%Y-%m-%d %H:%M:%S")

    master = create_node(node_id=0,
                         vote_responses={0: VoteResponse(vote=1, delay=0), 1: VoteResponse(vote=1, delay=0)},
                         tasks=[SendVoteRequest(vote_id=0, time_to_execute=1),
                                # KillSelf(time_to_execute=2),
                                # ResumeSelf(time_to_execute=2.5),
                                SendVoteRequest(vote_id=1, time_to_execute=1.5)],
                         node_cls=MasterNode)

    participant1 = create_node(node_id=1,
                               vote_responses={0: VoteResponse(vote=0, delay=0.5), 1: VoteResponse(vote=1, delay=0.5)},
                               tasks=[],
                               node_cls=Node)
    participant2 = create_node(node_id=2,
                               vote_responses={0: VoteResponse(vote=1, delay=0.5), 1: VoteResponse(vote=1, delay=0.5)},
                               tasks=[],
                               node_cls=Node)

    # link_failure(participant1, master, 2, 5)
    # link_failure(master, participant1, 2, 5)
    network_partition([master, participant1], [participant2], 2, 5)


if __name__ == '__main__':
    main()
