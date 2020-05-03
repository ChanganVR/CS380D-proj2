import random
from queue import Queue
import sys
import time
from threading import Event

from .nodes import Node, MasterNode, ObserverNode

observer = None
master = None
nodes = dict()
event = Event()


def start_master():
    global master, observer
    
    # ignore the command if master is already started
    if master != None:
        return
    
    # create the master (node 0) and observer (node -1)
    master = create_node(0, 0, MasterNode)
    observer = create_node(-1, 0, ObserverNode)


def kill_all():
    """
    Terminate all nodes by setting their stop flag and join
    Note that this function is designed for an impatient user
    and will NOT process any remaining messages
    """
    global master, observer, nodes

    while len(master.ack_wait) != 0 or master.tasks.qsize() > 0:
        # print(master.ack_wait)
        time.sleep(0.1)

    # ignore this command if we have nothing to kill
    if len(nodes) == 0:
        return
    
    # terminate all nodes and wait for them to join
    for node_id, node in nodes.items():
        node.terminate()
        node.join()
    
    # reset master, observer, and node dictionary
    master = None
    observer = None
    nodes = dict()


def create_node(node_id, init_money, node_cls=Node):
    """
    Create a node with id `node_id`, and initial money `init_money`
    """
    # initialize the new node (node1)
    node1 = node_cls(node_id, init_money)

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


def send(sender_id, receiver_id, money):
    """
    Send `money` from `sender` to `receiver`
    """
    global event
    master.send_money(sender_id, receiver_id, money, event)
    event.wait()
    event.clear()


def receive(receiver_id, sender_id=None):
    """
    Receive message for `receiver`
    """
    global event
    # print out the error message and return if we don't have any node to receive from
    if len(nodes) <= 3:
        print("Error: No node to receive from!!")
        return

    master.receive_money(sender_id, receiver_id, event)
    event.wait()
    event.clear()


def receive_all():
    """
    Repeatedly pick a node to receive a message from the incoming channel
    """
    global event
    master.receive_all(nodes, event)
    event.wait()
    event.clear()


def begin_snapshot(node_id):
    global event
    master.begin_snapshot(node_id, event)
    event.wait()
    event.clear()


def collect_state():
    global event
    master.collect_state(event)
    event.wait()
    event.clear()


def print_snapshot():
    global event
    master.print_snapshot(event)
    event.wait()
    event.clear()


def unknown_cmd(cmd):
    print (f'Unknown command: {cmd}!')


def eval_command(cmd, *args):
    # convert all arguments to int
    args = map(int, args)
    
    # define the accepted command with default to unknown
    cmd_func = {
        'StartMaster': start_master,
        'KillAll': kill_all,
        'CreateNode': create_node,
        'Send': send,
        'Receive': receive,
        'ReceiveAll': receive_all,
        'BeginSnapshot': begin_snapshot,
        'CollectState': collect_state,
        'PrintSnapshot': print_snapshot,
    }.get(cmd, lambda *_: unknown_cmd(cmd))

    # actually exectue the command
    return cmd_func(*args)


def main():
    with open(sys.argv[1], 'r') as fo:
        lines = fo.read().splitlines()
        # print(lines)
        for line in lines:
            # should main be blocked until some commands are executed?
            eval_command(*line.split())
            # time.sleep(0.2)
        kill_all()


if __name__ == '__main__':
    main()