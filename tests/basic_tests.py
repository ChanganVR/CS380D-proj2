import unittest
import logging
import time

from main import create_node
from algo.nodes import Node, MasterNode
from algo.tasks import *

class BasicTest(unittest.TestCase):

    def test_commit(self):

        logging.basicConfig(level=logging.ERROR, format='%(asctime)s, %(levelname)s: %(message)s',
                        datefmt="%Y-%m-%d %H:%M:%S")

        master = create_node(node_id=0,
                             vote_responses={0: VoteResponse(vote=1, delay=0)},
                             tasks=[SendVoteRequest(vote_id=0, time_to_execute=1)],
                             node_cls=MasterNode)

        participant1 = create_node(node_id=1,
                                   vote_responses={0: VoteResponse(vote=0, delay=0.5)},
                                   tasks=[],
                                   node_cls=Node)
        participant2 = create_node(node_id=2,
                                   vote_responses={0: VoteResponse(vote=1, delay=0.5)},
                                   tasks=[],
                                   node_cls=Node)

        time.sleep(1)

        self.assertTrue(participant1.vote_responses[0])