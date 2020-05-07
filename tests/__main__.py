import unittest
import logging
import time

from main import create_node
from algo.nodes import Node, MasterNode
from algo.tasks import *

def stop(nodes):
    for node in nodes:
        node.terminate()
        node.join()

class BasicTest(unittest.TestCase):


    @classmethod
    def setUpClass(cls):

        import os
        if os.path.exists('logs'):
            import shutil
            shutil.rmtree('logs')

        os.mkdir('logs')

        logging.basicConfig(level=logging.ERROR, format='%(asctime)s, %(levelname)s: %(message)s',
                        datefmt="%Y-%m-%d %H:%M:%S")

        cls.master = create_node(node_id=0,
                             vote_responses={0: VoteResponse(vote=1, delay=0), 1: VoteResponse(vote=1, delay=0)},
                             tasks=[SendVoteRequest(vote_id=0, time_to_execute=1),
                                    SendVoteRequest(vote_id=1, time_to_execute=1)],
                             node_cls=MasterNode)

        cls.participant1 = create_node(node_id=1,
                                   vote_responses={0: VoteResponse(vote=0, delay=0.5), 1: VoteResponse(vote=1, delay=0.5)},
                                   tasks=[],
                                   node_cls=Node)
        cls.participant2 = create_node(node_id=2,
                                   vote_responses={0: VoteResponse(vote=1, delay=0.5), 1: VoteResponse(vote=1, delay=0.5)},
                                   tasks=[],
                                   node_cls=Node)

        time.sleep(2)


    def test_votes(self):

        self.assertEqual(self.participant1.vote_responses[0].vote, 0)
        self.assertEqual(self.participant2.vote_responses[0].vote, 1)
        self.assertEqual(self.participant1.vote_responses[1].vote, 1)
        self.assertEqual(self.participant2.vote_responses[1].vote, 1)

    def test_logs(self):

        vote_responses = {0:'abort', 1:'commit'}

        for node_id in [1,2]:
            with open(f'logs/{node_id}', 'r') as log1:
                for line in log1.readlines():
                    _, vote_id, status = line.rstrip().split(':')
                    vote_id = int(vote_id)
                    self.assertTrue(status == vote_responses[vote_id] or status == 'requested')

    @classmethod
    def tearDownClass(self):
        stop([self.master, self.participant1, self.participant2])


if __name__ == '__main__':
    unittest.main()