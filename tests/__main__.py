import unittest
import os
import shutil

from main import create_node, link_failure, network_partition, stop
from algo.nodes import Node, MasterNode
from algo.tasks import *


def check_log(expected_results, num_nodes=3):
    # get the results from log files
    results = dict()
    for node_id in range(num_nodes):
        results[node_id] = dict()
        with open(f'logs/{node_id}', 'r') as log1:
            for line in log1.readlines():
                _, vote_id, status = line.rstrip().split(':')
                vote_id = int(vote_id)
                if status in ['abort', 'commit']:
                    results[node_id][vote_id] = status

    # check if every result meet the expected results
    for node_id in range(num_nodes):
        if node_id not in results:
            return False
        for vote_id, status in expected_results.items():
            if vote_id not in results[node_id]:
                return False
            if status != results[node_id][vote_id]:
                return False
    return True


class BasicTest(unittest.TestCase):
    """
    Normal cases. Transaction 1 should abort since participant 1 votes NO and transaction 2 should commit because of unanimous YES.
    """
    @classmethod
    def setUpClass(cls):
        if os.path.exists('logs'):
            shutil.rmtree('logs')
        os.mkdir('logs')

        logging.basicConfig(level=logging.ERROR, format='%(asctime)s, %(levelname)s: %(message)s',
                            datefmt="%Y-%m-%d %H:%M:%S")

        cls.master = create_node(node_id=0,
                                 vote_responses={0: VoteResponse(vote=1, delay=0), 1: VoteResponse(vote=1, delay=0)},
                                 tasks=[SendVoteRequest(vote_id=0, time_to_execute=1),
                                    SendVoteRequest(vote_id=1, time_to_execute=2)],
                                 node_cls=MasterNode)

        cls.participant1 = create_node(node_id=1,
                                       vote_responses={0: VoteResponse(vote=0, delay=0.5), 1: VoteResponse(vote=1, delay=0.5)},
                                       tasks=[],
                                       node_cls=Node)
        cls.participant2 = create_node(node_id=2,
                                       vote_responses={0: VoteResponse(vote=1, delay=0.5), 1: VoteResponse(vote=1, delay=0.5)},
                                       tasks=[],
                                       node_cls=Node)

        time.sleep(3)

    def test_logs(self):
        expected_vote_responses = {0: 'abort', 1: 'commit'}
        self.assertTrue(check_log(expected_vote_responses, 3))

    @classmethod
    def tearDownClass(cls):
        stop()


class ParticipantFailBeforeVoteTest(BasicTest):
    """
    Participant 1 dies before sending back the vote, master should timeout and abort the transaction.
    Participant 1 should also abort after recovery using termination protocol
    """
    @classmethod
    def setUpClass(cls):
        if os.path.exists('logs'):
            shutil.rmtree('logs')
        os.mkdir('logs')

        logging.basicConfig(level=logging.ERROR, format='%(asctime)s, %(levelname)s: %(message)s',
                            datefmt="%Y-%m-%d %H:%M:%S")

        cls.master = create_node(node_id=0,
                                 vote_responses={0: VoteResponse(vote=1, delay=0)},
                                 tasks=[SendVoteRequest(vote_id=0, time_to_execute=1)],
                                 node_cls=MasterNode)

        cls.participant1 = create_node(node_id=1,
                                       vote_responses={0: VoteResponse(vote=1, delay=0.5)},
                                       tasks=[KillSelf(time_to_execute=1.2), ResumeSelf(time_to_execute=2)],
                                       node_cls=Node)
        cls.participant2 = create_node(node_id=2,
                                       vote_responses={0: VoteResponse(vote=1, delay=0.5)},
                                       tasks=[],
                                       node_cls=Node)

        time.sleep(4)

    def test_logs(self):
        expected_vote_responses = {0: 'abort'}
        self.assertTrue(check_log(expected_vote_responses, 3))


class ParticipantFailAfterVoteTest(BasicTest):
    """
    Participant 1 dies after sending back the vote and didn't get the commit message from the master.
    Should use termination protocol after recovery and commit
    """
    @classmethod
    def setUpClass(cls):
        if os.path.exists('logs'):
            shutil.rmtree('logs')
        os.mkdir('logs')

        logging.basicConfig(level=logging.ERROR, format='%(asctime)s, %(levelname)s: %(message)s',
                            datefmt="%Y-%m-%d %H:%M:%S")

        cls.master = create_node(node_id=0,
                                 vote_responses={0: VoteResponse(vote=1, delay=0)},
                                 tasks=[SendVoteRequest(vote_id=0, time_to_execute=1)],
                                 node_cls=MasterNode)

        cls.participant1 = create_node(node_id=1,
                                       vote_responses={0: VoteResponse(vote=1, delay=0.5)},
                                       tasks=[KillSelf(time_to_execute=1.7), ResumeSelf(time_to_execute=3)],
                                       node_cls=Node)
        cls.participant2 = create_node(node_id=2,
                                       vote_responses={0: VoteResponse(vote=1, delay=0.5)},
                                       tasks=[],
                                       node_cls=Node)

        time.sleep(4)

    def test_logs(self):
        expected_vote_responses = {0: 'commit'}
        self.assertTrue(check_log(expected_vote_responses, 3))


class MasterFailAfterVoteRequestTest(BasicTest):
    """
    Master dies after sending the vote requests and didn't get back the votes.
    Should abort directly and notify every participant after recovery.
    """
    @classmethod
    def setUpClass(cls):
        if os.path.exists('logs'):
            shutil.rmtree('logs')
        os.mkdir('logs')

        logging.basicConfig(level=logging.ERROR, format='%(asctime)s, %(levelname)s: %(message)s',
                            datefmt="%Y-%m-%d %H:%M:%S")

        cls.master = create_node(node_id=0,
                                 vote_responses={0: VoteResponse(vote=1, delay=0)},
                                 tasks=[SendVoteRequest(vote_id=0, time_to_execute=1), KillSelf(time_to_execute=1.2),
                                        ResumeSelf(time_to_execute=2.5)],
                                 node_cls=MasterNode)

        cls.participant1 = create_node(node_id=1,
                                       vote_responses={0: VoteResponse(vote=1, delay=0.5)},
                                       tasks=[],
                                       node_cls=Node)
        cls.participant2 = create_node(node_id=2,
                                       vote_responses={0: VoteResponse(vote=1, delay=0.5)},
                                       tasks=[],
                                       node_cls=Node)

        time.sleep(4)

    def test_logs(self):
        expected_vote_responses = {0: 'abort'}
        self.assertTrue(check_log(expected_vote_responses, 3))


class MasterFailAfterDecisionTest(BasicTest):
    """
    Master dies after sending the decision to each node.
    Should commit before dying and do nothing after recovery
    """
    @classmethod
    def setUpClass(cls):
        if os.path.exists('logs'):
            shutil.rmtree('logs')
        os.mkdir('logs')

        logging.basicConfig(level=logging.ERROR, format='%(asctime)s, %(levelname)s: %(message)s',
                            datefmt="%Y-%m-%d %H:%M:%S")

        cls.master = create_node(node_id=0,
                                 vote_responses={0: VoteResponse(vote=1, delay=0)},
                                 tasks=[SendVoteRequest(vote_id=0, time_to_execute=1), KillSelf(time_to_execute=2),
                                        ResumeSelf(time_to_execute=2.5)],
                                 node_cls=MasterNode)

        cls.participant1 = create_node(node_id=1,
                                       vote_responses={0: VoteResponse(vote=1, delay=0.5)},
                                       tasks=[],
                                       node_cls=Node)
        cls.participant2 = create_node(node_id=2,
                                       vote_responses={0: VoteResponse(vote=1, delay=0.5)},
                                       tasks=[],
                                       node_cls=Node)

        time.sleep(3)

    def test_logs(self):
        expected_vote_responses = {0: 'commit'}
        self.assertTrue(check_log(expected_vote_responses, 3))


class UnusedLinkFailTest(BasicTest):
    """
    Channel from participant 1 to participant 2 fails at the beginning and didn't recover.
    Since the channel is not used under normal circumstances, the system should commit.
    """
    @classmethod
    def setUpClass(cls):
        if os.path.exists('logs'):
            shutil.rmtree('logs')
        os.mkdir('logs')

        logging.basicConfig(level=logging.ERROR, format='%(asctime)s, %(levelname)s: %(message)s',
                            datefmt="%Y-%m-%d %H:%M:%S")

        cls.master = create_node(node_id=0,
                                 vote_responses={0: VoteResponse(vote=1, delay=0)},
                                 tasks=[SendVoteRequest(vote_id=0, time_to_execute=1)],
                                 node_cls=MasterNode)

        cls.participant1 = create_node(node_id=1,
                                       vote_responses={0: VoteResponse(vote=1, delay=0.5)},
                                       tasks=[],
                                       node_cls=Node)
        cls.participant2 = create_node(node_id=2,
                                       vote_responses={0: VoteResponse(vote=1, delay=0.5)},
                                       tasks=[],
                                       node_cls=Node)

        link_failure(from_node=cls.participant1, to_node=cls.participant2, start_time=0, end_time=4)
        time.sleep(3)

    def test_logs(self):
        expected_vote_responses = {0: 'commit'}
        self.assertTrue(check_log(expected_vote_responses, 3))


class LinkFailAfterVoteRequestTest(BasicTest):
    """
    Channel from master to participant 1 fails after sending the vote request and didn't recover afterwards.
    Participant 1 should use termination protocol after timeout and commit
    """
    @classmethod
    def setUpClass(cls):
        if os.path.exists('logs'):
            shutil.rmtree('logs')
        os.mkdir('logs')

        logging.basicConfig(level=logging.ERROR, format='%(asctime)s, %(levelname)s: %(message)s',
                            datefmt="%Y-%m-%d %H:%M:%S")

        cls.master = create_node(node_id=0,
                                 vote_responses={0: VoteResponse(vote=1, delay=0)},
                                 tasks=[SendVoteRequest(vote_id=0, time_to_execute=1)],
                                 node_cls=MasterNode)

        cls.participant1 = create_node(node_id=1,
                                       vote_responses={0: VoteResponse(vote=1, delay=0.5)},
                                       tasks=[],
                                       node_cls=Node)
        cls.participant2 = create_node(node_id=2,
                                       vote_responses={0: VoteResponse(vote=1, delay=0.5)},
                                       tasks=[],
                                       node_cls=Node)

        link_failure(from_node=cls.master, to_node=cls.participant1, start_time=1.2, end_time=4)
        time.sleep(4)

    def test_logs(self):
        expected_vote_responses = {0: 'commit'}
        self.assertTrue(check_log(expected_vote_responses, 3))


class NetworkPartitionAfterVoteRequestTest(BasicTest):
    """
    Network partition occurs after the vote requests and cut the graph into 2 groups,
    [master and participant 1] and [participant 2] and recover after decision was made
    Should abort the transaction and use termination protocol after recovery
    """
    @classmethod
    def setUpClass(cls):
        if os.path.exists('logs'):
            shutil.rmtree('logs')
        os.mkdir('logs')

        logging.basicConfig(level=logging.ERROR, format='%(asctime)s, %(levelname)s: %(message)s',
                            datefmt="%Y-%m-%d %H:%M:%S")

        cls.master = create_node(node_id=0,
                                 vote_responses={0: VoteResponse(vote=1, delay=0)},
                                 tasks=[SendVoteRequest(vote_id=0, time_to_execute=1)],
                                 node_cls=MasterNode)

        cls.participant1 = create_node(node_id=1,
                                       vote_responses={0: VoteResponse(vote=1, delay=0.5)},
                                       tasks=[],
                                       node_cls=Node)
        cls.participant2 = create_node(node_id=2,
                                       vote_responses={0: VoteResponse(vote=1, delay=0.5)},
                                       tasks=[],
                                       node_cls=Node)

        network_partition([cls.master, cls.participant1], [cls.participant2], 1.5, 3)
        time.sleep(6)

    def test_logs(self):
        expected_vote_responses = {0: 'abort'}
        self.assertTrue(check_log(expected_vote_responses, 3))


if __name__ == '__main__':
    unittest.main()
