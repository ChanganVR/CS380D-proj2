# CS380D-proj2
In this project, we present an abstract implementation of 2PC, extended with failure handling. Nodes are implemented as thread objects, 
and messages are transfered in queue objects. We simulate the messenge delays by assuming an internal clock for each node.

Project Spec: https://docs.google.com/document/d/167ZNK8qxkkPh6bU4sqloyBiIub3WGYpdjwu2_6Z-jKA/edit?fbclid=IwAR0wo1P5XRtxDsEQiKOjyseEtyrIzc_z1EGeczANBc8mMr93db7GyZdPWzg

### Name, UT EIDs, UTCS IDs
Dian Chen, dc44632, dchen  
Ruei-Bang Chen, rc46658, rbchen  
Changan Chen, cc68838, changan


### Prerequisites
```
python (>=3.7)
```

### Implementation
We implemented the termination protocol and the loggings to handle node crash failures, link failures, and network partitions.

* `algo/nodes.py` contains the code for the participant and coordinator logic for 2PC, including part of the logging and the termination protocol logic.
* `algo/messages.py` implements messages, including `VoteReq`, `Vote`, `DecisionReq`, `Commit` and `Abort`.
* `algo/tasks.py` implements nodes' tasks such as node crashes and recovery.
* `tests` contains our unittest code. There are 8 testing scenarios, 5 of which test for node crash failures, 1 tests for link failures, and 1 test for network partition.

### Run Tests
To run the tests, run `python -m tests -v`.
