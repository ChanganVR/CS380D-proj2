# CS380D-proj1
In this project, we build a crude distributed banking system, 
where nodes can join the cluster and transfer money between each other.
We also implement the Chandy-Lamport algorithm to produce consistent global snapshot of all nodes.


Project Spec: https://github.com/vijay03/cs380d-s20/blob/master/projects/project1.pdf

### Name, UT EIDs, UTCS IDs
Dian Chen, dc44632, dchen  
Ruei-Bang Chen, rc46658, rbchen  
Changan Chen, cc68838, changan


### Prerequisites
```
python (>=3.7)
```

### Usage
This project needs no Makefile or compilation. 
To run the code, run the following command under the root directory.
```
./master <commands_file.txt>
```


### Protocol and Implementation
This distributed banking system is implemented based on multi-thread, where each node is one 
single thread and the communication is asynchronous. 
The codes are contained in three files: \_\_main\_\_.py, nodes.py and messages.py.

* \_\_main\_\_.py contains the code for the interface, which interprets commands 
in the text file and sends them to the master node.
* nodes.py implements a base class of node and two subclasses of master and observer.
  * Ordinary nodes check the incoming channel from master and observer and execute the received message.
  * Master node waits to be called by the interface and pass messages to corresponding nodes.
It also wait for the Ack message from other nodes to make sure messages are delivered and executed.
  * Observer node also collects states from ordinary nodes.
* messages.py implements different types of messages with each being a class. Message object is created in master node and passed to the incoming queue
of nodes. During execution, messages use the internal variables of nodes to perform the computation.


### Tests
In addition to the provided test cases , we also included ... new test cases in tests folder.
* case1.txt: test basic framework
* case[2,3,4].txt: provided test cases
* case5.txt: test three-node case
* case6.txt: test two rounds of taking snapshot
* case7.txt: test randomness of receive function and snapshot token broadcasting behavior, 4 possible outcomes
