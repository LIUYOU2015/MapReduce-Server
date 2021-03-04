# MapReduce-Server

A single machine, multi-process, multi-threaded MapReduce server in Python

Master module:
- listens for MapReduce jobs
- manages the jobs
- distributes work amongst the workers using TCP
- handles faults

Worker module: 
- register themselves with the master
- await commands, performing map, reduce or grouping tasks based on instructions given by the master
- send heartbeats to Maser using UDP
