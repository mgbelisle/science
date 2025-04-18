# paxos

This is a single decree paxos implementation written from scratch in Go. For an explanation of the paxos algorithm see [Wikipedia](https://en.wikipedia.org/wiki/Paxos_(computer_science)) or [Leslie Lamport's original whitepaper](https://www.microsoft.com/en-us/research/uploads/prod/2016/12/paxos-simple-Copy.pdf).

[paxos-demo](../paxos-demo/main.go) uses this package to solve a toy problem based on the Mission Impossible series.

[paxos-http](../paxos-http/main.go) uses this package to create a fault tolerant distributed key value store served over HTTP.

Paxos was chosen over raft for this sample project because paxos is the OG solution to the problem of distributed fault tolerance. For production code, raft is probably a better fit especially compared to single decree paxos.