# Andrew's Implementation of Labs of MIT 6.824

## [MIT 6.824(2015): Distributed Systems](http://nil.csail.mit.edu/6.824/2015/schedule.html)
6.824 is a core graduate subject with lectures, labs, an optional project, a mid-term exam, and a final exam. 6.824 is 12 units.

## Progress
1. Lab 1: MapReduce (Done)
  * Worker failure
2. Lab 2 : Primary/Backup Key/Value Service 
  * Part-A (Done)
    * View server (zookeeper-like component)
  * Part-B (Done) `A bug in ConcurrentSameUnreliable`
    * PRC at-most-one semantics (dedup)
    * replication from primary to backup
    * mutex of concurrency request
    * client requests of expired view
3. Lab 3: Paxos-based Key/Value Service
  * Part-A Paxos Service (Done)
  * Part-B: Paxos-based Key/Value Server (Done)
4. Lab 4: Sharded Key/Value Service (Undone)
5. Lab 5: Persistence (Undone)

## Deploy
1. Install golang, and setup golang environment variables and directories. [Click here](https://golang.org/doc/install) to learn it.
2. Setup the labs.

```
cd $GOPATH
git clone https://github.com/andrewlee302/MIT-6.824.git
cd MIT-6.824
export GOPATH=$GOPATH:$(pwd)
```
You can run the test cases in the labs.


