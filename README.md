# gora

A simple, bare-bones solr client for Go.

This is a driver in-the-making, and should be approached as such. We appreciate any feedback via issues. Please be harsh and do break our code.

Key features:
 * Uses HTTP connections.
 * Support for facets and standard queries.
 * Support for multiple hosts (SolrCloud) with recovery.

### Installation
	
```
go get github.com/wirelessregistry/gora
```
  
### Usage

Gora consists of two layers:
 * A low-level solr client. (see solrclient.go)
 * A connection-management pool. (see workerpool.go)
	
__Low-level client__

This is synchronous client, that simply constructs an appropriate JSON post to a given node+core combo. Its input is a SolrJob interface, and it deserializes the resulting JSON as a SolrResponse struct. 

The SolrJob interface provides a Bytes() function, which the client uses to obtain the raw query to send to the Solr server. A Handler() function is also provided, giving the job control over how it is processed by Solr.

See solrclient_test.go for usage examples.

__Connection Pool__

The key inconvenience when using the aforementioned client is concurrent processing and connection management. In detail:

One can launch multiple goroutines (e.g. in the master-slave pattern) to execute queries concurrently. This approach works well when a process does not launch excessive numbers of goroutines. When this does not hold, the connection pool can be launched with a fixed number of running goroutines. In this case, a process submits a job to the pool and awaits the query completion.

A pool can be started with a set of solr hosts (e.g. SolrCloud). In this case, equal number of goroutines will be dedicated to each host. Note that the sharding strategy is not taken into account when assigning jobs to routines. If a host becomes unavailable, the corresponding routines will take themselves offline and wait until the host is again available before taking on new jobs.

