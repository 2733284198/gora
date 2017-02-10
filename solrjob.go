package gora

// SolrJob is the interface that a SolrClient needs in order
// to get enough information to connnect to a Solr server.
type SolrJob interface {
	// Handler returns the method used to run the job. (select, update)
	Handler() string

	// Bytes returns the array of bytes representing the JSON query
	Bytes() []byte

	// ResultCh return the channel that the job's response will be sent to
	ResultCh() chan *SolrResponse

	// Wait is a convinience method that allows a one line function
	// to wait for a SolrResponse
	Wait() *SolrResponse

	GetRows() int
	GetStart() int
}
