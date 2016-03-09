package gora

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
)

// SolrQuery represents a SolrJob that can be submitted to a pool.
// It is a bog standard query representation.
type SolrQuery struct {
	Rows     int
	Start    int
	Query    string
	Facet    *string // this has to be a raw json facet query!
	Filter   *string // this has to be a raw json filter query!
	handler  string
	resultCh chan *SolrResponse
}

func NewSolrQuery(q string, s int, r int, filter *string, facet *string, handler string) *SolrQuery {
	return &SolrQuery{
		Query:    q,
		Start:    s,
		Rows:     r,
		Facet:    facet,
		Filter:   filter,
		handler:  handler,
		resultCh: make(chan *SolrResponse, 1),
	}
}

func escape(s string) string {
	return "\"" + s + "\""
}

func (q *SolrQuery) Handler() string {
	return q.handler
}

func (q *SolrQuery) ResultCh() chan *SolrResponse {
	return q.resultCh
}

func (q *SolrQuery) Wait() *SolrResponse {
	return <-q.ResultCh()
}

// Bytes will manually construct the JSON query that will be sent
// to the Solr server. We do this to embed additional JSON fields.
// This function executes a tad faster than using the json.Marshal function.
func (q *SolrQuery) Bytes() []byte {
	buffer := bytes.NewBufferString("{")
	buffer.Grow(256)
	buffer.WriteString(escape("query"))
	buffer.WriteString(":")
	buffer.WriteString(escape(q.Query))
	buffer.WriteString(",")
	if q.Filter != nil {
		buffer.WriteString(escape("filter"))
		buffer.WriteString(":")
		buffer.WriteString(*q.Filter)
		buffer.WriteString(",")
	}
	if q.Facet != nil {
		buffer.WriteString(escape("facet"))
		buffer.WriteString(":")
		buffer.WriteString(*q.Facet)
		buffer.WriteString(",")
	}
	buffer.WriteString(escape("params"))
	buffer.WriteString(":")
	buffer.WriteString("{")
	buffer.WriteString(escape("wt"))
	buffer.WriteString(":")
	buffer.WriteString(escape("json"))
	buffer.WriteString(",")
	buffer.WriteString(escape("start"))
	buffer.WriteString(":")
	buffer.WriteString(strconv.Itoa(q.Start))
	buffer.WriteString(",")
	buffer.WriteString(escape("rows"))
	buffer.WriteString(":")
	buffer.WriteString(strconv.Itoa(q.Rows))
	buffer.WriteString("}")
	buffer.WriteString("}")
	return buffer.Bytes()
}

// SolrUpdateQuery represents a query that will update or create a new Solr document
type SolrUpdateQuery struct {
	Documents map[string]interface{}
	handler   string
	resultCh  chan *SolrResponse
}

func NewSolrUpdateQuery(documents map[string]interface{}) *SolrUpdateQuery {
	return &SolrUpdateQuery{
		Documents: documents,
		handler:   "update",
		resultCh:  make(chan *SolrResponse, 1),
	}
}

func (q *SolrUpdateQuery) Handler() string {
	return q.handler
}

func (q *SolrUpdateQuery) ResultCh() chan *SolrResponse {
	return q.resultCh
}

func (q *SolrUpdateQuery) Wait() *SolrResponse {
	return <-q.ResultCh()
}

func (q *SolrUpdateQuery) Bytes() []byte {
	b, _ := json.Marshal(q.Documents)
	buffer := bytes.NewBufferString(fmt.Sprintf("{\"add\":{\"doc\":%s}, \"commit\": {}}", b))

	return buffer.Bytes()
}

// SolrDeleteQuery represents a query that will remove documents
type SolrDeleteQuery struct {
	handler  string
	match    string
	resultCh chan *SolrResponse
}

func NewSolrDeleteQuery(match string) *SolrDeleteQuery {
	return &SolrDeleteQuery{
		handler:  "update",
		match:    match,
		resultCh: make(chan *SolrResponse, 1),
	}
}

func (q *SolrDeleteQuery) Handler() string {
	return q.handler
}

func (q *SolrDeleteQuery) ResultCh() chan *SolrResponse {
	return q.resultCh
}

func (q *SolrDeleteQuery) Wait() *SolrResponse {
	return <-q.ResultCh()
}

func (q *SolrDeleteQuery) Bytes() []byte {
	buffer := bytes.NewBufferString(fmt.Sprintf("{\"delete\":{\"query\":\"%s\"}, \"commit\": {}}", q.match))

	return buffer.Bytes()
}
