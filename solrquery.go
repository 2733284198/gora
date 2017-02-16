package gora

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/golang/glog"
)

// SolrQuery represents a SolrJob that can be submitted to a pool.
// It is a bog standard query representation.
type SolrQuery struct {
	Rows     int
	Start    int
	Query    string
	Facet    *string // this has to be a raw json facet query!
	Filter   *string // this has to be a raw json filter query!
	Sort     *string // field order
	Params   map[string]interface{}
	handler  string
	resultCh chan *SolrResponse
}

func NewSolrQuery(q string, s, r int, filter, facet, sort *string, handler string) *SolrQuery {
	params := make(map[string]interface{})
	params["wt"] = "json"

	return &SolrQuery{
		Query:    q,
		Start:    s,
		Rows:     r,
		Facet:    facet,
		Filter:   filter,
		Sort:     sort,
		Params:   params,
		handler:  handler,
		resultCh: make(chan *SolrResponse, 1),
	}
}

func NewSolrSpatialQuery(q, stype, sfield string, lat, lon, d float64, s, r int, filter, facet, sort *string, handler string) *SolrQuery {
	params := make(map[string]interface{})
	params["wt"] = "json"
	params["fq"] = fmt.Sprintf("{!%s sfield=%s}", stype, sfield)
	params["pt"] = fmt.Sprintf("%f,%f", lat, lon)
	params["d"] = fmt.Sprintf("%f", d)

	return &SolrQuery{
		Query:    q,
		Start:    s,
		Rows:     r,
		Facet:    facet,
		Filter:   filter,
		Sort:     sort,
		Params:   params,
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

func (q *SolrQuery) GetRows() int {
	return q.Rows
}

func (q *SolrQuery) GetStart() int {
	return q.Start
}

// Bytes will manually construct the JSON query that will be sent
// to the Solr server. We do this to embed additional JSON fields.
// This function executes a tad faster than using the json.Marshal function.
func (q *SolrQuery) Bytes() []byte {
	query := make(map[string]interface{})

	query["query"] = q.Query

	if q.Sort != nil {
		query["sort"] = *q.Sort
	}

	if q.Filter != nil {
		query["filter"] = *q.Filter
	}

	if q.Facet != nil {
		query["facet"] = *q.Facet
	}

	q.Params["start"] = q.Start
	q.Params["rows"] = q.Rows

	query["params"] = q.Params

	b, err := json.Marshal(query)
	if err != nil {
		glog.Error(err)
	}

	return b
}

// SolrUpdateQuery represents a query that will update or create a new Solr document
type SolrUpdateQuery struct {
	Documents map[string]interface{}
	handler   string
	resultCh  chan *SolrResponse
}

func NewSolrUpdateQuery(document map[string]interface{}) *SolrUpdateQuery {
	return &SolrUpdateQuery{
		Documents: document,
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

func (q *SolrUpdateQuery) GetRows() int {
	return 0
}

func (q *SolrUpdateQuery) GetStart() int {
	return 0
}

func (q *SolrUpdateQuery) Bytes() []byte {
	b, _ := json.Marshal(q.Documents)
	buffer := bytes.NewBufferString(fmt.Sprintf("{\"add\":{\"doc\":%s}, \"commit\": {}}", b))

	return buffer.Bytes()
}

// SolrBatchUpdateQuery represents a query that will update or create several Solr documents
type SolrBatchUpdateQuery struct {
	Documents    []map[string]interface{}
	CommitWithin int
	handler      string
	resultCh     chan *SolrResponse
}

func NewSolrBatchUpdateQuery(documents []map[string]interface{}) *SolrBatchUpdateQuery {
	return &SolrBatchUpdateQuery{
		Documents:    documents,
		handler:      "update",
		CommitWithin: 0,
		resultCh:     make(chan *SolrResponse, 1),
	}
}

func NewSolrBatchUpdateQueryCommitWithin(timeInMillis int, documents []map[string]interface{}) *SolrBatchUpdateQuery {
	q := NewSolrBatchUpdateQuery(documents)
	q.CommitWithin = timeInMillis
	return q
}

func (q *SolrBatchUpdateQuery) Handler() string {
	return q.handler
}

func (q *SolrBatchUpdateQuery) ResultCh() chan *SolrResponse {
	return q.resultCh
}

func (q *SolrBatchUpdateQuery) Wait() *SolrResponse {
	return <-q.ResultCh()
}

func (q *SolrBatchUpdateQuery) GetRows() int {
	return 0
}

func (q *SolrBatchUpdateQuery) GetStart() int {
	return 0
}

func (q *SolrBatchUpdateQuery) Bytes() []byte {
	docs := make([]string, len(q.Documents))
	for i, d := range q.Documents {
		b, _ := json.Marshal(d)
		if q.CommitWithin > 0 {
			docs[i] = fmt.Sprintf("\"add\":{\"doc\":%s,\"commitWithin\":%d}", b, q.CommitWithin)
		} else {
			docs[i] = fmt.Sprintf("\"add\":{\"doc\":%s}", b)
		}
	}

	buf := strings.Join(docs, ",")
	var buffer *bytes.Buffer
	if q.CommitWithin > 0 {
		buffer = bytes.NewBufferString(fmt.Sprintf("{%s}", buf))
	} else {
		buffer = bytes.NewBufferString(fmt.Sprintf("{%s, \"commit\": {}}", buf))
	}

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

func (q *SolrDeleteQuery) GetRows() int {
	return 0
}

func (q *SolrDeleteQuery) GetStart() int {
	return 0
}

func (q *SolrDeleteQuery) Bytes() []byte {
	query := fmt.Sprintf("{\"delete\":{\"query\":%s}, \"commit\": {}}", strconv.Quote(q.match))
	buffer := bytes.NewBufferString(query)

	return buffer.Bytes()
}

// SolrBatchDeleteQuery represents a query that will remove documents
type SolrBatchDeleteQuery struct {
	Ids      []string
	handler  string
	resultCh chan *SolrResponse
}

func NewSolrBatchDeleteQuery(ids []string) *SolrBatchDeleteQuery {
	return &SolrBatchDeleteQuery{
		handler:  "update",
		Ids:      ids,
		resultCh: make(chan *SolrResponse, 1),
	}
}

func (q *SolrBatchDeleteQuery) Handler() string {
	return q.handler
}

func (q *SolrBatchDeleteQuery) ResultCh() chan *SolrResponse {
	return q.resultCh
}

func (q *SolrBatchDeleteQuery) Wait() *SolrResponse {
	return <-q.ResultCh()
}

func (q *SolrBatchDeleteQuery) GetRows() int {
	return 0
}

func (q *SolrBatchDeleteQuery) GetStart() int {
	return 0
}

func (q *SolrBatchDeleteQuery) Bytes() []byte {
	b, _ := json.Marshal(q.Ids)
	query := fmt.Sprintf(`"delete":%s`, b)

	buffer := bytes.NewBufferString(fmt.Sprintf(`{%s, "commit": {}}`, query))

	return buffer.Bytes()
}
