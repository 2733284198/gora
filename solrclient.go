package gora

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
)

// SolrClient is the interface that workers in the workerpool use
// to get SolrResponses for SolrJobs.
//
// Execute(SolrJob) should always return a SolrRespose. If there was
// an error executing the SolrJob, the error should be put in the
// SolrResponse, and the retry flag should be set depending on whether
// this error is recoverable or not.
//
// TestConnection() will be called by the worker if an error has
// previously occurred. The worker will not accept any new jobs
// until the SolrClient has a vsalid connection.
type SolrClient interface {
	Execute(SolrJob) (*SolrResponse, bool)
	TestConnection() bool
}

type HttpSolrClient struct {
	// Host specifies the URL of the Solr Server
	Host string

	// Core specifies the Solr core to work with
	Core string

	client *http.Client
}

// NewHttpSolrClient creates a SolrClient with an http.Client connection
func NewHttpSolrClient(host, core string) SolrClient {
	transport := http.Transport{
		MaxIdleConnsPerHost: 2,
	}

	httpClient := http.Client{
		Transport: &transport,
	}

	client := HttpSolrClient{
		Host:   host,
		Core:   core,
		client: &httpClient,
	}

	return &client
}

// TestConnection will issue an empty query to the Solr server.
// As long as we don't get an error, we know that the Solr server
// received the query, and that this connection is valid.
func (c *HttpSolrClient) TestConnection() bool {
	_, err := c.execQuery("", []byte(""))
	return err == nil
}

// Execute will send the given job to the Solr server and wait for
// a response. If an error is received, the retry value will be determined
// and the error will be placed in an empty SolrResponse.
func (c *HttpSolrClient) Execute(job SolrJob) (*SolrResponse, bool) {
	handler := job.Handler()
	jobBytes := job.Bytes()

	emptyResponse := &SolrResponse{}
	byteResponse, err := c.execQuery(handler, jobBytes)
	if err != nil {
		emptyResponse.Error = err
		return emptyResponse, c.temporaryError(err)
	}

	solrResponse, err := SolrResponseFromHTTPResponse(byteResponse)
	if err != nil {
		emptyResponse.Error = err
		return emptyResponse, false
	}

	return solrResponse, false
}

// temporaryError tries to determine whether this error is recoverable.
// Most errors will be type *url.Error, and we can ask that error
// whether it is temporary, or timeout related.
func (c *HttpSolrClient) temporaryError(err error) bool {
	if err == nil {
		return false
	}

	// Could have an internal buffer problem, we should try again
	if err == bytes.ErrTooLarge {
		return true
	}

	// If we get a totally unknown error, send it back to the caller
	urlError, ok := err.(*url.Error)
	if !ok {
		return false
	}

	return urlError.Temporary() || urlError.Timeout()
}

// execQuery creates the full URL and posts an array of bytes to that url.
func (c *HttpSolrClient) execQuery(handler string, json []byte) ([]byte, error) {
	url := fmt.Sprintf("%s/solr/%s/%s", c.Host, c.Core, handler)
	r, err := c.client.Post(url, "application/json", bytes.NewReader(json))
	if err != nil {
		return nil, err
	}

	defer r.Body.Close()

	// read the response and check
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}

	return body, nil
}
