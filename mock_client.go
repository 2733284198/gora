package gora

import "strconv"

type MockSolrClient struct {
	Timeout      bool
	ConnectCount int
	CloseCh      chan struct{}
	Response     *SolrResponse
}

func (c *MockSolrClient) Execute(s SolrJob) (*SolrResponse, bool) {
	c.Response.Status, _ = strconv.Atoi(string(s.Bytes()))

	// Client will keep iterating through the result set until
	// no additional documents are returned.
	if c.Response != nil && c.Response.Response != nil {
		if s.GetStart() > len(c.Response.Response.Docs) {
			res := &SolrResponse{
				Response: &DocumentCollection{},
				Status:   c.Response.Status,
			}

			return res, c.Timeout
		}
	}

	return c.Response, c.Timeout
}

func (c *MockSolrClient) TestConnection() bool {
	c.ConnectCount += 1

	if c.ConnectCount > 3 {
		c.CloseCh <- struct{}{}
	}

	return c.Timeout
}

func MockClientConstructor(hostUrl string, core string, ch chan struct{}) SolrClient {
	return &MockSolrClient{
		Timeout:  false,
		CloseCh:  ch,
		Response: &SolrResponse{},
	}
}

type MockSolrJob struct {
	payload []byte
	rCh     chan *SolrResponse
}

func NewMockSolrJob(payload []byte) *MockSolrJob {
	return &MockSolrJob{
		payload: payload,
		rCh:     make(chan *SolrResponse, 1),
	}
}

func (j *MockSolrJob) Handler() string {
	return "mochHandler"
}

func (j *MockSolrJob) Bytes() []byte {
	return j.payload
}

func (j *MockSolrJob) ResultCh() chan *SolrResponse {
	return j.rCh
}

func (j *MockSolrJob) Wait() *SolrResponse {
	return <-j.ResultCh()
}

func (j *MockSolrJob) GetRows() int {
	return 0
}

func (j *MockSolrJob) GetStart() int {
	return 0
}
