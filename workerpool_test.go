package gora

import (
	"strconv"
	"sync"
	"testing"
)

type mockSolrClient struct {
	timeout bool
}

func (c *mockSolrClient) Execute(s SolrJob) (*SolrResponse, bool) {
	res := &SolrResponse{}
	res.Status, _ = strconv.Atoi(string(s.Bytes()))

	return res, c.timeout
}

func faultyClientConstructor() func(hostUrl string, core string) SolrClient {
	return func(hostUrl string, core string) SolrClient {
		return &mockSolrClient{
			timeout: true,
		}
	}
}

func (c *mockSolrClient) TestConnection() bool {
	return c.timeout
}

func mockClientConstructor(hostUrl string, core string) SolrClient {
	return &mockSolrClient{}
}

type mockSolrJob struct {
	payload []byte
	rCh     chan *SolrResponse
}

func newMockSolrJob(payload []byte) *mockSolrJob {
	return &mockSolrJob{
		payload: payload,
		rCh:     make(chan *SolrResponse, 1),
	}
}

func (j *mockSolrJob) Handler() string {
	return "mochHandler"
}

func (j *mockSolrJob) Bytes() []byte {
	return j.payload
}

func (j *mockSolrJob) ResultCh() chan *SolrResponse {
	return j.rCh
}

func (j *mockSolrJob) Wait() *SolrResponse {
	return <-j.ResultCh()
}

func TestPoolNoSubmission(t *testing.T) {
	p := NewPool("", []string{"localhost:1111", "localhost:2222"}, 10, 1)
	err := p.Submit(nil)
	if err != ErrPoolNotRunning {
		t.Fatalf("Expected errPoolNotRunning")
	}

	sigCh, _ := p.Run()
	p.Stop()

	sigCh2, _ := p.Run()
	_, err = p.Run()
	if err != ErrPoolRunning {
		t.Errorf("Expected %v. Got %v.", ErrPoolRunning, err)
	}

	p.Stop()

	<-sigCh
	<-sigCh2
}

func TestPoolMock(t *testing.T) {
	solrClientConstructor = mockClientConstructor

	testPoolMock(t, 1, 0)
	testPoolMock(t, 10, 0)
	testPoolMock(t, 50, 1)
}

func testPoolMock(t *testing.T, nw int, bl int) {
	var wg sync.WaitGroup

	p := NewPool("", []string{"0.0.0.0", "1.1.1.1"}, nw, bl)
	sig, _ := p.Run()

	for i := 0; i < 9999; i++ {
		job := newMockSolrJob([]byte(strconv.Itoa(i)))

		go func(j SolrJob) {
			p.Submit(j)
		}(job)

		wg.Add(1)
		go func(j SolrJob) {
			res := <-j.ResultCh()
			expected, _ := strconv.Atoi(string(j.Bytes()))

			if res.Status != expected {
				t.Errorf("Expected %v. Got %v.", expected, res.Status)
			}
			wg.Done()
		}(job)
	}

	wg.Wait()
	p.Stop()
	<-sig
}

func TestPoolFaultyWorker(t *testing.T) {
	solrClientConstructor = faultyClientConstructor()

	p := NewPool("", []string{"0.0.0.0"}, 1, 0)
	sig, _ := p.Run()

	job := newMockSolrJob([]byte("dead"))
	p.Submit(job)

	resp := job.Wait()
	if resp.Error != ErrTimeout {
		t.Errorf("Expected %v. Got %v.", ErrTimeout, resp.Error)
	}

	p.Stop()
	<-sig
}
