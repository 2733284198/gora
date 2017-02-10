package gora

import (
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestPoolNoSubmission(t *testing.T) {
	ch := make(chan struct{}, 1)
	client := MockClientConstructor("0.0.0.0", "", ch)
	p := NewPool([]SolrClient{client}, 10, 1, 1)
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
	testPoolMock(t, 1, 0)
	testPoolMock(t, 10, 0)
	testPoolMock(t, 50, 1)
}

func testPoolMock(t *testing.T, nw int, bl int) {
	var wg sync.WaitGroup

	ch := make(chan struct{}, 1)
	client := MockClientConstructor("0.0.0.0", "", ch)
	p := NewPool([]SolrClient{client}, nw, bl, 1)
	sig, err := p.Run()
	if err != nil {
		t.Fatal("Unexpected error ", err)
	}

	for i := 0; i < 9999; i++ {
		job := NewMockSolrJob([]byte(strconv.Itoa(i)))

		go func(j SolrJob) {
			p.Submit(j)
		}(job)

		wg.Add(1)
		go func(j SolrJob, expected int) {
			j.Wait()
			wg.Done()
		}(job, i)
	}

	wg.Wait()
	p.Stop()
	<-sig
}

func TestPoolFaultyWorker(t *testing.T) {
	ch := make(chan struct{}, 1)
	client := &MockSolrClient{
		Timeout:  true,
		CloseCh:  ch,
		Response: &SolrResponse{},
	}

	p := NewPool([]SolrClient{client}, 1, 0, 1)
	sig, _ := p.Run()

	job := NewMockSolrJob([]byte("dead"))
	p.Submit(job)

	resp := job.Wait()
	if resp.Error != ErrTimeout {
		t.Errorf("Expected %v. Got %v.", ErrTimeout, resp.Error)
	}

	// Must allow wait time for 3 connection fails
	waitTime := time.After(time.Second * 5)
	select {
	case <-ch:
	case <-waitTime:
		t.Error("Got timeout waiting for mock client")
	}
	p.Stop()
	<-sig
}
