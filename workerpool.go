package gora

import (
	"errors"
	"sync"
	"time"
)

var (
	ErrPoolNotRunning  = errors.New("Pool is not running.")
	ErrPoolRunning     = errors.New("Pool is running.")
	ErrNoActiveWorkers = errors.New("Pool has no active workers.")
	ErrTimeout         = errors.New("Host Timeout")
)

type worker struct {
	parent     *Pool
	client     SolrClient
	jobCh      <-chan SolrJob
	dieCh      <-chan struct{}
	sigDeathCh chan<- struct{}
}

var (
	solrClientConstructor = NewHttpSolrClient
)

func newWorker(parent *Pool, jCh <-chan SolrJob, dCh <-chan struct{}, sDeathCh chan<- struct{}, hostUrl string, core string) *worker {
	return &worker{
		parent:     parent,
		jobCh:      jCh,
		dieCh:      dCh,
		sigDeathCh: sDeathCh,
		client:     solrClientConstructor(hostUrl, core),
	}
}

func (w *worker) work() {
	var resp *SolrResponse
	var hostReachable <-chan time.Time

	timeout := false
	jCh := w.jobCh
	hostReachable = nil

	for {

		if timeout {
			jCh = nil
			hostReachable = time.After(time.Second * 10)
		} else {
			jCh = w.jobCh
			hostReachable = nil
		}

		select {
		case <-w.dieCh:
			w.sigDeathCh <- struct{}{}
			return

		case job := <-jCh:
			resp, timeout = w.client.Execute(job)
			if timeout {
				resp.Error = ErrTimeout
			}
			job.ResultCh() <- resp

		case <-hostReachable:
			timeout = w.hostOffline()
		}
	}
}

func (w *worker) hostOffline() bool {
	return w.client.TestConnection()
}

// Pool holds all the data about our worker pool
type Pool struct {
	// nWorkersPerHost specifies the number of workers per each Solr server
	nWorkersPerHost int

	// hostUrls specifies the URL of each Solr server
	hostUrls []string

	// bufferLen specifies the number of jobs that can be in queue without blocking
	bufferLen int

	// solrCore is the core to work with on the Solr server
	solrCore string

	jobCh chan SolrJob
	dieCh chan struct{}
	lock  sync.Mutex
}

// NewPool will create a Pool structure with an array of Solr servers.
// It will create numWorkers per Solr server, and allow bufLen jobs
// before the workers start blocking.
func NewPool(core string, hostUrls []string, numWorkers int, bufLen int) *Pool {
	p := &Pool{}
	p.solrCore = core
	p.hostUrls = hostUrls
	p.nWorkersPerHost = numWorkers
	p.bufferLen = bufLen

	return p
}

// Run will create a goroutine for each worker, and a master goroutine
// to control those workers. A channel to close down the pool will be
// returned to the caller.
func (p *Pool) Run() (<-chan struct{}, error) {
	p.lock.Lock()
	defer p.lock.Unlock()

	if p.jobCh != nil {
		return nil, ErrPoolRunning
	}

	nWorkers := p.nWorkersPerHost * len(p.hostUrls)

	p.jobCh = make(chan SolrJob, p.bufferLen)
	p.dieCh = make(chan struct{}, 1)

	sigPoolDeathCh := make(chan struct{}, 1)
	collectWorkersDeathCh := make(chan struct{}, nWorkers)
	dieChs := make([]chan struct{}, 0, nWorkers)

	for _, host := range p.hostUrls {
		for i := 0; i < p.nWorkersPerHost; i++ {
			dieCh := make(chan struct{}, 1)
			dieChs = append(dieChs, dieCh)

			w := newWorker(p, p.jobCh, dieCh, collectWorkersDeathCh, host, p.solrCore)
			go w.work()
		}
	}

	go master(p.dieCh, dieChs, sigPoolDeathCh, collectWorkersDeathCh)

	return sigPoolDeathCh, nil
}

// Submit will enter a job into the queue for the worker pool
func (p *Pool) Submit(s SolrJob) error {
	p.lock.Lock()
	defer p.lock.Unlock()

	if p.jobCh == nil {
		return ErrPoolNotRunning
	}

	p.jobCh <- s
	return nil
}

// Stop will gracefully stop processing jobs and shutdown the workers
func (p *Pool) Stop() {
	p.lock.Lock()
	defer p.lock.Unlock()

	if p.jobCh == nil {
		return
	}

	p.dieCh <- struct{}{}
	p.jobCh = nil
}

func master(dieCh chan struct{}, dieChs []chan struct{}, sigPoolDeathCh chan<- struct{}, collectWorkersDeathCh <-chan struct{}) {
	workersLeft := len(dieChs)

	for {
		if workersLeft == 0 {
			sigPoolDeathCh <- struct{}{}
			return
		}

		select {
		case <-dieCh:
			for i, _ := range dieChs {
				dieChs[i] <- struct{}{}
			}

		case <-collectWorkersDeathCh:
			workersLeft--
		}
	}
}
