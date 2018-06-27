package dispatch

import (
	"github.com/rcrowley/go-metrics"
)

//Dispatcher - Structure for scheduling work to be done
type Dispatcher struct {
	JobQueue   chan Job
	WorkerPool chan chan Job
	maxWorkers int

	registry       metrics.Registry
	jobsDispatched metrics.Counter
}

func NewDispatcher(jobQueue chan Job, maxWorkers int) *Dispatcher {
	d := &Dispatcher{}

	d.JobQueue = jobQueue
	d.WorkerPool = make(chan chan Job, maxWorkers)
	d.maxWorkers = maxWorkers

	d.registry = metrics.NewRegistry()
	d.jobsDispatched = metrics.NewCounter()
	d.registry.Register("jobs.dispatched", d.jobsDispatched)

	return d
}

// Statistics - return current statistics for this dispatcher
func (d *Dispatcher) Statistics() (metrics.Registry, error) {
	return d.registry, nil
}

// Run - Start the Dispatcher threads and get ready to dispatch messages
// as they come in.
func (d *Dispatcher) Run() {
	// starting n number of workers
	for i := 0; i < d.maxWorkers; i++ {
		worker := NewWorker(d.WorkerPool)
		worker.Start()
	}

	go d.dispatch()
}

func (d *Dispatcher) dispatch() {
	for {
		select {
		case job := <-d.JobQueue:

			// a job request has been received
			go func(job Job) {
				// try to obtain a worker job channel that is available.
				// this will block until a worker is idle
				jobChannel := <-d.WorkerPool

				d.jobsDispatched.Inc(1)

				// dispatch the job to the worker job channel
				jobChannel <- job
			}(job)
		}
	}
}
