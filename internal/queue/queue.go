package queue

import (
	"fmt"
	"sync"
	"time"
)

type Job struct {
	ID         string
	Data       interface{}
	DequeuedAt time.Time
}

type Queue struct {
	// I am making the purposeful decision to use slices for the
	// time being instead of linked lists, as we expect queues to be relatively small
	// and so dequeuing will be basically constant time.
	// The advantages in terms of maintainability and
	// memory is probably better here. This may change in the future.
	items    []interface{}
	mu       sync.Mutex
	inFlight map[string]*Job // jobID -> Job
	nextID   int             // Whenever we make a new job, we need an id, which is this
}

func NewQueue() *Queue {
	return &Queue{
		items:    make([]interface{}, 0),
		inFlight: make(map[string]*Job),
		nextID:   1,
	}
}

func (q *Queue) Enqueue(item interface{}) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.items = append(q.items, item)
}

func (q *Queue) DequeueWithId() (string, interface{}, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.items) == 0 {
		return "", nil, false
	}

	item := q.items[0]
	// TODO: This is sort of a memory leak? i think items[0] will never go away,
	// even if we never reference it later, as q.items still will reference
	// the rest of the array. We can later make a resize function or similar that
	// gets rid of the first x amount of items periodically to make this amortized
	// o(1) or similar
	q.items = q.items[1:]

	// get the next jobId
	// TODO: this is a security leak since we know the order of the tasks except not really since this
	// task queue should only be used by the people that already know the order of the tasks
	jobID := fmt.Sprintf("%d", q.nextID)
	q.nextID++

	q.inFlight[jobID] = &Job{
		ID:         jobID,
		Data:       item,
		DequeuedAt: time.Now(),
	}

	return jobID, item, true
}

// asked to acknowledge that a job is done
func (q *Queue) Ack(jobId string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if _, exists := q.inFlight[jobId]; !exists {
		return fmt.Errorf("there is no job with id %s that is inflight", jobId)
	}

	delete(q.inFlight, jobId)
	return nil
}

// requeues jobs that haven't been acked within timeout
func (q *Queue) CheckTimeouts(timeout time.Duration) []string {
	q.mu.Lock()
	defer q.mu.Unlock()

	var requeued []string
	now := time.Now()

	for jobID, job := range q.inFlight {
		if now.Sub(job.DequeuedAt) > timeout {
			// Timeout expired, requeue the job
			q.items = append(q.items, job.Data)
			delete(q.inFlight, jobID)
			requeued = append(requeued, jobID)
		}
	}

	return requeued
}
