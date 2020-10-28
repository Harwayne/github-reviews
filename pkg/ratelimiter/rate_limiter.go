package ratelimiter

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/google/go-github/github"
	"golang.org/x/time/rate"
)

type RateLimiter struct {
	limiter   *rate.Limiter
	workItems chan *workItem
	workers   []*worker
}

func New(parallelWorkers int) *RateLimiter {
	rl := &RateLimiter{
		limiter:   rate.NewLimiter(10, 10),
		workItems: make(chan *workItem),
		workers:   make([]*worker, parallelWorkers),
	}
	for i := 0; i < len(rl.workers); i++ {
		rl.workers[i] = &worker{
			limiter:   rl.limiter,
			workItems: rl.workItems,
		}
		go rl.workers[i].Start(context.Background())
	}
	return rl
}

func (r *RateLimiter) DoWork(f func() (interface{}, *github.Response)) interface{} {
	output := make(chan *workItemOutput)
	wi := &workItem{
		f:      f,
		output: output,
	}
	r.workItems <- wi
	wiOut := <-output
	r.updateRateLimits(wiOut.response)
	return wiOut.value
}

func (r *RateLimiter) updateRateLimits(ghResponse *github.Response) {
	if ghResponse == nil {
		return
	}
	tokensPerSecond := rate.Limit(1000)
	if ghResponse.Rate.Remaining < 500 {
		tokensPerSecond = calculateTokensPerSecond(ghResponse.Rate)
		log.Printf("Setting the limit to %v\n", tokensPerSecond)
	}
	r.limiter.SetLimit(tokensPerSecond)
}

func calculateTokensPerSecond(ghRate github.Rate) rate.Limit {
	timeLeft := ghRate.Reset.Sub(time.Now())
	log.Printf("Remaining %v, Timeleft %s, Reset %s, Now %s\n", ghRate.Remaining, timeLeft, ghRate.Reset, time.Now())
	requestsPerSecond := float64(ghRate.Remaining) / timeLeft.Seconds()
	// Add wiggle room, in case multiple processes are using quota.
	requestsPerSecond /= 5.0
	return rate.Limit(requestsPerSecond)
}

type worker struct {
	limiter   *rate.Limiter
	workItems <-chan *workItem
}

func (w *worker) Start(_ context.Context) {
	for {
		w.doWork()
	}
}

func (w *worker) doWork() {
	err := w.limiter.Wait(context.Background())
	if err != nil {
		panic(fmt.Errorf("waiting on limiter: %w", err))
	}

	select {
	case wi := <-w.workItems:
		w.completeWorkItem(wi)
	case <-time.After(time.Second):
		// Skip this token.
	}
}

func (w *worker) completeWorkItem(wi *workItem) {
	v, r := wi.f()
	out := &workItemOutput{
		value:    v,
		response: r,
	}
	wi.output <- out
}

type workItem struct {
	f      func() (interface{}, *github.Response)
	output chan<- *workItemOutput
}

type workItemOutput struct {
	value    interface{}
	response *github.Response
}
