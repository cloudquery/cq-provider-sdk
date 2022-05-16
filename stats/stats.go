package stats

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/elliotchance/orderedmap"
	"github.com/hashicorp/go-hclog"
	"github.com/segmentio/stats/v4"
)

type stat struct {
	Start    time.Time
	Duration time.Duration
}

type logHandler struct {
	logger            hclog.Logger
	trackedOperations *orderedmap.OrderedMap
	mu                sync.Mutex
}

type Options struct {
	tick    time.Duration
	handler stats.Handler
}

func NewClockWithObserve(name string, tags ...stats.Tag) *stats.Clock {
	// The default clock doesn't send a measurement on start (only on stop)
	// We want both on start AND stop, so we wrap the ClockAt method
	cl := stats.DefaultEngine.ClockAt(name, time.Now(), tags...)
	stats.DefaultEngine.Observe(name, time.Duration(0), tags...)
	return cl
}

func meta(name string, tags []stats.Tag) (string, bool) {
	var stamp = false
	var s []string
	s = append(s, name)
	for _, t := range tags {
		// stamp is added on `clock.Stop()`
		// we want that both `clock.Start()` and `clock.Stop()` have the same map id
		if t.Name != "stamp" {
			s = append(s, t.Name, t.Value)
		} else {
			stamp = true
		}
	}
	return strings.Join(s, ":"), stamp
}

// This is executed in the context of the calling method
// We would like to keep track of still running operations, and completed operations durations
// HandleMeasures can be called by `NewClockWithObserve` which indicates a "start" of an operation
// Or by `clock.Stop` which indicates a "stop" of an operation
// We pass the measurements to a channel and periodically aggregate the data and print a hearbeat log
func (h *logHandler) HandleMeasures(time time.Time, measures ...stats.Measure) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, m := range measures {
		id, stamp := meta(m.Fields[0].Name, m.Tags)
		if stamp {
			item, ok := h.trackedOperations.Get(id)
			if ok {
				h.trackedOperations.Set(id, stat{Start: item.(stat).Start, Duration: m.Fields[0].Value.Duration()})
			}
		} else {
			h.trackedOperations.Set(id, stat{Start: time, Duration: 0})
		}
	}
}

// This is executed in the context of the tick go routine
func (h *logHandler) Flush() {
	h.mu.Lock()
	defer h.mu.Unlock()

	durationReported := make([]string, 0)
	for el := h.trackedOperations.Front(); el != nil; el = el.Next() {
		id := el.Key
		stat := el.Value.(stat)
		if stat.Duration == 0 {
			// `clock.Stop` was not called, so the operation is still running
			// We log the duration since the start of the operation
			h.logger.Debug("heartbeat", "id", id, "running_for", time.Since(stat.Start).Round(time.Second).String())
		} else {
			// `clock.Stop` was called, so we log the total duration and remove the operation from future logs
			durationReported = append(durationReported, id.(string))
			h.logger.Debug("heartbeat", "id", id, "duration", stat.Duration.Round(time.Second).String())
		}
	}

	for _, id := range durationReported {
		h.trackedOperations.Delete(id)
	}
}

func Start(ctx context.Context, logger hclog.Logger, options ...func(*Options)) {
	stats.DefaultEngine.Prefix = ""

	opts := &Options{tick: time.Minute, handler: newHandler(logger)}
	for _, o := range options {
		o(opts)
	}

	stats.Register(opts.handler)

	go func() {
		ticker := time.NewTicker(opts.tick)
		for range ticker.C {
			select {
			case <-ctx.Done():
				return
			default:
				stats.Flush()
			}
		}
	}()
}

func WithTick(tick time.Duration) func(*Options) {
	return func(opts *Options) {
		opts.tick = tick
	}
}

func WithHandler(handler stats.Handler) func(*Options) {
	return func(opts *Options) {
		opts.handler = handler
	}
}

func Flush() {
	stats.Flush()
}

func newHandler(logger hclog.Logger) stats.Handler {
	return &logHandler{logger: logger, trackedOperations: orderedmap.NewOrderedMap()}
}
