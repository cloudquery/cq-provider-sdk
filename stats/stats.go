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

const (
	bufferSize = 100
)

type measure struct {
	id       string
	duration time.Duration
	stamp    bool
	time     time.Time
}

type stat struct {
	Start    time.Time
	Duration time.Duration
}

type logHandler struct {
	logger   hclog.Logger
	measures chan measure
	stats    *orderedmap.OrderedMap
	mu       sync.Mutex
}

type Options struct {
	Logger  hclog.Logger
	Tick    time.Duration
	Handler stats.Handler
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
		// stamp is added on clock.Stop()
		// we want that both clock.Start() and clock.Stop() have the same map id
		if t.Name != "stamp" {
			s = append(s, t.Name, t.Value)
		} else {
			stamp = true
		}
	}
	return strings.Join(s, ":"), stamp
}

// This is executed in the context of the calling method
func (h *logHandler) HandleMeasures(time time.Time, measures ...stats.Measure) {
	for _, m := range measures {
		id, stamp := meta(m.Fields[0].Name, m.Tags)
		h.measures <- measure{id: id, duration: m.Fields[0].Value.Duration(), time: time, stamp: stamp}
	}
}

// This is executed in the context of the tick go routine
func (h *logHandler) Flush() {
	h.mu.Lock()
	defer h.mu.Unlock()

	hasItems := true
	for hasItems {
		select {
		case m := <-h.measures:
			if m.stamp {
				item, ok := h.stats.Get(m.id)
				if ok {
					h.stats.Set(m.id, stat{Start: item.(stat).Start, Duration: m.duration})
				}
			} else {
				h.stats.Set(m.id, stat{Start: m.time, Duration: 0})
			}
		default:
			hasItems = false
		}
	}

	durationReported := make([]string, 0)
	for el := h.stats.Front(); el != nil; el = el.Next() {
		id := el.Key
		stat := el.Value.(stat)
		if stat.Duration == 0 {
			h.logger.Debug("heartbeat", "id", id, "running_for", time.Since(stat.Start).Round(time.Second).String())
		} else {
			durationReported = append(durationReported, id.(string))
			h.logger.Debug("heartbeat", "id", id, "duration", stat.Duration.Round(time.Second).String())
		}
	}

	for _, id := range durationReported {
		h.stats.Delete(id)
	}
}

func Start(ctx context.Context, opts *Options) {
	stats.DefaultEngine.Prefix = ""

	if opts.Tick == 0 {
		opts.Tick = time.Minute
	}

	if opts.Handler == nil {
		opts.Handler = newHandler(opts.Logger)
	}

	stats.Register(opts.Handler)

	go func() {
		ticker := time.NewTicker(opts.Tick)
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

func Flush() {
	stats.Flush()
}

func newHandler(logger hclog.Logger) stats.Handler {
	return &logHandler{logger: logger, measures: make(chan measure, bufferSize), stats: orderedmap.NewOrderedMap()}
}
