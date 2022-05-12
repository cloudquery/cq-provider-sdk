package stats

import (
	"strings"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/segmentio/stats/v4"
)

type Measure struct {
	id    string
	value stats.Value
	stamp bool
	time  time.Time
}

type Stat struct {
	Start    time.Time
	Duration int64
}

type LogHandler struct {
	logger   hclog.Logger
	measures chan Measure
	stats    map[string]Stat
}

func NewClockWithObserve(name string, tags ...stats.Tag) *stats.Clock {
	now := time.Now()
	cl := stats.DefaultEngine.ClockAt(name, now, tags...)
	stats.DefaultEngine.Observe(name, now, tags...)
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

func (h *LogHandler) HandleMeasures(time time.Time, measures ...stats.Measure) {
	for _, m := range measures {
		id, stamp := meta(m.Name, m.Tags)
		h.measures <- Measure{id: id, value: m.Fields[0].Value, time: time, stamp: stamp}
	}
}

func (h *LogHandler) Flush() {
	for m := range h.measures {
		if m.stamp {
			if m.stamp {
				item := h.stats[m.id]
				item.Duration = m.value.Int()
			} else {
				h.stats[m.id] = Stat{Start: m.time}
			}
		}
	}
}

func newHandler(logger hclog.Logger) stats.Handler {
	return &LogHandler{logger: logger, measures: make(chan Measure), stats: make(map[string]Stat)}
}

func Start(logger hclog.Logger) {
	stats.DefaultEngine.Prefix = ""
	stats.Register(newHandler(logger))

	go func() {
		for range time.Tick(time.Second * 30) {
			stats.Flush()
		}
	}()
}

func Flush() {
	stats.Flush()
}
