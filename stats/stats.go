package stats

import (
	"context"
	"strings"
	"time"

	"github.com/elliotchance/orderedmap"
	"github.com/hashicorp/go-hclog"
	"github.com/segmentio/stats/v4"
)

type Measure struct {
	id       string
	duration time.Duration
	stamp    bool
	time     time.Time
}

type Stat struct {
	Start    time.Time
	Duration time.Duration
}

type LogHandler struct {
	logger   hclog.Logger
	measures chan Measure
	stats    *orderedmap.OrderedMap
}

func NewClockWithObserve(name string, tags ...stats.Tag) *stats.Clock {
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

func (h *LogHandler) HandleMeasures(time time.Time, measures ...stats.Measure) {
	for _, m := range measures {
		id, stamp := meta(m.Fields[0].Name, m.Tags)
		h.measures <- Measure{id: id, duration: m.Fields[0].Value.Duration(), time: time, stamp: stamp}
	}
}

func (h *LogHandler) Flush() {
	hasItems := true
	for hasItems {
		select {
		case m := <-h.measures:
			if m.stamp {
				item, ok := h.stats.Get(m.id)
				if ok {
					h.stats.Set(m.id, Stat{Start: item.(Stat).Start, Duration: m.duration})
				}
			} else {
				h.stats.Set(m.id, Stat{Start: m.time, Duration: 0})
			}
		default:
			hasItems = false
		}
	}

	durationReported := make([]string, 0)
	for el := h.stats.Front(); el != nil; el = el.Next() {
		id := el.Key
		stat := el.Value.(Stat)
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

const BUFFER_SIZE = 100

func newHandler(logger hclog.Logger) stats.Handler {
	return &LogHandler{logger: logger, measures: make(chan Measure, BUFFER_SIZE), stats: orderedmap.NewOrderedMap()}
}

func Start(ctx context.Context, logger hclog.Logger) {
	stats.DefaultEngine.Prefix = ""
	stats.Register(newHandler(logger))

	go func() {
		ticker := time.NewTicker(time.Second * 30)
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
