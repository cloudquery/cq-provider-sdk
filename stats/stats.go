package stats

import (
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/segmentio/stats/v4"
)

type Measures struct {
	measures []stats.Measure
	time     time.Time
}
type LogHandler struct {
	logger   hclog.Logger
	measures chan Measures
}

func (h *LogHandler) HandleMeasures(time time.Time, measures ...stats.Measure) {
	h.measures <- Measures{measures: measures, time: time}
}

func (h *LogHandler) Flush() {
	for m := range h.measures {
		h.logger.Debug("heartbeat", "time", m.time, "measures", m)
	}
}

func newHandler(logger hclog.Logger) stats.Handler {
	return &LogHandler{logger: logger, measures: make(chan Measures)}
}

func Start(logger hclog.Logger) {
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
