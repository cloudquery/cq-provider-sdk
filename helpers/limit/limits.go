package limit

import (
	"github.com/pbnjay/memory"
	"math"
)

const (
	gbInBytes         int     = 1024 * 1024 * 1024
	goroutinesPerGB   float64 = 250000
	minimalGoRoutines float64 = 100
	//
	goroutineReducer = 0.8
)

type Rlimit struct {
	Cur uint64
	Max uint64
}

func GetMaxGoRoutines() uint64 {
	limit := calculateGoRoutines(getMemory())
	ulimit, err := GetUlimit()
	if err != nil || ulimit.Max == 0 {
		return limit
	}
	if ulimit.Max > limit {
		return limit
	}
	return ulimit.Max
}

func getMemory() uint64 {
	return memory.TotalMemory()
}

func calculateGoRoutines(totalMemory uint64) uint64 {
	var total uint64 = 0
	if totalMemory == 0 {
		// assume we have 2 GB RAM
		total = uint64(math.Max(minimalGoRoutines, goroutinesPerGB*2*goroutineReducer))
	} else {
		total = uint64(math.Max(minimalGoRoutines, (goroutinesPerGB*float64(totalMemory)/float64(gbInBytes))*goroutineReducer))
	}
	mfo, err := calculateFileLimit()
	if err != nil {
		return total
	}

	if mfo < total {
		return uint64(float64(mfo) * 0.3)
	}
	return total
}
