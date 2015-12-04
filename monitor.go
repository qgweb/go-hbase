package hbase

import (
	"log"
	"os"
	"time"

	"github.com/rcrowley/go-metrics"
)

type Context struct {
	name string
	tag  string
}
type GetStatusMonitor struct {
	registry  metrics.Registry
	context   Context
	callCount metrics.Counter
}

func (m *GetStatusMonitor) OnCallSuccess() {
	m.callCount.Inc(1)
}

func NewGetStatusMonitor(name, tag string) *GetStatusMonitor {
	registry := metrics.NewRegistry()
	go metrics.Log(registry, 1*time.Second, log.New(os.Stderr, "metrcs: ", log.Lmicroseconds))
	return &GetStatusMonitor{
		registry: registry,
		context: Context{
			name: name,
			tag:  tag,
		},
		callCount: metrics.NewRegisteredCounter("calls.success", registry),
	}
}
