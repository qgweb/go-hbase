package hbase

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/rcrowley/go-metrics"
	"github.com/vrischmann/go-metrics-influxdb"
)

type sinkType int

const (
	logSink = iota
	influxSink
)

type context struct {
	prefix string
	tag    string
}
type GetStatusMonitor struct {
	registry    metrics.Registry
	ctx         context
	callCounter metrics.Counter
}

func (m *GetStatusMonitor) OnCallSuccess() {
	m.callCounter.Inc(1)
}

func NewRegistry(sink sinkType) metrics.Registry {
	registry := metrics.NewRegistry()
	if metrics.UseNilMetrics {
		return registry
	}
	switch sink {
	case influxSink:
		go influxdb.InfluxDB(
			registry,
			time.Second*1,
			"http://localhost:8086",
			"mydb",
			"myuser",
			"mypassword",
		)
	case logSink:
		go metrics.Log(registry, 1*time.Second, log.New(os.Stderr, "metrcs: ", log.Lmicroseconds))
	default:
		break
	}
	return registry
}
func NewGetStatusMonitor(prefix string) *GetStatusMonitor {
	registry := NewRegistry(influxSink)
	ctx := context{
		prefix: prefix,
		tag:    "get",
	}
	return &GetStatusMonitor{
		registry:    registry,
		ctx:         ctx,
		callCounter: metrics.NewRegisteredCounter(fmt.Sprintf("%s.%s.%s", prefix, ctx.tag, "success"), registry),
	}
}

type RPCMonitor struct {
	registry    metrics.Registry
	ctx         context
	callTimer   metrics.Timer
	callCounter metrics.Counter
	callError   metrics.Counter
}

func NewRPCMonitor(prefix string) *RPCMonitor {
	registry := NewRegistry(influxSink)
	ctx := context{
		prefix: prefix,
		tag:    "rpc",
	}
	return &RPCMonitor{
		registry:    registry,
		ctx:         ctx,
		callTimer:   metrics.NewRegisteredTimer(fmt.Sprintf("%s.%s.%s", prefix, ctx.tag, "timer"), registry),
		callCounter: metrics.NewRegisteredCounter(fmt.Sprintf("%s.%s.%s", prefix, ctx.tag, "success"), registry),
		callError:   metrics.NewRegisteredCounter(fmt.Sprintf("%s.%s.%s", prefix, ctx.tag, "error"), registry),
	}
}
func (m *RPCMonitor) OnCallFinish(t time.Time) {
	m.callTimer.UpdateSince(t)
	m.callCounter.Inc(1)
}
func (m *RPCMonitor) OnCallError() {
	m.callError.Inc(1)
}
