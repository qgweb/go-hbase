package hbase

import (
	"fmt"
	"time"

	"github.com/rcrowley/go-metrics"
	"github.com/vrischmann/go-metrics-influxdb"
)

type Context struct {
	prefix string
	tag    string
}
type GetStatusMonitor struct {
	registry    metrics.Registry
	context     Context
	callCounter metrics.Counter
}

func (m *GetStatusMonitor) OnCallSuccess() {
	m.callCounter.Inc(1)
}

// TODO: add output option
func NewRegistry() metrics.Registry {
	registry := metrics.NewRegistry()
	go influxdb.InfluxDB(
		registry,                // metrics registry
		time.Second*1,           // interval
		"http://localhost:8086", // the InfluxDB url
		"mydb",                  // your InfluxDB database
		"myuser",                // your InfluxDB user
		"mypassword",            // your InfluxDB password
	)
	//go metrics.Log(registry, 1*time.Second, log.New(os.Stderr, "metrcs: ", log.Lmicroseconds))
	return registry
}
func NewGetStatusMonitor(prefix string) *GetStatusMonitor {
	registry := NewRegistry()
	context := Context{
		prefix: prefix,
		tag:    "get",
	}
	return &GetStatusMonitor{
		registry:    registry,
		context:     context,
		callCounter: metrics.NewRegisteredCounter(fmt.Sprintf("%s.%s.%s", prefix, context.tag, "success"), registry),
	}
}

type RPCMonitor struct {
	registry    metrics.Registry
	context     Context
	callTimer   metrics.Timer
	callCounter metrics.Counter
	callError   metrics.Counter
}

func NewRPCMonitor(prefix string) *RPCMonitor {
	registry := NewRegistry()
	context := Context{
		prefix: prefix,
		tag:    "rpc",
	}
	return &RPCMonitor{
		registry:    registry,
		context:     context,
		callTimer:   metrics.NewRegisteredTimer(fmt.Sprintf("%s.%s.%s", prefix, context.tag, "timer"), registry),
		callCounter: metrics.NewRegisteredCounter(fmt.Sprintf("%s.%s.%s", prefix, context.tag, "success"), registry),
		callError:   metrics.NewRegisteredCounter(fmt.Sprintf("%s.%s.%s", prefix, context.tag, "error"), registry),
	}
}
func (m *RPCMonitor) OnCallFinish(t time.Time) {
	m.callTimer.UpdateSince(t)
	m.callCounter.Inc(1)
}
func (m *RPCMonitor) OnCallError() {
	m.callError.Inc(1)
}
