package hbase

import (
	"time"

	"github.com/rcrowley/go-metrics"
	"github.com/vrischmann/go-metrics-influxdb"
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
	go influxdb.InfluxDB(
		registry,                // metrics registry
		time.Second*1,           // interval
		"http://localhost:8086", // the InfluxDB url
		"mydb",                  // your InfluxDB database
		"myuser",                // your InfluxDB user
		"mypassword",            // your InfluxDB password
	)
	//go metrics.Log(registry, 1*time.Second, log.New(os.Stderr, "metrcs: ", log.Lmicroseconds))
	return &GetStatusMonitor{
		registry: registry,
		context: Context{
			name: name,
			tag:  tag,
		},
		callCount: metrics.NewRegisteredCounter("calls.success", registry),
	}
}
