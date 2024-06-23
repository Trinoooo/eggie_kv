package server

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/push"
	"log"
	"time"
)

type MetricsHelper struct {
	ConnectionAcceptCounter prometheus.Counter // socket accept qps
}

func NewMetricsHelper() *MetricsHelper {
	connectionAcceptCounter := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "eggie_kv_connection_accept_counter",
	})
	registry := prometheus.NewRegistry()
	registry.MustRegister(
		connectionAcceptCounter,
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	pusher := push.New("http://localhost:9091", "eggie_kv").Gatherer(registry)
	go func() {
		for {
			if err := pusher.Add(); err != nil {
				log.Printf("prometheus pusher push failed. err: %v", err)
			}
			// push every 5 ms
			time.Sleep(5 * time.Millisecond)
		}
	}()

	return &MetricsHelper{
		ConnectionAcceptCounter: connectionAcceptCounter,
	}
}
