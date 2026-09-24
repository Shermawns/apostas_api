package metrics

import (
	"fmt"
	"io"
	"sort"
	"sync"
)

var registry = struct {
	sync.Mutex
	values map[string]uint64
}{values: map[string]uint64{
	"apostas_wager_duplicates_total":                          0,
	"apostas_sqs_retries_total":                               0,
	"apostas_sqs_dlq_total":                                   0,
	"apostas_concurrency_conflicts_total":                     0,
	"apostas_outbox_retries_total":                            0,
	"apostas_outbox_published_total":                          0,
	"apostas_outbox_delay_milliseconds_total":                 0,
	"apostas_sqs_processing_latency_milliseconds_total":       0,
	"apostas_reconciliation_divergences_total":                0,
	`apostas_wager_results_total{status="PROCESSED"}`:         0,
	`apostas_wager_results_total{status="REJECTED"}`:          0,
	`apostas_wager_results_total{status="PENDING_REFERENCE"}`: 0,
}}

func Inc(name string) {
	registry.Lock()
	registry.values[name]++
	registry.Unlock()
}

func Add(name string, value uint64) {
	registry.Lock()
	registry.values[name] += value
	registry.Unlock()
}

func WritePrometheus(w io.Writer) error {
	registry.Lock()
	snapshot := make(map[string]uint64, len(registry.values))
	for key, value := range registry.values {
		snapshot[key] = value
	}
	registry.Unlock()
	keys := make([]string, 0, len(snapshot))
	for key := range snapshot {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if _, err := fmt.Fprintf(w, "%s %d\n", key, snapshot[key]); err != nil {
			return err
		}
	}
	return nil
}
